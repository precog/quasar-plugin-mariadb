/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.mariadb.datasource

import quasar.plugin.mariadb.{HI, MariaDbHygiene}

import scala._, Predef._
import scala.collection.immutable.Map
import java.lang.Throwable

import cats.{Defer, Id}
import cats.data.NonEmptyList
import cats.effect.{Bracket, Resource}
import cats.implicits._

import doobie._
import doobie.enum.JdbcType
import doobie.implicits._

import quasar.api.{ColumnType, DataPathSegment}
import quasar.api.push.InternalKey
import quasar.connector.{MonadResourceErr, Offset}
import quasar.connector.datasource.{LightweightDatasourceModule, Loader}
import quasar.lib.jdbc._
import quasar.lib.jdbc.datasource._

import org.slf4s.Logger

import java.time.format.DateTimeFormatter

import skolems.∃

private[datasource] object MariaDbDatasource {
  val DefaultResultChunkSize: Int = 4096

  def apply[F[_]: Bracket[?[_], Throwable]: Defer: MonadResourceErr](
      xa: Transactor[F],
      discovery: JdbcDiscovery,
      log: Logger)
      : LightweightDatasourceModule.DS[F] = {

    val maskInterpreter =
      MaskInterpreter(MariaDbHygiene) { (table, schema) =>
        discovery.tableColumns(table.asIdent, schema.map(_.asIdent))
          .map(m =>
            if (m.jdbcType == JdbcType.Bit && m.vendorType == Mapping.TINYINT)
              Some(m.name -> ColumnType.Boolean)
            else
              Mapping.MariaDbColumnTypes.get(m.vendorType)
                .orElse(Mapping.JdbcColumnTypes.get(m.jdbcType))
                .tupleLeft(m.name))
          .unNone
          .compile.to(Map)
      }

    val loader =
      JdbcLoader(xa, discovery, MariaDbHygiene) {
        RValueLoader.seek[HI](
          Slf4sLogHandler(log),
          DefaultResultChunkSize,
          MariaDbRValueColumn,
          offsetFragment)
        .compose(maskInterpreter.andThen(Resource.eval(_)))
      }

    JdbcDatasource(
      xa,
      discovery,
      MariaDbDatasourceModule.kind,
      NonEmptyList.one(Loader.Batch(loader)))
  }

  private def offsetFragment(offset: Offset): Either[String, Fragment] = {
    internalize(offset) flatMap { ioffset =>
      columnFragment(ioffset) map { cfr =>
        offsetFragment(cfr, ioffset.value)
      }
    }
  }

  private def internalize(offset: Offset)
      : Either[String, Offset.Internal] = offset match {
    case _: Offset.External =>
      Left("MariaDb/MySQL datasource supports only internal offsets")

    case i: Offset.Internal => i.asRight[String]
  }

  private def columnFragment(offset: Offset.Internal)
      : Either[String, Fragment] = offset.path match {
    case NonEmptyList(DataPathSegment.Field(s), List()) =>
      Fragment.const(MariaDbHygiene.hygienicIdent(Ident(s)).forSql).asRight[String]
    case _ =>
      Left(s"Incorrect offset path")
  }

  private def offsetFragment(colFragment: Fragment, key: ∃[InternalKey.Actual]): Fragment = {
    val actual: InternalKey[Id, _] = key.value

    val actualFragment = actual match {
      case InternalKey.RealKey(k) =>
        Fragment.const(k.toString)

      case InternalKey.StringKey(s) =>
        fr0"'" ++ Fragment.const0(s) ++ fr0"'"

      case InternalKey.DateTimeKey(d) =>
        val formatter = DateTimeFormatter.ofPattern("YYYY-MM-dd HH:mm:ss.SSSSSS")
        val str = formatter.format(d)
        fr0"'" ++ Fragment.const0(str) ++ fr0"'"
    }

    colFragment ++ fr">=" ++ actualFragment
  }
}
