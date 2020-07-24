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

package quasar.plugin.mariadb.destination

import quasar.plugin.mariadb._

import scala._, Predef._

import java.io.InputStream

import cats.data.NonEmptyList
import cats.effect.ConcurrentEffect
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Stream

import org.mariadb.jdbc.MariaDbStatement

import org.slf4s.Logger

import quasar.plugin.jdbc.Slf4sLogHandler
import quasar.plugin.jdbc.destination.WriteMode

object CsvCreateSink {
  def apply[F[_]: ConcurrentEffect](
      writeMode: WriteMode,
      xa: Transactor[F],
      logger: Logger)(
      obj: Either[HI, (HI, HI)],
      cols: NonEmptyList[(HI, MariaDbType)],
      bytes: Stream[F, Byte])
      : Stream[F, Unit] = {

    val logHandler = Slf4sLogHandler(logger)

    val objFragment = obj.fold(
      t => Fragment.const0(t.forSql),
      { case (d, t) => Fragment.const0(d.forSql) ++ fr0"." ++ Fragment.const0(t.forSql) })

    def dropTableIfExists =
      (fr"DROP TABLE IF EXISTS" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    def truncateTable =
      (fr"TRUNCATE" ++ objFragment)
        .updateWithLogHandler(logHandler)
        .run

    def createTable(ifNotExists: Boolean) = {
      val stmt = if (ifNotExists) fr"CREATE TABLE IF NOT EXISTS" else fr"CREATE TABLE"

      (stmt ++ objFragment ++ fr0" " ++ columnSpecs(cols))
        .updateWithLogHandler(logHandler)
        .run
    }

    def loadCsv(bytes: InputStream) = {
      val loadData =
        fr"LOAD DATA LOCAL INFILE 'data.csv' INTO TABLE" ++
          objFragment ++
          fr0""" CHARACTER SET utf8mb4 FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\r\n'"""

      HC.createStatement(for {
        _ <- FS.raw(_.unwrap(classOf[MariaDbStatement]).setLocalInfileInputStream(bytes))
        _ <- FS.execute(loadData.update.sql)
      } yield ())
    }

    def doLoad(bytes: InputStream) = for {
      _ <- writeMode match {
        case WriteMode.Create =>
          createTable(ifNotExists = false)

        case WriteMode.Replace =>
          dropTableIfExists >> createTable(ifNotExists = false)

        case WriteMode.Truncate =>
          createTable(ifNotExists = true) >> truncateTable
      }

      _ <- loadCsv(bytes)
    } yield ()

    bytes.through(fs2.io.toInputStream[F]).evalMap(doLoad(_).transact(xa))
  }

  ////

  private def columnSpecs(cols: NonEmptyList[(HI, MariaDbType)]): Fragment =
    Fragments.parentheses(
      cols
        .map { case (n, t) => Fragment.const(n.forSql) ++ t.asSql }
        .intercalate(fr","))
}
