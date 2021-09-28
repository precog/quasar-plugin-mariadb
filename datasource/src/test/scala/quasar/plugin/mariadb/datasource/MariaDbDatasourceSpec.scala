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

import quasar.plugin.mariadb.TestHarness

import scala._, Predef._
import java.time._

import argonaut._, Argonaut._

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.implicits.javatime._

import fs2.Stream

import org.slf4s.Logging

import quasar.ScalarStages
import quasar.api.DataPathSegment
import quasar.api.push.{InternalKey, ExternalOffsetKey}
import quasar.api.resource.ResourcePath
import quasar.common.data.RValue
import quasar.connector.{QueryResult, Offset}
import quasar.connector.datasource.DatasourceModule
import quasar.lib.jdbc.JdbcDiscovery
import quasar.qscript.InterpretedRead

import skolems.∃

object MariaDbDatasourceSpec extends TestHarness with Logging {
  import RValue._

  type DS = DatasourceModule.DS[IO]

  sequential

  def harnessed(jdbcUrl: String = TestUrl(Some(TestDb)))
      : Resource[IO, (Transactor[IO], DS, ResourcePath, String)] =
    tableHarness(jdbcUrl) map {
      case (xa, path, name) =>
        val disc = JdbcDiscovery(MariaDbDatasourceModule.discoverableTableTypes(log))
        (xa, MariaDbDatasource(xa, disc, log), path, name)
    }

  def loadRValues(ds: DS, p: ResourcePath): IO[List[RValue]] =
    ds.loadFull(InterpretedRead(p, ScalarStages.Id)).value use {
      case Some(QueryResult.Parsed(_, data, _)) =>
        data.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)

      case _ => IO.pure(List[RValue]())
    }

  def seekRValues(ds: DS, p: ResourcePath, offset: Offset): IO[List[RValue]] =
    ds.loadFrom(InterpretedRead(p, ScalarStages.Id), offset).value use {
      case Some(QueryResult.Parsed(_, data, _)) =>
        data.data.asInstanceOf[Stream[IO, RValue]].compile.to(List)
      case _ => IO.pure(List[RValue]())
    }

  def obj(assocs: (String, RValue)*): RValue =
    rObject(Map(assocs: _*))

  "loading data" >> {
    "boolean" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (b BOOLEAN)").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (b) VALUES (true), (false)").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          // Some dbs use BIT and others TINYINT, we can't currently distinguish the bool
          // TINYINT case from other TINYINT cases, so the column ends up as a number
          val expectedB = List(rBoolean(true), rBoolean(false)).map(b => obj("b" -> b))
          val expectedI = List(rLong(1), rLong(0)).map(b => obj("b" -> b))

          (results must containTheSameElementsAs(expectedB)) or (results must containTheSameElementsAs(expectedI))
        }
      }
    }

    "string" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (c CHAR(5), vc VARCHAR(5), txt TEXT)").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (c, vc, txt) VALUES ('abcde', 'fghij', 'klmnopqrs'), ('foo', 'bar', 'baz')").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj("c" -> rString("abcde"), "vc" -> rString("fghij"), "txt" -> rString("klmnopqrs")),
            obj("c" -> rString("foo"), "vc" -> rString("bar"), "txt" -> rString("baz")))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    // TINYINT | SMALLINT | MEDIUMINT | INTEGER | BIGINT | DOUBLE | FLOAT | DECIMAL
    "number" >> {
      def insert(tbl: String, tiny: Int, small: Int, med: Int, norm: Int, big: Long, dbl: Double, flt: Float, dec: BigDecimal): ConnectionIO[Int] = {
        val sql =
          fr"INSERT INTO" ++ frag(tbl) ++ fr0" (tiny, small, med, norm, big, dbl, flt, xct) values ($tiny, $small, $med, $norm, $big, $dbl, $flt, $dec)"

        sql.update.run
      }

      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (tiny TINYINT, small SMALLINT, med MEDIUMINT, norm INT, big BIGINT, dbl DOUBLE, flt FLOAT, xct DECIMAL(65, 30))").update.run

          _ <- insert(name,
            -128,
            -32768,
            -8388608,
            -2147483648,
            -9223372036854775808L,
            -1.7976931348623157E+308,
            -3.40282E+38f,
            BigDecimal("-99999999999999999999999999999999999.9999999999999999999999999999"))

          _ <- insert(name,
            127,
            32767,
            8388607,
            2147483647,
            9223372036854775807L,
            1.7976931348623157E+308,
            3.40282E+38f,
            BigDecimal("99999999999999999999999999999999999.9999999999999999999999999999"))
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj(
              "tiny" -> rLong(-128L),
              "small" -> rLong(-32768L),
              "med" -> rLong(-8388608L),
              "norm" -> rLong(-2147483648L),
              "big" -> rLong(-9223372036854775808L),
              "dbl" -> rDouble(-1.7976931348623157E+308),
              "flt" -> rDouble(-3.40282E+38),
              "xct" -> rNum(BigDecimal("-99999999999999999999999999999999999.9999999999999999999999999999"))),
            obj(
              "tiny" -> rLong(127L),
              "small" -> rLong(32767L),
              "med" -> rLong(8388607L),
              "norm" -> rLong(2147483647L),
              "big" -> rLong(9223372036854775807L),
              "dbl" -> rDouble(1.7976931348623157E+308),
              "flt" -> rDouble(3.40282E+38),
              "xct" -> rNum(BigDecimal("99999999999999999999999999999999999.9999999999999999999999999999"))))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    // TIME | DATE | TIMESTAMP | DATETIME | YEAR
    "temporal" >> {
      val minDate = LocalDate.parse("1000-01-01")
      val maxDate = LocalDate.parse("9999-12-31")

      val minTime = LocalTime.parse("00:00:00")
      val maxTime = LocalTime.parse("23:59:59.999999")

      val minDateTime = LocalDateTime.parse("1000-01-01T00:00:00.000000")
      val maxDateTime = LocalDateTime.parse("9999-12-31T23:59:59.999999")

      val minTimestamp = LocalDateTime.parse("1970-01-01T00:00:01")
      val maxTimestamp = LocalDateTime.parse("2038-01-19T03:14:07")

      val minYear = 1901
      val maxYear = 2155

      def insert(tbl: String, lt: LocalTime, ld: LocalDate, ts: LocalDateTime, dt: LocalDateTime, y: Int): ConnectionIO[Int] = {
        val sql =
          fr"INSERT INTO" ++ frag(tbl) ++ fr0" (lt, ld, ts, dt, y) values ($lt, $ld, $ts, $dt, $y)"

        sql.update.run
      }

      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (lt TIME(6), ld DATE, ts TIMESTAMP(6), dt DATETIME(6), y YEAR)").update.run

          _ <- insert(name, minTime, minDate, minTimestamp, minDateTime, minYear)
          _ <- insert(name, maxTime, maxDate, maxTimestamp, maxDateTime, maxYear)
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj(
              "lt" -> rLocalTime(minTime),
              "ld" -> rLocalDate(minDate),
              "ts" -> rLocalDateTime(minTimestamp),
              "dt" -> rLocalDateTime(minDateTime),
              "y" -> rLong(minYear.toLong)),
            obj(
              "lt" -> rLocalTime(maxTime),
              "ld" -> rLocalDate(maxDate),
              "ts" -> rLocalDateTime(maxTimestamp),
              "dt" -> rLocalDateTime(maxDateTime),
              "y" -> rLong(maxYear.toLong)))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    "inet6" >> {
      harnessed() use { case (xa, ds, path, name) =>
        onlyVendors(xa, Vendors.MariaDB) {
          val setup = for {
            _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (ip INET6)").update.run
            _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (ip) VALUES ('2001:db8::ff00:42:8329'), ('::192.0.2.128')").update.run
          } yield ()

          (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
            val expected = List(
              obj("ip" -> rString("2001:db8::ff00:42:8329")),
              obj("ip" -> rString("::192.0.2.128")))

            results must containTheSameElementsAs(expected)
          }
        }
      }
    }

    "json" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val js1 = """{"foo": 1, "bar": [2, 3]}"""

        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (js JSON)").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (js) VALUES ($js1), ('false')").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val parsed = results flatMap { rv =>
            (rField1("js").composePrism(rString))
              .getOption(rv)
              .flatMap(_.parseOption)
              .toList
          }

          val expected = List(
            Json("foo" := 1, "bar" := List(2, 3)),
            jFalse)

          parsed must containTheSameElementsAs(expected)
        }
      }
    }

    "set" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (st SET('foo', 'bar', 'baz'))").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (st) VALUES ('foo,baz'), ('bar'), ('')").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj("st" -> rString("foo,baz")),
            obj("st" -> rString("bar")),
            obj("st" -> rString("")))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    "enum" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (en ENUM('red', 'green', 'blue'))").update.run
          _ <- (fr"INSERT INTO" ++ frag(name) ++ fr0" (en) VALUES ('green'), ('green'), ('blue')").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          val expected = List(
            obj("en" -> rString("green")),
            obj("en" -> rString("green")),
            obj("en" -> rString("blue")))

          results must containTheSameElementsAs(expected)
        }
      }
    }

    "empty table returns empty results" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup = for {
          _ <- (fr"CREATE TABLE" ++ frag(name) ++ fr0" (en ENUM('red', 'green', 'blue'), ts TIMESTAMP(6), n INT, d DOUBLE, x LONGTEXT)").update.run
        } yield ()

        (setup.transact(xa) >> loadRValues(ds, path)) map { results =>
          results must beEmpty
        }
      }
    }
  }
  "seek data" >> {
    import DataPathSegment._

    "errors when path is incorrect" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val key = ∃(InternalKey.Actual.string("1"))
        val twoFields = Offset.Internal(NonEmptyList.of(Field("foo"), Field("bar")), key)
        val index = Offset.Internal(NonEmptyList.one(Index(0)), key)
        val all = Offset.Internal(NonEmptyList.one(AllFields), key)
        val allIndices = Offset.Internal(NonEmptyList.one(AllIndices), key)
        val external = Offset.External(ExternalOffsetKey(Array(0x01, 0x01)))

        for {
          two <- seekRValues(ds, path, twoFields).attempt
          ind <- seekRValues(ds, path, index).attempt
          allF <- seekRValues(ds, path, all).attempt
          allI <- seekRValues(ds, path, allIndices).attempt
          ext <- seekRValues(ds, path, external).attempt
        } yield {
          two must beLeft
          ind must beLeft
          allF must beLeft
          allI must beLeft
          ext must beLeft
        }
      }
    }
    "string offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ frag(name) ++ fr0" (id INT, off TEXT)").update.run >>
          (fr"INSERT INTO" ++ frag(name) ++ fr0" VALUES (0, 'foo'), (1, 'bar'), (2, 'baz')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("off")), ∃(InternalKey.Actual.string("baz")))

        val expected = List(
          obj("id" -> rLong(2), "off" -> rString("baz")),
          obj("id" -> rLong(0), "off" -> rString("foo")))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
    "numeric offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ frag(name) ++ fr0" (id INT, off INT)").update.run >>
          (fr"INSERT INTO" ++ frag(name) ++ fr0" VALUES (0, 1), (1, 2), (2, 3), (3, 4), (4, 5)").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("off")), ∃(InternalKey.Actual.real(3)))

        val expected = List(
          obj("id" -> rLong(2), "off" -> rLong(3)),
          obj("id" -> rLong(3), "off" -> rLong(4)),
          obj("id" -> rLong(4), "off" -> rLong(5)))

        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
    "dateTime offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ frag(name) ++ fr0" (id TEXT, off TIMESTAMP(6))").update.run >>
          (fr"INSERT INTO" ++ frag(name) ++ fr" VALUES ('a', '2001-11-11 11:11:11.111000')," ++
            fr"('b', '2002-02-02 11:11:11.110001')," ++
            fr"('c', '2003-12-12 12:12:12.100021')," ++
            fr"('d', '2004-08-13 13:13:13.000131')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("off")), ∃(InternalKey.Actual.dateTime(
            OffsetDateTime.parse("2003-12-12T12:12:12.100021+00:00"))))

        val expected = List(
          obj("id" -> rString("c"), "off" -> rLocalDateTime(LocalDateTime.parse("2003-12-12T12:12:12.100021"))),
          obj("id" -> rString("d"), "off" -> rLocalDateTime(LocalDateTime.parse("2004-08-13T13:13:13.000131"))))


        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
    "localDateTime offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ frag(name) ++ fr0" (id TEXT, off TIMESTAMP(6))").update.run >>
          (fr"INSERT INTO" ++ frag(name) ++ fr" VALUES ('a', '2001-11-11 11:11:11.111000')," ++
            fr"('b', '2002-02-02 11:11:11.110001')," ++
            fr"('c', '2003-12-12 12:12:12.100021')," ++
            fr"('d', '2004-08-13 13:13:13.000131')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("off")), ∃(InternalKey.Actual.localDateTime(
            LocalDateTime.parse("2003-12-12T12:12:12.100021"))))

        val expected = List(
          obj("id" -> rString("c"), "off" -> rLocalDateTime(LocalDateTime.parse("2003-12-12T12:12:12.100021"))),
          obj("id" -> rString("d"), "off" -> rLocalDateTime(LocalDateTime.parse("2004-08-13T13:13:13.000131"))))


        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
    "localDate offset" >> {
      harnessed() use { case (xa, ds, path, name) =>
        val setup =
          (fr"CREATE TABLE" ++ frag(name) ++ fr0" (id TEXT, off TIMESTAMP(6))").update.run >>
          (fr"INSERT INTO" ++ frag(name) ++ fr" VALUES ('a', '2001-11-11 11:11:11.111000')," ++
            fr"('b', '2002-02-02 11:11:11.110001')," ++
            fr"('c', '2003-12-12 12:12:12.100021')," ++
            fr"('d', '2004-08-13 13:13:13.000131')").update.run

        val offset =
          Offset.Internal(NonEmptyList.one(Field("off")), ∃(InternalKey.Actual.localDate(
            LocalDate.parse("2003-12-12"))))

        val expected = List(
          obj("id" -> rString("c"), "off" -> rLocalDateTime(LocalDateTime.parse("2003-12-12T12:12:12.100021"))),
          obj("id" -> rString("d"), "off" -> rLocalDateTime(LocalDateTime.parse("2004-08-13T13:13:13.000131"))))


        setup.transact(xa) >> seekRValues(ds, path, offset) map { results =>
          results must containTheSameElementsAs(expected)
        }
      }
    }
  }
}
