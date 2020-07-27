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

import scala._, Predef._

import cats.data.{Ior, NonEmptyList}

import doobie.Fragment

import quasar.api.Labeled
import quasar.api.push.param._
import quasar.connector.destination.Constructor

sealed abstract class MariaDbType(spec: String) extends Product with Serializable {
  def asSql: Fragment = Fragment.const0(spec)
}

object MariaDbType {
  case object BOOLEAN extends MariaDbTypeId.SelfIdentified("BOOLEAN", 0)

  sealed abstract class Numeric(spec: String, signedness: Signedness)
      extends MariaDbType(s"$spec $signedness")

  final case class TINYINT(signedness: Signedness) extends Numeric("TINYINT", signedness)
  case object TINYINT extends MariaDbTypeId.HigherKinded(1) {
    val constructor = numericConstructor(TINYINT(_))
  }

  final case class SMALLINT(signedness: Signedness) extends Numeric("SMALLINT", signedness)
  case object SMALLINT extends MariaDbTypeId.HigherKinded(2) {
    val constructor = numericConstructor(SMALLINT(_))
  }

  final case class MEDIUMINT(signedness: Signedness) extends Numeric("MEDIUMINT", signedness)
  case object MEDIUMINT extends MariaDbTypeId.HigherKinded(3) {
    val constructor = numericConstructor(MEDIUMINT(_))
  }

  final case class INT(signedness: Signedness) extends Numeric("INT", signedness)
  case object INT extends MariaDbTypeId.HigherKinded(4) {
    val constructor = numericConstructor(INT(_))
  }

  final case class BIGINT(signedness: Signedness) extends Numeric("BIGINT", signedness)
  case object BIGINT extends MariaDbTypeId.HigherKinded(5) {
    val constructor = numericConstructor(BIGINT(_))
  }

  final case class FLOAT(signedness: Signedness) extends Numeric("FLOAT", signedness)
  case object FLOAT extends MariaDbTypeId.HigherKinded(6) {
    val constructor = numericConstructor(FLOAT(_))
  }

  final case class DOUBLE(signedness: Signedness) extends Numeric("DOUBLE", signedness)
  case object DOUBLE extends MariaDbTypeId.HigherKinded(7) {
    val constructor = numericConstructor(DOUBLE(_))
  }

  final case class DECIMAL(precision: Int, scale: Int, signedness: Signedness)
      extends Numeric(s"DECIMAL($precision, $scale)", signedness)

  case object DECIMAL extends MariaDbTypeId.HigherKinded(8) {
    val constructor = {
      val precisionParam: Labeled[Formal[Int]] =
        Labeled("Precision", Formal.integer(Some(Ior.both(0, 65)), None, None))

      // TODO: Max scale is 38 for version >= 10.2.1, need to condition on runtime
      //       version if we want to support this, using conservative value for now.
      val scaleParam: Labeled[Formal[Int]] =
        Labeled("Scale", Formal.integer(Some(Ior.both(0, 30)), None, None))

      Constructor.Ternary(
        precisionParam,
        scaleParam,
        SignednessParam,
        DECIMAL(_, _, _))
    }
  }

  final case class CHAR(length: Int) extends MariaDbType(s"CHAR($length) CHARACTER SET utf8mb4")
  case object CHAR extends MariaDbTypeId.HigherKinded(9) {
    val constructor = Constructor.Unary(LengthCharsParam(255), CHAR(_))
  }

  final case class VARCHAR(length: Int) extends MariaDbType(s"VARCHAR($length) CHARACTER SET utf8mb4")
  case object VARCHAR extends MariaDbTypeId.HigherKinded(10) {
    val constructor = Constructor.Unary(LengthCharsParam(16383), VARCHAR(_))
  }

  final case class BINARY(length: Int) extends MariaDbType(s"BINARY($length)")
  case object BINARY extends MariaDbTypeId.HigherKinded(11) {
    val constructor = Constructor.Unary(LengthBytesParam, BINARY(_))
  }

  final case class VARBINARY(length: Int) extends MariaDbType(s"VARBINARY($length)")
  case object VARBINARY extends MariaDbTypeId.HigherKinded(12) {
    val constructor = Constructor.Unary(LengthBytesParam, VARBINARY(_))
  }

  final case object TINYBLOB extends MariaDbTypeId.SelfIdentified("TINYBLOB", 13)
  final case object BLOB extends MariaDbTypeId.SelfIdentified("BLOB", 14)
  final case object MEDIUMBLOB extends MariaDbTypeId.SelfIdentified("MEDIUMBLOB", 15)
  final case object LONGBLOB extends MariaDbTypeId.SelfIdentified("LONGBLOB", 16)

  final case object TINYTEXT extends MariaDbTypeId.SelfIdentified("TINYTEXT CHARACTER SET utf8mb4", 17)
  final case object TEXT extends MariaDbTypeId.SelfIdentified("TEXT CHARACTER SET utf8mb4", 18)
  final case object MEDIUMTEXT extends MariaDbTypeId.SelfIdentified("MEDIUMTEXT CHARACTER SET utf8mb4", 19)
  final case object LONGTEXT extends MariaDbTypeId.SelfIdentified("LONGTEXT CHARACTER SET utf8mb4", 20)

  final case object YEAR extends MariaDbTypeId.SelfIdentified("YEAR", 21)
  final case object DATE extends MariaDbTypeId.SelfIdentified("DATE", 22)

  final case class TIME(precision: Int) extends MariaDbType(s"TIME($precision)")
  case object TIME extends MariaDbTypeId.HigherKinded(23) {
    val constructor = Constructor.Unary(MicrosParam, TIME(_))
  }

  final case class DATETIME(precision: Int) extends MariaDbType(s"DATETIME($precision)")
  case object DATETIME extends MariaDbTypeId.HigherKinded(24) {
    val constructor = Constructor.Unary(MicrosParam, DATETIME(_))
  }

  ////

  private val SignednessParam: Labeled[Formal[Signedness]] = {
    import Signedness._

    val ss =
      NonEmptyList.of(SIGNED, UNSIGNED, ZEROFILL)
        .map(s => s.toString -> s)

    Labeled("Signedness", Formal.enum(ss.head, ss.tail: _*))
  }

  private def LengthCharsParam(max: Int): Labeled[Formal[Int]] =
    Labeled("Length (characters)", Formal.integer(Some(Ior.both(0, max)), None, None))

  private val LengthBytesParam: Labeled[Formal[Int]] =
    Labeled("Length (bytes)", Formal.integer(Some(Ior.left(0)), None, None))

  private val MicrosParam: Labeled[Formal[Int]] =
    Labeled("Microsecond Precision", Formal.integer(Some(Ior.both(0, 6)), None, None))

  private def numericConstructor(f: Signedness => MariaDbType): Constructor[MariaDbType] =
    Constructor.Unary(SignednessParam, f)
}

sealed trait MariaDbTypeId extends Product with Serializable {
  def ordinal: Int
}

object MariaDbTypeId {
  import MariaDbType._

  sealed abstract class SelfIdentified(spec: String, val ordinal: Int)
      extends MariaDbType(spec) with MariaDbTypeId

  sealed abstract class HigherKinded(val ordinal: Int) extends MariaDbTypeId {
    def constructor: Constructor[MariaDbType]
  }

  val allIds: Set[MariaDbTypeId] =
    Set(
      BOOLEAN,

      TINYINT,
      SMALLINT,
      MEDIUMINT,
      INT,
      BIGINT,

      FLOAT,
      DOUBLE,
      DECIMAL,

      CHAR,
      VARCHAR,
      BINARY,
      VARBINARY,

      TINYBLOB,
      BLOB,
      MEDIUMBLOB,
      LONGBLOB,

      TINYTEXT,
      TEXT,
      MEDIUMTEXT,
      LONGTEXT,

      YEAR,
      DATE,
      TIME,
      DATETIME)
}
