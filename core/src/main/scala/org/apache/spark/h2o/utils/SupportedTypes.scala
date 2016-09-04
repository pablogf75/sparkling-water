/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.h2o.utils

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import water.fvec.Vec

import scala.language.postfixOps
import scala.reflect.runtime.universe._
import language.implicitConversions

/**
  * All type associations are gathered in this file.
  */
object SupportedTypes extends Enumeration {

  type VecType = Byte

  final case class SupportedType[T](
                                vecType  : VecType,
                                sparkType: DataType,
                                javaClass: Class[_], // note, not always T, since T is scala class
                                types    : Type*
      ) extends Val {
    def matches(t: Type) = types exists (t <:<)
  }

  import java.{lang => jl, sql => js}

  import scala.reflect.runtime.universe.definitions._

  val Boolean   = SupportedType[scala.Boolean] (Vec.T_NUM,  BooleanType,   classOf[jl.Boolean  ], BooleanTpe, typeOf[jl.Boolean])
  val Byte      = SupportedType[scala.Byte   ] (Vec.T_NUM,  ByteType,      classOf[jl.Byte     ], ByteTpe,    typeOf[jl.Byte])
  val Short     = SupportedType[scala.Short  ] (Vec.T_NUM,  ShortType,     classOf[jl.Short    ], ShortTpe,   typeOf[jl.Short])
  val Integer   = SupportedType[scala.Int    ] (Vec.T_NUM,  IntegerType,   classOf[jl.Integer  ], IntTpe,     typeOf[jl.Integer])
  val Long      = SupportedType[scala.Long   ] (Vec.T_NUM,  LongType,      classOf[jl.Long     ], LongTpe,    typeOf[jl.Long])
  val Float     = SupportedType[scala.Float  ] (Vec.T_NUM,  FloatType,     classOf[jl.Float    ], FloatTpe,   typeOf[jl.Float])
  val Double    = SupportedType[scala.Double ] (Vec.T_NUM,  DoubleType,    classOf[jl.Double   ], DoubleTpe,  typeOf[jl.Double])
  val Timestamp = SupportedType[js.Timestamp ] (Vec.T_TIME, TimestampType, classOf[js.Timestamp], typeOf[js.Timestamp])
  val String    = SupportedType[String       ] (Vec.T_STR,  StringType,    classOf[String],       typeOf[String])
  val UUID      = SupportedType[String       ] (Vec.T_UUID, StringType,    classOf[String],       typeOf[String])
  val Category  = SupportedType[String       ] (Vec.T_CAT,  StringType,    classOf[String],       typeOf[String])
  val UTF8      = SupportedType[String       ] (Vec.T_STR,  StringType,    classOf[String],       typeOf[UTF8String]) // todo

  private implicit def val2type(v: Value): SupportedType[_] = v.asInstanceOf[SupportedType[_]]

  val all: List[SupportedType[_]] = values.toList map val2type

  private def indexBy[F](f: SupportedType[_] => F): Map[F, SupportedType[_]] = all map (t => f(t) -> t) toMap

  val ClassIndex: Map[Class[_], SupportedType[_]] = indexBy (_.javaClass)

  val VecTypeIndex: Map[VecType, SupportedType[_]] = indexBy (_.vecType)

  val SparkIndex: Map[DataType, SupportedType[_]] = indexBy (_.sparkType)
}
