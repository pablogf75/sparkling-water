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

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._

/**
  * All type associations are gathered in this file.
  */
object SupportedTypes extends Enumeration {

  type VecType = Byte

  trait SupportedType {
    def vecType: VecType
    def sparkType: DataType
    def javaClass: Class[_]
    def matches(t: Type): Boolean
  }

  final case class SimpleType[T : TypeTag](
                                vecType  : VecType,
                                sparkType: DataType,
                                javaClass: Class[_], // note, not always T, since T is scala class
                                defaultValue: T,
                                private val extraTypes : Type*
      ) extends Val with SupportedType {
    lazy val types = typeOf[T]::extraTypes.toList
    def matches(t: Type) = {
      types exists (t <:<)
    }
  }

  final case class OptionalType[T](content: SupportedType) extends Val with SupportedType {
    override def vecType: VecType = content.vecType
    override def sparkType: DataType = content.sparkType
    override def javaClass: Class[_] = content.javaClass
    override def matches(t: Type): Boolean = {
      t match {
        case TypeRef(_, _, Seq(optType)) =>
          content matches optType
        case _ => false
      }
    }
  }

  import java.{lang => jl, sql => js}

  import scala.reflect.runtime.universe.definitions._

  private val ZeroTime = new js.Timestamp(0L)

  val Boolean   = SimpleType[scala.Boolean] (Vec.T_NUM,  BooleanType,   classOf[jl.Boolean  ], false,      BooleanTpe, typeOf[jl.Boolean])
  val Byte      = SimpleType[scala.Byte   ] (Vec.T_NUM,  ByteType,      classOf[jl.Byte     ], 0.toByte,   ByteTpe,    typeOf[jl.Byte])
  val Short     = SimpleType[scala.Short  ] (Vec.T_NUM,  ShortType,     classOf[jl.Short    ], 0.toShort,  ShortTpe,   typeOf[jl.Short])
  val Integer   = SimpleType[scala.Int    ] (Vec.T_NUM,  IntegerType,   classOf[jl.Integer  ], 0,          IntTpe,     typeOf[jl.Integer])
  val Long      = SimpleType[scala.Long   ] (Vec.T_NUM,  LongType,      classOf[jl.Long     ], 0L,         LongTpe,    typeOf[jl.Long])
  val Float     = SimpleType[scala.Float  ] (Vec.T_NUM,  FloatType,     classOf[jl.Float    ], scala.Float.NaN,  FloatTpe, typeOf[jl.Float])
  val Double    = SimpleType[scala.Double ] (Vec.T_NUM,  DoubleType,    classOf[jl.Double   ], scala.Double.NaN, DoubleTpe, typeOf[jl.Double])
  val Timestamp = SimpleType[js.Timestamp ] (Vec.T_TIME, TimestampType, classOf[js.Timestamp], ZeroTime)
  val String    = SimpleType[String       ] (Vec.T_STR,  StringType,    classOf[String],       null)
  val UUID      = SimpleType[String       ] (Vec.T_UUID, StringType,    classOf[String],       null)
  val Category  = SimpleType[String       ] (Vec.T_CAT,  StringType,    classOf[String],       null)
  // todo(vlad): figure out if this is the right thing
  val UTF8      = SimpleType[UTF8String   ] (Vec.T_STR,  StringType,    classOf[String],       null,       typeOf[UTF8String])

  private implicit def val2type(v: Value): SimpleType[_] = v.asInstanceOf[SimpleType[_]]

  val allSimple: List[SimpleType[_]] = values.toList map val2type

  val allOptional = allSimple map OptionalType.apply

  val all: List[SupportedType] = allSimple ++ allOptional

  private def indexBy[F](f: SupportedType => F): Map[F, SupportedType] = allSimple map (t => f(t) -> t) toMap

  val ClassIndex: Map[Class[_], SupportedType] = indexBy (_.javaClass)

  val VecTypeIndex: Map[VecType, SupportedType] = indexBy (_.vecType)

  val SparkIndex: Map[DataType, SupportedType] = indexBy (_.sparkType)
}
