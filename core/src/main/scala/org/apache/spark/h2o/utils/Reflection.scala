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
import water.fvec.Vec

import scala.reflect.runtime.universe._

/**
  * Created by vpatryshev on 9/1/16.
  */
object Reflection extends Enumeration {

  type VecType = Byte

  final case class TypeSpec[T](
                                 vecType  : VecType,
                                 javaClass: Class[_], // note, it's not T, since T is scala class... or do we need scala reflection?
                                 scalaType: Type,
                                 sparkType: DataType
      ) extends Val {
  }

  import java.{lang => jl, sql => js}

  import scala.reflect.runtime.universe.definitions._

  val Boolean   = TypeSpec[scala.Boolean] (Vec.T_NUM,  classOf[jl.Boolean  ], BooleanTpe, BooleanType)
  val Byte      = TypeSpec[scala.Byte   ] (Vec.T_NUM,  classOf[jl.Byte     ], ByteTpe,    ByteType)
  val Short     = TypeSpec[scala.Short  ] (Vec.T_NUM,  classOf[jl.Short    ], ShortTpe,   ShortType)
  val Integer   = TypeSpec[scala.Int    ] (Vec.T_NUM,  classOf[jl.Integer  ], IntTpe,     IntegerType)
  val Long      = TypeSpec[scala.Long   ] (Vec.T_NUM,  classOf[jl.Long     ], LongTpe,    LongType)
  val Float     = TypeSpec[scala.Float  ] (Vec.T_NUM,  classOf[jl.Float    ], FloatTpe,   FloatType)
  val Double    = TypeSpec[scala.Double ] (Vec.T_NUM,  classOf[jl.Double   ], DoubleTpe,  DoubleType)
  val Timestamp = TypeSpec[js.Timestamp ] (Vec.T_TIME, classOf[js.Timestamp], NothingTpe, TimestampType)
  val UUID      = TypeSpec[String       ] (Vec.T_UUID, classOf[Nothing],      NothingTpe, StringType)
  val Category  = TypeSpec[String       ] (Vec.T_CAT,  classOf[Nothing],      NothingTpe, StringType)
  val String    = TypeSpec[String       ] (Vec.T_STR,  classOf[String],       NothingTpe, StringType)
  val UTF8      = TypeSpec[String       ] (Vec.T_STR,  classOf[String],       NothingTpe, StringType) // todo

  private implicit def val2info(v: Value) = v.asInstanceOf[TypeSpec[_]]

  val ClassMap: Map[Class[_], Byte] = values map(t => t.javaClass -> t.vecType) toMap

  def typeSpecOf(value : Any): TypeSpec[_] = {
    value match {
      case n: Byte => Byte
      case n: Short => Short
      case n: Int => Integer
      case n: Long => Long
      case n: Float => Float
      case n: Double => Double
      case n: Boolean => Boolean
      case n: String => String
      case n: java.sql.Timestamp => Timestamp
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }

  /*
    def typ(tpe: `Type`) : Class[_] = {
  tpe match {
    // Unroll Option[_] type
    case t if t <:< typeOf[Option[_]] =>
      val TypeRef(_, _, Seq(optType)) = t
      typ(optType)
    case t if t <:< typeOf[String]            => classOf[String]
    case t if t <:< typeOf[java.lang.Integer] => classOf[java.lang.Integer]
    case t if t <:< typeOf[java.lang.Long]    => classOf[java.lang.Long]
    case t if t <:< typeOf[java.lang.Double]  => classOf[java.lang.Double]
    case t if t <:< typeOf[java.lang.Float]   => classOf[java.lang.Float]
    case t if t <:< typeOf[java.lang.Short]   => classOf[java.lang.Short]
    case t if t <:< typeOf[java.lang.Byte]    => classOf[java.lang.Byte]
    case t if t <:< typeOf[java.lang.Boolean] => classOf[java.lang.Boolean]
    case t if t <:< definitions.IntTpe        => classOf[java.lang.Integer]
    case t if t <:< definitions.LongTpe       => classOf[java.lang.Long]
    case t if t <:< definitions.DoubleTpe     => classOf[java.lang.Double]
    case t if t <:< definitions.FloatTpe      => classOf[java.lang.Float]
    case t if t <:< definitions.ShortTpe      => classOf[java.lang.Short]
    case t if t <:< definitions.ByteTpe       => classOf[java.lang.Byte]
    case t if t <:< definitions.BooleanTpe    => classOf[java.lang.Boolean]
    case t if t <:< typeOf[java.sql.Timestamp] => classOf[java.sql.Timestamp]
    case t => throw new IllegalArgumentException(s"Type $t is not supported!")
  }
}

/**
 * Return catalyst structural type for given H2O vector.
 *
 * The mapping of type is flat, if type is unrecognized
 * {@link IllegalArgumentException} is thrown.
 *
 * @param v H2O vector
 * @return catalyst data type
 */
def vecTypeToDataType(v: Vec): DataType = {
  v.get_type() match {
    case Vec.T_BAD  => ByteType // vector is full of NAs, use any type
    case Vec.T_NUM  => numericVecTypeToDataType(v)
    case Vec.T_CAT  => StringType
    case Vec.T_UUID => StringType
    case Vec.T_STR  => StringType
    case Vec.T_TIME => TimestampType
    case typ => throw new IllegalArgumentException("Unknown vector type " + typ)
  }
}

def numericVecTypeToDataType(v: Vec): DataType = {
  if (v.isInt) {
    val min = v.min()
    val max = v.max()
    if (min > Byte.MinValue && max < Byte.MaxValue) {
      ByteType
    } else if (min > Short.MinValue && max < Short.MaxValue) {
      ShortType
    } else if (min > Int.MinValue && max < Int.MaxValue) {
      IntegerType
    } else {
      LongType
    }
  } else DoubleType
}

/** Method translating SQL types into Sparkling Water types */
def dataTypeToVecType(dt : DataType) : Byte = dt match {
  case BinaryType  => Vec.T_NUM
  case ByteType    => Vec.T_NUM
  case ShortType   => Vec.T_NUM
  case IntegerType => Vec.T_NUM
  case LongType    => Vec.T_NUM
  case FloatType   => Vec.T_NUM
  case DoubleType  => Vec.T_NUM
  case BooleanType => Vec.T_NUM
  case TimestampType => Vec.T_TIME
  case StringType  => Vec.T_STR
  //case StructType  => dt.
  case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
}

   */
}
