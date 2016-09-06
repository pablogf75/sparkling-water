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

import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.sql.types._
import water.api.API
import water.fvec.Vec
import language.postfixOps

/**
 * Work with reflection only inside this helper.
 */
object ReflectionUtils {
  import scala.reflect.runtime.universe._

  def fieldNamesOf[T: TypeTag] : Array[String] = {
    typeOf[T].members.sorted.collect { case m if !m.isMethod => m.name.toString.trim }.toArray
  }

  def vecTypesOf[T:TypeTag]: Array[VecType] = memberTypesOf[T] map (_.vecType)

  def memberTypesOf[T](implicit ttag: TypeTag[T]): Array[SupportedType] = {
    val st = typeOf[T]
    val nameFilter = fieldNamesOf[T]
    supportedTypesOf[T](nameFilter)
  }

  def listMemberTypes(st: Type, nameFilter: Array[String]): Array[Type] = {
    val formalTypeArgs = st.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = st
    val attr = st.members.sorted
      .filter(!_.isMethod)
      .filter( s => nameFilter.contains(s.name.toString.trim))
      .map( s =>
        s.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
      )
    attr toArray
  }

  private def typeName(t: Type): String = {
    val name = classFor(t).getSimpleName
    if (t <:< typeOf[Option[_]]) s"Option[$name]" else name
  }

  def typeNamesOf[T: TypeTag](nameFilter: Array[String]): Array[String] = {
    val types: Seq[Type] = listMemberTypes(typeOf[T], nameFilter)
    nameTheseTypes(types)
  }

  private def nameTheseTypes(tt: Seq[Type]) : Array[String] = {
    tt map typeName toArray
  }

  private def classesOf(tt: Seq[Type]) : Array[Class[_]] = {
    (tt map classFor).toArray
  }

  def supportedTypesOf[T: TypeTag](nameFilter: Array[String]): Array[SupportedType] = {
    val types: Seq[Type] = listMemberTypes(typeOf[T], nameFilter)
    types map supportedTypeFor toArray
  }

  def reflector(ref: AnyRef) = new {
    def getV[T](name: String): T = ref.getClass.getMethods.find(_.getName == name).get.invoke(ref).asInstanceOf[T]
    def setV(name: String, value: Any): Unit = ref.getClass.getMethods.find(_.getName == name + "_$eq").get.invoke(ref, value.asInstanceOf[AnyRef])
  }

  /** Return API annotation assigned with the given field
    * or null.
    *
    * @param klazz  class to query
    * @param fieldName  field name to query
    * @return instance of API annotation assigned with the field or null
    */
  def api(klazz: Class[_], fieldName: String): API = {
    klazz.getField(fieldName).getAnnotation(classOf[API])
  }

  import scala.reflect.runtime.universe._

  def supportedTypeOf(value : Any): SupportedType = {
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

  def supportedTypeFor(tpe: Type) : SupportedType = {
    if (tpe <:< typeOf[Option[_]]) {
      val TypeRef(_, _, Seq(optType)) = tpe
      supportedTypeFor(optType)
    } else {
      SupportedTypes.all find (_.matches(tpe)) getOrElse {
        throw new IllegalArgumentException(s"Type $tpe is not supported!")
      }
    }
  }

  def classFor(tpe: Type) : Class[_] = supportedTypeFor(tpe).javaClass

  def vecTypeFor(t: Class[_]): Byte = ClassIndex(t).vecType

  def vecTypeFor(t: Type): Byte = vecTypeFor(classFor(t))

  def vecTypeOf[T](implicit ttag: TypeTag[T]) = vecTypeFor(typeOf[T])

  /** Method translating SQL types into Sparkling Water types */
  def vecTypeFor(dt : DataType) : Byte = SparkIndex(dt).vecType

  import SupportedTypes._

  /**
    * Return catalyst structural type for given H2O vector.
    *
    * The mapping of type is flat.
    * @throws IllegalArgumentException if type is recognized
    *
    * @param v H2O vector
    * @return catalyst data type
    */
  def dataTypeFor(v: Vec): DataType = supportedType(v).sparkType

  def memberTypes(p: Product) = p.productIterator map supportedTypeOf toArray

  def supportedType(v: Vec): SupportedType = {
    v.get_type() match {
      case Vec.T_BAD  => Byte // vector is full of NAs, use any type
      case Vec.T_NUM  => detectSupportedNumericType(v)
      case Vec.T_CAT  => String
      case Vec.T_UUID => String
      case Vec.T_STR  => String
      case Vec.T_TIME => Timestamp
      case typ => throw new IllegalArgumentException("Unknown vector type " + typ)
    }
  }

  private def detectSupportedNumericType(v: Vec): SupportedType = {
    if (v.isInt) {
      val min = v.min()
      val max = v.max()
      if (min > scala.Byte.MinValue && max < scala.Byte.MaxValue) {
        Byte
      } else if (min > scala.Short.MinValue && max < scala.Short.MaxValue) {
        Short
      } else if (min > scala.Int.MinValue && max < scala.Int.MaxValue) {
        Integer
      } else {
        Long
      }
    } else Double
  }
}
