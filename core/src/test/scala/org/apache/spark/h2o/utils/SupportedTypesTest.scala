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
import org.apache.spark.unsafe.types.UTF8String
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import ReflectionUtils._

import scala.reflect.runtime.universe._

/**
  * Tests for type info handling
  */
@RunWith(classOf[JUnitRunner])
class SupportedTypesTest extends FunSuite {

  test("Compare ClassIndex with the old version") {
    val oldKeys = OldH2OTypeUtils.dataTypeToVecType.keySet
    val newKeys = ClassIndex.keySet
    assert(oldKeys.diff(newKeys).isEmpty)
    assert(OldH2OTypeUtils.dataTypeToVecType ==ClassIndex.filterKeys(oldKeys.contains).mapValues(_.vecType))
  }

  test ("Compare with oldVersion of dataTypeTOVecType") {
    import org.apache.spark.sql.types._
    val samples = ByteType::ShortType::IntegerType::LongType::FloatType::DoubleType::StringType::TimestampType::Nil
    val oldStuff = samples map OldReflectionUtils.dataTypeToVecType
    val newStuff = samples map SparkIndex map (_.vecType)
    assert(oldStuff == newStuff)
  }

  // TODO(vlad): move to ReflectionUtilsTest
  test("Infer type from a value") {
    def mustBe[T](expected: SupportedType[T], value: T) = assert(supportedTypeOf(value) == expected)

    mustBe(Boolean, true)
    mustBe(Boolean, false)
    mustBe(Byte, 1.asInstanceOf[Byte])
    mustBe(Byte, -1.asInstanceOf[Byte])
    mustBe(Short, 2.asInstanceOf[Short])
    mustBe(Short, -2.asInstanceOf[Short])
    mustBe(Integer, 3)
    mustBe(Integer, -3)
    mustBe(Long, 4L)
    mustBe(Long, -4L)
    mustBe(Float, 5.1f)
    mustBe(Float, -5.2f)
    mustBe(Double, 6.3)
    mustBe(Double, -6.3)
    mustBe(Double, math.Pi)
    mustBe(Timestamp, new java.sql.Timestamp(1472777182999L))
    mustBe(String, "")
    mustBe(String, "Hello H2O")
  }

  test("Fail to infer type from a weird value") {
    def mustFail[T](msg: String, value: T) = try {
      val t = supportedTypeOf(value); fail(s"Not acceptable: $msg: got $t")
    } catch {
      case iae: IllegalArgumentException => ;//success
    }

    mustFail("Null", null)
    mustFail("Date", new java.util.Date(12345L))
    mustFail("Option(None)", None)
    mustFail("Option(Some)", Some("one"))
  }

  // TODO(Vlad): move to ReflectionUtilsTest
  test("Infer type from scala type") {
    import scala.reflect.runtime.universe.definitions._

    def mustBe[T](expected: SupportedType[T], t: Type) = assert(ReflectionUtils.supportedTypeFor(t) == expected)

    mustBe(Boolean, BooleanTpe)
    mustBe(Boolean, typeOf[scala.Boolean])
    mustBe(Boolean, typeOf[java.lang.Boolean])
    mustBe(Byte, ByteTpe)
    mustBe(Byte, typeOf[scala.Byte])
    mustBe(Byte, typeOf[java.lang.Byte])
    mustBe(Short, ShortTpe)
    mustBe(Short, typeOf[scala.Short])
    mustBe(Short, typeOf[java.lang.Short])
    mustBe(Integer, IntTpe)
    mustBe(Integer, typeOf[scala.Int])
    mustBe(Integer, typeOf[java.lang.Integer])
    mustBe(Long, LongTpe)
    mustBe(Long, typeOf[scala.Long])
    mustBe(Long, typeOf[java.lang.Long])
    mustBe(Float, FloatTpe)
    mustBe(Float, typeOf[scala.Float])
    mustBe(Float, typeOf[java.lang.Float])
    mustBe(Double, DoubleTpe)
    mustBe(Double, typeOf[scala.Double])
    mustBe(Double, typeOf[java.lang.Double])
    mustBe(Timestamp,typeOf[java.sql.Timestamp])
    mustBe(String, typeOf[String])
    mustBe(UTF8, typeOf[UTF8String])
  }

}
