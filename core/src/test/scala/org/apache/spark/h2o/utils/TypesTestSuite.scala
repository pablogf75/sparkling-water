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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Reflection._

/**
  * Tests for type info handling
  */
@RunWith(classOf[JUnitRunner])
class TypesTest extends FunSuite {

  test("Compare types ClassMap with the old version") {
    val oldKeys = OldH2OTypeUtils.dataTypeToVecType.keySet
    val newKeys = ClassMap.keySet
    assert(oldKeys.diff(newKeys).isEmpty)
    assert(OldH2OTypeUtils.dataTypeToVecType ==ClassMap.filterKeys(oldKeys.contains))
  }

  test("Infer type from a value") {

    def mustBe[T](expected: TypeSpec[T], value: T) = assert(typeSpecOf(value) == expected)

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
      val t = typeSpecOf(value); fail(s"Not acceptable: $msg: got $t")
    } catch {
      case iae: IllegalArgumentException => ;//success
    }

    mustFail("Null", null)
    mustFail("Date", new java.util.Date(12345L))
    mustFail("Option(None)", None)
    mustFail("Option(Some)", Some("one"))
  }
}

object TypesTestSuite {} // what is it for?
