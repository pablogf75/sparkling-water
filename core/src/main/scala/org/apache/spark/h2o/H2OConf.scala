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

package org.apache.spark.h2o

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.h2o.backends.external.ExternalBackendConf
import org.apache.spark.h2o.backends.internal.InternalBackendConf
import org.apache.spark.{Logging, SparkContext, SparkEnv}

/**
  * Configuration holder which is representing
  * properties passed from user to Sparkling Water.
  */
class H2OConf(@transient val sc: SparkContext) extends Logging with ExternalBackendConf with InternalBackendConf {

  /** Support for creating H2OConf in Java environments */
  def this(jsc: JavaSparkContext) = this(jsc.sc)
  val sparkConf = sc.getConf
  // Precondition
  require(sparkConf != null, "Spark conf was null")

  /** Copy this object */
  override def clone: H2OConf = {
    new H2OConf(sc).setAll(getAll)
  }

  /** Set a configuration variable. */
  def set(key: String, value: String): H2OConf = {
    sparkConf.set(key, value)
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): H2OConf = {
    sparkConf.remove(key)
    this
  }

  def contains(key: String): Boolean = sparkConf.contains(key)

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = sparkConf.get(key)

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = sparkConf.get(key, defaultValue)

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = sparkConf.getOption(key)

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    sparkConf.getAll
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): H2OConf = {
    sparkConf.setAll(settings)
    this
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = sparkConf.getInt(key, defaultValue)

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = sparkConf.getLong(key, defaultValue)

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = sparkConf.getDouble(key, defaultValue)

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = sparkConf.getBoolean(key, defaultValue)


  override def toString: String = {
    if(runsInExternalClusterMode){
      externalConfString
    }else{
      internalConfString
    }
  }
}

class SparklingEnv(val h2oConf: H2OConf) {

}
/**
  * Similar object to SparkEnv which helps us to get H2OConf and other configuration
  * at the state it is at start of H2OContext
  */
object SparklingEnv {
  private var h2oConf: Option[H2OConf] = None
  private[h2o] def setConf(h2oConf: H2OConf) {
    this.h2oConf = Some(h2oConf)
  }

  /**
    * Returns the H2OConf
    *
    * Note this method can be used after H2OContext has been started, otherwise it ends up with an exception
    */
  def getConf: H2OConf = h2oConf.getOrElse( throw new RuntimeException("H2OContext has to be started in order to get the configuration"))
}
