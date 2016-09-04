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

package org.apache.spark.h2o.backends.internal

import java.util.UUID

import org.apache.spark.h2o.converters.ReadConverterContext
import org.apache.spark.sql.types.DataType
import water.fvec.{Chunk, Frame, Vec}
import water.parser.BufferedString
import water.{DKV, Key}

class InternalReadConverterContext(override val keyName: String, override val chunkIdx: Int) extends ReadConverterContext{
  /** Lazily fetched H2OFrame from K/V store */
  private lazy val fr: Frame = underlyingFrame

  /** Chunks for this partition */
  private lazy val chks: Array[Chunk] = water.fvec.FrameUtils.getChunks(fr, chunkIdx)

  /** Number of rows in this partition */
  private lazy val nrows = chks(0)._len

  override def isNA(columnNum: Int): Boolean = chks(columnNum).isNA(rowIdx)

  private def get[T](columnNum: Int, read: Chunk => T): Option[T] = {
    for {
      chunk <- Option(chks(columnNum)) if !chunk.isNA(rowIdx)
      data <- Option(read(chunk))
    } yield data
  }

  override def getLong(columnNum: Int): Option[Long] =  get(columnNum, _.at8(rowIdx))
  override def getDouble(columnNum: Int): Option[Double] = get(columnNum, _.atd(rowIdx))

  private def categoricalString(chunk: Chunk) = chunk.vec().domain()(chunk.at8(rowIdx).toInt)

  private def uuidString(chunk: Chunk) = new UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx)).toString

  private def plainString(chunk: Chunk) = chunk.atStr(new BufferedString(), rowIdx).toString

  val StringProviders = Map[Byte, (Chunk => String)](
    Vec.T_CAT -> categoricalString,
    Vec.T_UUID -> uuidString,
    Vec.T_STR -> plainString
  ) withDefault((t:Byte) => {
    assert(assertion = false, s"Should never be here, type is $t")
    // TODO(vlad): this is temporarily here, to provide None is string is missing
    (_:Chunk) => null
  }
  )

  private def stringProvider(columnNum: Int): (Chunk => String) = StringProviders(fr.vec(columnNum).get_type())


  override def getString(columnNum: Int): Option[String] = get(columnNum, stringProvider(columnNum))

  private def underlyingFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]

  override def numRows: Int = nrows

  private val DefaultReader: Reader = _ => None

  /**
    * Given a a column number, returns an Option[T]
    * with the value parsed according to TypeName.
    * You can override it.
    *
    * A map from type name to option reader
    */
  lazy val OptionReaders: Map[TypeName, OptionReader] = Map(
    "Boolean"    -> getBoolean,
    "Byte"       -> getByte,
    "Double"     -> getDouble,
    "Float"      -> getFloat,
    "Integer"    -> getInt,
    "Long"       -> getLong,
    "Short"      -> getShort,
    "String"     -> getString,
    "UTF8String" -> getUTF8String,
    "Timestamp"  -> getTimestamp)

  /**
    * Given a type name, returns a default value for the case it's NaN in spark
    * You can override it.
    */
  val Defaults: Map[TypeName, Any] = Map(
    "Boolean"   -> false,
    "Byte"      -> 0.toByte,
    "Double"    -> Double.NaN,
    "Float"     -> Float.NaN,
    "Integer"   -> 0,
    "Long"      -> 0L,
    "Short"     -> 0.toShort,
    "String"    -> null,
    "UTFString" -> null,
    "Timestamp" -> 0L
  )

  private def twoReaders = (key: TypeName, op: OptionReader) =>
    List((s"Option[$key]":TypeName) -> op,
      key                        -> ((col: Int) => op(col).getOrElse(Defaults(key)))
    )

  private lazy val availableReaders: Map[TypeName, Reader] =
    OptionReaders flatMap twoReaders.tupled

  lazy val readerMap: Map[TypeName, Reader] = availableReaders withDefaultValue DefaultReader

  def columnValueProviders(columnIndexesWithTypes: Array[(Int, DataType)]): Array[() => Option[Any]] = {
    for {
      (i, ft) <- columnIndexesWithTypes
      provider = () => readerFor(ft)(i)
    } yield provider
  }
}
