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
package org.apache.spark.ml

import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.spark.models.MissingValuesHandling
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{DataTypes, StructField}
import water.fvec.{Frame, H2OFrame}

object FrameMLUtils {
  def toLabeledPoints(parms: Frame,
                      _response_column: String,
                      nfeatures: Int,
                      means: Array[Double],
                      missingHandler: MissingValuesHandling,
                      h2oContext: H2OContext,
                      sqlContext: SQLContext): RDD[LabeledPoint] = {
    val domains = parms.domains()

    val trainingDF = h2oContext.asDataFrame(new H2OFrame(parms))(sqlContext)
    val fields: Array[StructField] = trainingDF.schema.fields
    var trainingRDD = trainingDF.rdd

    if (MissingValuesHandling.Skip.eq(missingHandler)) {
      trainingRDD = trainingRDD.filter(_.anyNull)
    } else if(MissingValuesHandling.MeanImputation.eq(missingHandler)) {
      (0 until nfeatures).
        foreach( i => means(i) = trainingRDD.map(row => toDouble(row.get(i),fields(i),domains(i))).mean())
    }

    trainingRDD.map(row => {
      val features = new Array[Double](nfeatures)
      (0 until nfeatures).foreach(i => features(i) = if(row.isNullAt(i)) means(i) else toDouble(row.get(i), fields(i), domains(i)))

      new LabeledPoint(
        toDouble(row.getAs[String](_response_column), fields(fields.length - 1), domains(domains.length - 1)),
        Vectors.dense(features)
      )
    })
  }

  private def toDouble(value: Any, fieldStruct: StructField, domain: Array[String]): Double = {
    fieldStruct.dataType match {
      case DataTypes.ByteType => value.asInstanceOf[Byte].doubleValue
      case DataTypes.ShortType => value.asInstanceOf[Short].doubleValue
      case DataTypes.IntegerType => value.asInstanceOf[Integer].doubleValue
      case DataTypes.DoubleType => value.asInstanceOf[Double]
      case DataTypes.StringType => domain.indexOf(value)
      case _ => throw new IllegalArgumentException("Target column has to be an enum or a number. " + fieldStruct.toString)
    }
  }
}
