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

package org.apache.spark.ml.tuning

import scala.util.Random

import com.sigopt.Sigopt
import com.sigopt.model.{Assignments, Bounds, Experiment, Parameter, Suggestion, Observation}

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.collection.JavaConverters._

/**
 * :: Experimental ::
 * K-fold cross validation.
 */
@Experimental
class SigOptCrossValidator (override val uid: String) extends CrossValidator {
  var numIterations: Int = 0
  var experimentId: String = ""

  def setNumIterations(i: Int) {
    this.numIterations = i
  }

  def setExperimentId(i: String) {
    this.experimentId = i
  }

  def setSigCV(name: String, token: String, iters: Int, parameters: Seq[(String, Double, Double, String)]) = {
    Sigopt.clientToken = token
    this.setNumIterations(iters)
    val e: Experiment = Experiment.create()
      .data(new Experiment.Builder()
        .name(name)
        .parameters(parameters.map({ case (name, min, max, typ) => {
          new Parameter.Builder()
            .name(name)
            .`type`(typ)
            .bounds(new Bounds.Builder()
              .min(min)
              .max(max)
              .build())
            .build()}}).asJava)
        .build())
      .call()
    this.setExperimentId(e.getId())
  }

  def createSuggestion(estimator: Estimator[_]): Suggestion = {
    new Experiment(this.experimentId).suggestions().create().call()
  }

  def setEstimatorParamMapsFromAssignments(estimator: Estimator[_], assignments: Assignments) = {
    val assignmentsMap: Map[String, Double] = assignments.asScala.toMap.asInstanceOf[Map[String, Double]]
    val paramMap = ParamMap()
    for {
      (paramName, value) <- assignmentsMap
    } paramMap.put(estimator.getParam(paramName), value)
    this.setEstimatorParamMaps(Array(paramMap))
  }

  def createObservation(s: Suggestion, metric: Double): Observation = {
    new Experiment(this.experimentId).observations().create()
      .data(new Observation.Builder()
        .suggestion(s.getId())
        .value(metric)
        .build())
      .call()
  }

  override def fit(dataset: DataFrame): CrossValidatorModel = {
    val sqlContext = dataset.sqlContext
    val schema = dataset.schema
    transformSchema(schema)
    val est = $(estimator)
    val eval = $(evaluator)
    val numModels = this.numIterations
    var i = 0
    val observations = for {
      _ <- (1 to this.numIterations)
    } yield {
      val suggestion = this.createSuggestion(est)
      this.setEstimatorParamMapsFromAssignments(est, suggestion.getAssignments())
      val epm = $(estimatorParamMaps)
      val splits = MLUtils.kFold(dataset.rdd, $(numFolds), Random.nextInt)
      val accMetrics = for {
        (training, validation) <- splits
      } yield {
        val trainingDataset = sqlContext.createDataFrame(training, schema).cache();
        val validationDataset = sqlContext.createDataFrame(validation, schema).cache();
        val models = est.fit(trainingDataset, epm(0))
        eval.evaluate((models.asInstanceOf[Model[_]]).transform(validationDataset, epm(0)))
      }
      val avgMetric: Double = (accMetrics).sum / (accMetrics).length
      this.createObservation(suggestion, avgMetric)
    }
    val bestObservation = if (eval.isLargerBetter) observations.maxBy(_.getValue) else observations.minBy(_.getValue)
    this.setEstimatorParamMapsFromAssignments(est, bestObservation.getAssignments())
    val epm = $(estimatorParamMaps)
    val bestModel = (est.fit(dataset, epm(0))).asInstanceOf[Model[_]]
    val metrics = observations.toArray.map(_.getValue().doubleValue())
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }

  override def copy(extra: ParamMap): SigOptCrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[SigOptCrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied.setExperimentId(this.experimentId)
    copied.setNumIterations(this.numIterations)
    copied
  }
}
