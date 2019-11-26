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

package com.sigopt.spark

import scala.util.Random

import com.sigopt.Sigopt
import com.sigopt.model.{Assignments, Bounds, Experiment, Parameter, Suggestion, Observation}

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.tuning._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Dataset, DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType
import org.json4s.{DefaultFormats, JObject, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.apache.spark.util.Utils
import scala.collection.JavaConverters._

private[spark] case class Metadata(uid: String, params: JValue, metadata: JValue, className: String)

private[spark] trait SigOptCrossValidatorParams extends Params {
  protected def transformSchemaImpl(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  val estimator: Param[Estimator[_]] = new Param(this, "estimator", "estimator for selection")
  def getEstimator: Estimator[_] = $(estimator)

  val evaluator: Param[Evaluator] = new Param(this, "evaluator",
    "evaluator used to select hyper-parameters that maximize the validated metric")
  def getEvaluator: Evaluator = $(evaluator)

  val numFolds: IntParam = new IntParam(
    this,
    "numFolds",
    "number of folds for cross validation (>= 2)",
    ParamValidators.gtEq(2)
  )

  def getNumFolds: Int = $(numFolds)

  val numIterations: IntParam = new IntParam(
    this,
    "numIterations",
    "number of cross validations to run (>= 2)",
    ParamValidators.gtEq(2)
  )

  def getNumIterations: Int = $(numIterations)

  val experimentId: Param[String] = new Param[String](
    this,
    "experimentId",
    "SigOpt experiment ID to use"
  )

  def getExperimentId: String = $(experimentId)

  val clientToken: Param[String] = new Param[String](
    this,
    "clientToken",
    "SigOpt API Token"
  )

  def getClientToken: String = $(clientToken)

  setDefault(numFolds -> 3)
  setDefault(numIterations -> 10)
}

private[spark] object SigOptCrossValidatorParams {
  def validateParams(instance: Any) {
    // TODO(patrick): Validate params
  }

  def loadParamsInstance[T](path: String, sc: SparkContext): T = {
    val metadata = loadMetadata(path, sc)
    val cls = Class.forName(metadata.className)
    cls.getMethod("read").invoke(null).asInstanceOf[MLReader[T]].load(path)
  }

  def saveImpl(
    path: String,
    cv: SigOptCrossValidatorParams,
    sc: SparkContext
  ): Unit = {
    import org.json4s.JsonDSL._
    val jsonParams = List(
      "numFolds" -> parse(cv.numFolds.jsonEncode(cv.getNumFolds)),
      "numIterations" -> parse(cv.numIterations.jsonEncode(cv.getNumIterations)),
      "experimentId" -> parse(cv.experimentId.jsonEncode(cv.getExperimentId)),
      "clientToken" -> parse(cv.clientToken.jsonEncode(cv.getClientToken))
    )
    saveMetadata(cv, path, sc, Some(jsonParams))
    val evaluatorPath = new Path(path, "evaluator").toString
    cv.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)
    val estimatorPath = new Path(path, "estimator").toString
    cv.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
  }

  def loadImpl[M <: Model[M]](
    path: String,
    sc: SparkContext,
    expectedClassName: String
  ): (Metadata, Estimator[M], Evaluator) = {
    val metadata = loadMetadata(path, sc, expectedClassName)
    val evaluatorPath = new Path(path, "evaluator").toString
    val evaluator = loadParamsInstance[Evaluator](evaluatorPath, sc)
    val estimatorPath = new Path(path, "estimator").toString
    val estimator = loadParamsInstance[Estimator[M]](estimatorPath, sc)
    (metadata, estimator, evaluator)
  }

  def saveMetadata(
    instance: Params,
    path: String,
    sc: SparkContext,
    paramMap: Option[JValue] = None
  ): Unit = {
    import org.json4s.JsonDSL._
		val uid = instance.uid
    val cls = instance.getClass.getName
    val params = instance.extractParamMap().toSeq.asInstanceOf[Seq[ParamPair[Any]]]
    val jsonParams = paramMap.getOrElse(render(params.map { case ParamPair(p, v) =>
      p.name -> parse(p.jsonEncode(v))
    }.toList))
    val metadata = (
      ("class" -> cls) ~
      ("timestamp" -> System.currentTimeMillis()) ~
      ("sparkVersion" -> sc.version) ~
      ("uid" -> uid) ~
      ("paramMap" -> jsonParams)
    )
    val metadataJson: String = compact(render(metadata))
    val metadataPath = new Path(path, "metadata").toString
    sc.parallelize(Seq(metadataJson), 1).saveAsTextFile(metadataPath)
  }

  def loadMetadata(path: String, sc: SparkContext, expectedClassName: String = "") = {
    val metadataPath = new Path(path, "metadata").toString
    val metadataStr = sc.textFile(metadataPath, 1).first()
    val metadata = parse(metadataStr)

    implicit val format = DefaultFormats
    val uid = (metadata \ "uid").extract[String]
    val params = metadata \ "paramMap"
    val className = (metadata \ "class").extract[String]
    if (expectedClassName.nonEmpty) {
      require(
        className == expectedClassName,
        s"Error loading metadata: Expected class name $expectedClassName but found class name $className"
      )
    }

    Metadata(uid, params, metadata, className)
  }
}

@Experimental
class SigOptCrossValidator (override val uid: String)
    extends Estimator[SigOptCrossValidatorModel]
    with SigOptCrossValidatorParams
    with MLWritable {
  def this() = this(Identifiable.randomUID("cv"))

  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  /** @group setParam */
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setNumIterations(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  def setExperimentId(value: String): this.type = set(experimentId, value)

  /** @group setParam */
  def setClientToken(value: String): this.type = {
    Sigopt.clientToken = value
    set(clientToken, value)
  }

  def createExperiment(name: String, parameters: Seq[(String, Double, Double, String)]) = {
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

  def createSuggestion(experimentId: String, estimator: Estimator[_]): Suggestion = {
    new Experiment(experimentId).suggestions().create().call()
  }

  def estimatorParamMapsFromAssignments(estimator: Estimator[_], assignments: Assignments) = {
    val assignmentsMap: Map[String, Double] = assignments.asScala.toMap.asInstanceOf[Map[String, Double]]
    val paramMap = ParamMap()
    for {
      (paramName, value) <- assignmentsMap
    } paramMap.put(estimator.getParam(paramName), value)
    paramMap
  }

  def createObservation(experimentId: String, s: Suggestion, metric: Double): Observation = {
    new Experiment(experimentId).observations().create()
      .data(new Observation.Builder()
        .suggestion(s.getId())
        .value(metric)
        .build())
      .call()
  }

  override def fit(dataset: Dataset[_]): SigOptCrossValidatorModel = {
    val sqlContext = dataset.sqlContext
    val schema = dataset.schema
    transformSchema(schema)
    val est = $(estimator)
    val eval = $(evaluator)
    val folds = $(numFolds)
    val iterations = $(numIterations)
    Sigopt.clientToken = $(clientToken)
    val eid = $(experimentId)

    val experiment = Experiment.fetch(eid).call();

    val observations = for {
      _ <- (1 to iterations)
    } yield {
      val suggestion = createSuggestion(eid, est)
      val epm = this.estimatorParamMapsFromAssignments(est, suggestion.getAssignments())
      val splits = MLUtils.kFold(dataset.toDF.rdd, folds, 0)
      val accMetrics = for {
        (training, validation) <- splits
      } yield {
        val trainingDataset = sqlContext.createDataFrame(training, schema).cache()
        val validationDataset = sqlContext.createDataFrame(validation, schema).cache()
        val models = est.fit(trainingDataset, epm)
        eval.evaluate((models.asInstanceOf[Model[_]]).transform(validationDataset, epm))
      }
      val avgMetric: Double = (accMetrics).sum / (accMetrics).length
      this.createObservation(eid, suggestion, avgMetric)
    }
    val bestObservation = if (eval.isLargerBetter) observations.maxBy(_.getValue) else observations.minBy(_.getValue)
    val epm = this.estimatorParamMapsFromAssignments(est, bestObservation.getAssignments())
    val bestModel = (est.fit(dataset, epm)).asInstanceOf[Model[_]]
    copyValues(new SigOptCrossValidatorModel(uid, bestModel).setParent(this))
  }

  override def copy(extra: ParamMap): SigOptCrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[SigOptCrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    if (copied.isDefined(numFolds)) {
      copied.setNumFolds(copied.getNumFolds)
    }
    if (copied.isDefined(numIterations)) {
      copied.setNumIterations(copied.getNumIterations)
    }
    if (copied.isDefined(experimentId)) {
      copied.setExperimentId(copied.getExperimentId)
    }
    if (copied.isDefined(clientToken)) {
      copied.setClientToken(copied.getClientToken)
    }
    copied
  }

  override def write: MLWriter = new SigOptCrossValidator.SigOptCrossValidatorWriter(this)
}

object SigOptCrossValidator extends MLReadable[SigOptCrossValidator] {
  override def read: MLReader[SigOptCrossValidator] = new SigOptCrossValidatorReader
  override def load(path: String): SigOptCrossValidator = super.load(path)

  private[SigOptCrossValidator] class SigOptCrossValidatorWriter(instance: SigOptCrossValidator) extends MLWriter {
    SigOptCrossValidatorParams.validateParams(instance)
    override protected def saveImpl(path: String): Unit = {
      SigOptCrossValidatorParams.saveImpl(path, instance, sc)
    }
  }

  private class SigOptCrossValidatorReader extends MLReader[SigOptCrossValidator] {
    private val className = classOf[SigOptCrossValidator].getName
    override def load(path: String): SigOptCrossValidator = {
      val (metadata, estimator, evaluator) = SigOptCrossValidatorParams.loadImpl(path, sc, className)

      implicit val format = DefaultFormats
      new SigOptCrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setNumFolds((metadata.params \ "numFolds").extract[Int])
        .setNumIterations((metadata.params \ "numIterations").extract[Int])
        .setExperimentId((metadata.params \ "experimentId").extract[String])
        .setClientToken((metadata.params \ "clientToken").extract[String])
    }
  }
}

class SigOptCrossValidatorModel private[spark] (
  override val uid: String,
  val bestModel: Model[_]
) extends Model[SigOptCrossValidatorModel] with SigOptCrossValidatorParams with MLWritable {
  override def write: MLWriter = new SigOptCrossValidatorModel.SigOptCrossValidatorModelWriter(this)
	override def copy(extra: ParamMap): SigOptCrossValidatorModel = {
    val copied = new SigOptCrossValidatorModel(uid, bestModel.copy(extra).asInstanceOf[Model[_]])
    copyValues(copied, extra).setParent(parent)
  }
	override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, true)
    bestModel.transform(dataset)
  }
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)
}

object SigOptCrossValidatorModel extends MLReadable[SigOptCrossValidatorModel] {
  override def read: MLReader[SigOptCrossValidatorModel] = new SigOptCrossValidatorModelReader
  override def load(path: String): SigOptCrossValidatorModel = super.load(path)

  private[SigOptCrossValidatorModel]
  class SigOptCrossValidatorModelWriter(instance: SigOptCrossValidatorModel) extends MLWriter {
    SigOptCrossValidatorParams.validateParams(instance)
    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class SigOptCrossValidatorModelReader extends MLReader[SigOptCrossValidatorModel] {
    private val className = classOf[SigOptCrossValidatorModel].getName

    override def load(path: String): SigOptCrossValidatorModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator) = SigOptCrossValidatorParams.loadImpl(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = SigOptCrossValidatorParams.loadParamsInstance[Model[_]](bestModelPath, sc)
      val model = new SigOptCrossValidatorModel(metadata.uid, bestModel)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.numFolds, (metadata.params \ "numFolds").extract[Int])
        .set(model.numIterations, (metadata.params \ "numIterations").extract[Int])
        .set(model.experimentId, (metadata.params \ "experimentId").extract[String])
        .set(model.clientToken,(metadata.params \ "clientToken").extract[String])
    }
  }
}
