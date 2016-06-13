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

import scalaj.http._
import org.json4s._
import org.json4s.native.JsonMethods._ 

import java.util.{List => JList}
import scala.collection.JavaConverters._

import com.github.fommil.netlib.F2jBLAS
import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.param.SigParamBuilder
/**
 * Params for [[CrossValidator]] and [[CrossValidatorModel]].
 */
private[ml] trait CrossValidatorParams extends ValidatorParams {
  /**
   * Param for number of folds for cross validation.  Must be >= 2.
   * Default: 3
   *
   * @group param
   */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)
}

private[ml] trait SigOptCrossValidatorParams extends CrossValidatorParams{
  /**
   * Sets the base credentials required to access an experiment based on the group id
   * The remaining logic is calculated in the CrossValidtor class through ansync JSON hyyp requests
   */
  val token: String 
  val n_iter: Int
  val group_id: String 
  val experiment_id: String
  val paramter_bounds: Map[Param[T], (Double, Double)]
  var sigoptParamMaps: JValue
  var hyperparameters: Map[Param[T], [T]]
  var suggestion_id : Int 
  def auth_url(group_id: String): String = {
    s"https://api.sigopt.com/v1/clients/$group_id"
  }
  val post_url: String = "https://api.sigopt.com/v1/experiments"
  
  def base_opt(experiment_id : String): String = {
    s"https://api.sigopt.com/v1/experiments/$experiment_id/suggestions"
  }
  def base_obs(experiment_id: String): String = {
    s"https://api.sigopt.com/v1/experiments/$experiment_id/observations"
  }

}

@DeveloperApi
class SigOptCrossValidator extends CrossValidator with SigOptCrossValidatorParams{
  @Since("2.0.0")
  /** @group setParam */
  @Since("2.0.0")
  def setSigOptToken(value: String): this.type = set(token, value)
  /** @group setParam */
  @Since("2.0.0")
  def setSigOptGroup(value: String): this.type = set(group_id, value)
  @Since("2.0.0")
  def set_n_iter(value: Int): this.type = set(n_iter, value)
  
  case class ParameterBounds(map: Map[String, (Double, Double)]])
  @Since("2.0.0")
  def setSigOptParamMaps(value: Map[[T], ([T], [T])): JValue = {
    set(sigoptParamMaps, write(ParaneterBounds(value)))    // defines the JSOn for the sigopt
  }

  //1 SET UP THE EXPERIMENT
  //this only has to be done once
  @Since("2.0.0")
  def establishExperiment(token: String, data: String): this.type = {
    //this.experiment_id =
    set(experiment_id,compact(render(parse(Http(post_url).postData(newishdata).auth(this.token, "").headers(Seq("content-type" -> "application/json")).asString.body) \\ "id"))
     ))))
  }


  //supply and ask for a suggestion
  def askSuggestion() = {
    var suggestion_url: String = base_opt(this.experiment_id)
    var json_suggestion: JValue = parse(Http(suggestion_url).postData(sigoptParamMaps).auth(this.token, "").headers(Seq("content-type" -> "application/json")).asString.body))
    set(hyperparameters, (json_suggestion \\ "assignments").extract[[Map[String, Any]]])
    set(suggestion_id, (json_suggestion \\ "id").extract[Int])
  }
  
  def observeSuggestion(suggestion_id : Int = this.suggestion_id, metric: Double): Map[String, Any] = {
    case class Observations(suggestion_id: Int, metric: Double)
    var observation_url: String = base_obs(this.experiment_id)
    var new_parameters: JValue = parse(Http(observation_url).postData(write(Observations(suggestion_id, metric))).auth(this.token, "").headers(Seq("content-type" -> "application/json")).asString.body)))
    set(hyperparameters, new_parameters.extract[Map[String, Any]])
  }
  //2
  //POST a metric and the parameters as a JSON grid to the base_opt url
  

  // def setParamBounds(parameters :List[Param[T]], bounds: List[(Double, Double)]) : Map[Param[T], (Double, Double)] = {
  //     val m : Map[Param[T], (Double, Double)] = (parameters zip bounds).toMap 
  //     set(parameter_bounds, m)
  // }


  //3
  //Calculate the new metric and make an observation that you then post back to the json 

  //4 grab the iteration of a new suggestion from base_obs jand shove it back into the model and continue  
  

  ///Use Case is cv = new SighOptCrossValidator.setSigOptToken("ADFFEHIGHFDN").setSigOptGroup(1113)
  //override the fit definition to do incremental serialization over a grid. this is the hard part.

  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
      val schema = dataset.schema
      transformSchema(schema, logging = true)
      val sparkSession = dataset.sparkSession
      val est = $(estimator)
      val eval = $(evaluator)
      val epm = $(estimatorParamMaps)
      val numModels = epm.length
      val num_iter = $(n_iter)
      val metrics = new Array[Double](epm.length)
      val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
      splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
        val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
        val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
        // multi-model training
        logDebug(s"Train split $splitIndex with multiple sets of parameters.")
        val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
        trainingDataset.unpersist()
        var i = 0 
        while (i < numModels) {
          // TODO: duplicate evaluator to take extra params from input
          val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
          logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
          metrics(i) += metric
          i += 1
        }
        validationDataset.unpersist()
      }
      f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
      logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
      val (bestMetric, bestIndex) =
        if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
        else metrics.zipWithIndex.minBy(_._1)
      logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
      logInfo(s"Best cross-validation metric: $bestMetric.")
      val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
      copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
    }

}



/**
 * :: Experimental ::
 * K-fold cross validation.
 */
@Since("1.2.0")
@Experimental
class CrossValidator @Since("1.2.0") (@Since("1.4.0") override val uid: String)
  extends Estimator[CrossValidatorModel]
  with CrossValidatorParams with MLWritable with Logging {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("cv"))

  private val f2jBLAS = new F2jBLAS

  /** @group setParam */
  @Since("1.2.0")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFolds(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSigOptToken(value: Int): this.type = set(numFolds, value)

  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps) //init
    val numModels = epm.length
    val metrics = new Array[Double](epm.length)
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      // multi-model training
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")
      val models = est.fit(trainingDataset, epm).asInstanceOf[Seq[Model[_]]]
      trainingDataset.unpersist()
      var i = 0
      while (i < numModels) {
        // TODO: duplicate evaluator to take extra params from input
        val metric = eval.evaluate(models(i).transform(validationDataset, epm(i)))
        logDebug(s"Got metric $metric for model trained with ${epm(i)}.")
        metrics(i) += metric
        i += 1
      }
      //http
      validationDataset.unpersist()
    }
    f2jBLAS.dscal(numModels, 1.0 / $(numFolds), metrics, 1)
    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[CrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
  // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
  // However, this case should be unusual.
  @Since("1.6.0")
  override def write: MLWriter = new CrossValidator.CrossValidatorWriter(this)
}

@Since("1.6.0")
object CrossValidator extends MLReadable[CrossValidator] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidator] = new CrossValidatorReader

  @Since("1.6.0")
  override def load(path: String): CrossValidator = super.load(path)

  private[CrossValidator] class CrossValidatorWriter(instance: CrossValidator) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
  }

  private class CrossValidatorReader extends MLReader[CrossValidator] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidator].getName

    override def load(path: String): CrossValidator = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      new CrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setNumFolds(numFolds)
        .setSeed(seed)
    }
  }
}

/**
 * :: Experimental ::
 * Model from k-fold cross validation.
 *
 * @param bestModel The best model selected from k-fold cross validation.
 * @param avgMetrics Average cross-validation metrics for each paramMap in
 *                   [[CrossValidator.estimatorParamMaps]], in the corresponding order.
 */
@Since("1.2.0")
@Experimental
class CrossValidatorModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.2.0") val bestModel: Model[_],
    @Since("1.5.0") val avgMetrics: Array[Double])
  extends Model[CrossValidatorModel] with CrossValidatorParams with MLWritable {

  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) = {
    this(uid, bestModel, avgMetrics.asScala.toArray)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidatorModel = {
    val copied = new CrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new CrossValidatorModel.CrossValidatorModelWriter(this)
}

@Since("1.6.0")
object CrossValidatorModel extends MLReadable[CrossValidatorModel] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidatorModel] = new CrossValidatorModelReader

  @Since("1.6.0")
  override def load(path: String): CrossValidatorModel = super.load(path)

  private[CrossValidatorModel]
  class CrossValidatorModelWriter(instance: CrossValidatorModel) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "avgMetrics" -> instance.avgMetrics.toSeq
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class CrossValidatorModelReader extends MLReader[CrossValidatorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorModel].getName

    override def load(path: String): CrossValidatorModel = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val model = new CrossValidatorModel(metadata.uid, bestModel, avgMetrics)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
        .set(model.numFolds, numFolds)
        .set(model.seed, seed)
    }
  }
}