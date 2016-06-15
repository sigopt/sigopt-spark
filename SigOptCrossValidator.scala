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


///usr/local/spark/bin$ ./spark-shell --jars /home/ubuntu/.sbt/0.13/staging/b33786ea2577dfe8745d/scalaj-http/target/scala-2.10/scalaj-http_2.10-2.3.0.jar

 ./spark-shell --jars /home/ubuntu/.sbt/0.13/staging/b33786ea2577dfe8745d/scalaj-http/target/scala-2.10/scalaj-http_2.10-2.3.0.jar /home/ubuntu/.sbt/0.13/staging/ae76074e94ae8bdd915e/json4s/native/target/scala-2.11/json4s-native_2.11-3.4.0-SNAPSHOT.jar

 */

package org.apache.spark.ml.tuning

import scalaj.http._
import org.json4s._
import org.apache.spark.ml.tuning._
import org.json4s.native.JsonMethods._ 
import native.Serialization.{read, write => swrite}
implicit val formats = native.Serialization.formats(NoTypeHints)

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
// import org.apache.spark.ml.param.SigParamBuilder


private[ml] trait SigOptCrossValidatorParams extends CrossValidatorParams{
  // Defines the basic parameters that will be required for running SigOpt experiments

  // def null_string(s: String): Boolean = {
  //   (Option(s) match {
  //    case Full(s) if (!s.isEmpty) => String filled
  //    case _ => String was null
  //    }
  // } may use this as a function in Param instantiation

  val token: Param = new Param(this, "sigOptToken", "An Api Authentication token", ParamValidators.alwaysTrue())
  val n_iter: IntParam = new IntParam(this, "nIter", "Iterations of cross validation", ParamValidators.getEq(2))
  def get_n_iter:Int = $(n_iter)
  def get_token: String = $(token)

}


@DeveloperApi
class SigOptCrossValidator extends CrossValidator with SigOptCrossValidatorParams with SigParameterBounds{
  
  case class SigParameters( name: String, max:Double, min:Double, `type`: String)  //I use case classes for json serialization

  case class SigExperiment(name: String, team_id: String, token: String, parameters: Array[SigParameters])

  val experiment_id: Int= 0
  var suggestion_id: Int = 0

  @Since("2.0.0")
  /** @group setParam */
  def this() = this(Identifiable.randomUID("cv"))

  @Since("2.0.0")
  def setSigOptToken(value: String): this.type = set(token, value)

  @Since("2.0.0")
  def set_n_iter(value: Int): this.type = set(n_iter, value)

  @Since("2.0.0")
  def set_exp(value: Int): this.type = set(experiment_id, value)
  
  @Since("2.0.0")
  def set_sug(value: Int): this.type = set(suggestion_id, value)

  @Since("2.0.0")
  def base_opt(experiment_id : String): String = {
    s"https://api.sigopt.com/v1/experiments/$experiment_id/suggestions"
  }
  
  @Since("2.0.0")
  def base_obs(experiment_id: String): String = {
    s"https://api.sigopt.com/v1/experiments/$experiment_id/observations"
  }

  /*
  val sigCV = new SigOptCrossValidator().setupSigCV("My new experiment to classify cool rocks.", "ADJGJGENFCJFCFN$", Array("distance", 20, 0, "double"))
  sigCV.fit(dataframe)



  */
  

  def setupSigCV(name: String, token_val: String, id: Int, bound_val: Array[(String, Double, Double, String)]) = {
    setSigOptToken(token_val)
    val post_url : String = "https://api.sigopt.com/v1/experiments"  //Endpoint for establishing an experiment
    var sigarray:Array[SigParameters] = Array()
    for(i <- bound_val){
      sigarray :+ SigParameters(i._1.toLowerCase(), i._2, i._3, i._4.toLowerCase())
    }
    val json_experiment:String = swrite(SigExperiment(name, sigarray))
    val experiment_response = Http(post_url).auth(token, "").postData(json_experiment).headers(Seq("content-type" -> "application/json")).asString.body
    this.experiment_id = (parse(experiment_reponse) \\ "id").extract[Int]   //with bounds set and an experiment set save the id for further us
  }
  
  //Once the Experiment is setup we will mostly utilize the following two functions.
  //Ask Suggestion queries the SigOpt API for the next parameters to search and then writes these to the estimator. 

  def askSuggestion() = {
    var paramGrid = mutable.Map.empty[Param[_], Any]
    var suggestion_url: String = base_opt(this.experiment_id)
    val suggestion_response = parse(Http(suggestion_url).auth(token, "").asString.body)
    var suggest_paramMap = ((suggestion_response \\ "data")(0) \\ "assignments").extract[Map[Any, Any]]  //pulling out the most recent suggestions 
    this.suggestion_id = ((suggestion_response \\ "data")(0) \\ "id").extract[Int]                       //identifying the current suggestion

    for (suggestion_parameter, value <- suggest_paramMap)
      paramGrid.put(suggestion_parameter, value)

    var paramMaps = Array(new ParamMap)
    paramGrid.foreach(v =>  paramMaps.map(_.put(v._1.asInstanceOf[Param[Any]], v._2)))  //strict typing is killer.
    setEstimatorParamMaps(paramMaps)
  }
  

  //Then we have an observation part where we supply the new loss function given the parameters we were suggested to use 

  def observeSuggestion(metric: Double) = {
    case class Observations(suggestion: Int, value: Double)
    var observation_url: String = base_obs(this.experiment_id)
    Http(observation_url).postData(write(Observations(this.suggestion_id, metric))).auth(this.token, "").headers(Seq("content-type" -> "application/json")).asString.body)))
    askSuggestion()
  }

  //*The* place where everthing happpens
  override def fit(dataset: Dataset[_]): SigOptCrossValidatorModel = {
      val schema = dataset.schema
      transformSchema(schema, logging = true)
      val sparkSession = dataset.sparkSession
      val est = $(estimator)
      val eval = $(evaluator)
      //need to grab the suggestion first
      askSuggestion()
      val epm = $(estimatorParamMaps)
      val num_iter = $(n_iter)
      val metrics = new Array[Double](num_iter)
      val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
      splits.zipWithIndex.foreach { case ((training, validation), splitIndex) =>
        val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
        val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
        // multi-model training
        logDebug(s"Train split $splitIndex with multiple sets of parameters.")
        val model = est.fit(trainingDataset, epm).asInstanceOf[Model[_]]
        trainingDataset.unpersist()
        var i = 0 
        while (i < num_iter) {
          // TODO: duplicate evaluator to take extra params from input
          val metric = eval.evaluate(model.transform(validationDataset, epm)
          observeSuggestion(metric)
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
      logInfo(s"Best set of parameters:\n${epm}")
      logInfo(s"Best cross-validation metric: $bestMetric.")
      val bestModel = est.fit(dataset, epm.asInstanceOf[Model[_]]
      copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
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
@Since("2.0.0")
@Experimental
class SigOptCrossValidatorModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.2.0") val bestModel: Model[_],
    @Since("1.5.0") val avgMetrics: Array[Double])
  extends Model[SigOptCrossValidatorModel] with SigOptCrossValidatorParams with MLWritable {

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
  override def copy(extra: ParamMap): SigOptCrossValidatorModel = {
    val copied = new SigOptCrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new SigOptCrossValidatorModel.SigOptCrossValidatorModelWriter(this)
}



object SigOptCrossValidatorModel extends MLReadable[SigOptCrossValidator] {
  
  import CrossValidatorModel._

  @Since("1.6.0")
  override def read: MLReader[SigOptCrossValidator] = new SigOptCrossValidatorReader

  @Since("1.6.0")
  override def load(path: String): SigOptCrossValidator = super.load(path)

  private[SigOptCrossValidator] class SigOptCrossValidatorWriter(instance: SigOptCrossValidator) extends MLWriter {

    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
        import org.json4s.JsonDSL._
        val extraMetadata = "avgMetrics" -> instance.avgMetrics.toSeq
        ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
        val bestModelPath = new Path(path, "bestModel").toString
        instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)  
    }
  }

  private class SigOptCrossValidatorReader extends MLReader[SigOptCrossValidator] {

    /** Checked against metadata when loading model */
    private val className = classOf[SigOptCrossValidator].getName

    override def load(path: String): SigOptCrossValidator = {
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) = ValidatorParams.loadImpl(path, sc, className)
      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val seed = (metadata.params \ "seed").extract[Long]
      val n_iter = (metadata.params \ "n_iter").extract[Int]
      val token = (metadata.params \ "token").extract[String]
      val bounds = (metadata.params \ "bounds").extract[Map[Any, (Any, Any)]]
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val model =new SigOptCrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setNumFolds(numFolds)
        .setSeed(seed)
        .setSigOptToken(tokenn)
        .set_n_iter(n_iter)
        .setSigOptParamMaps(bounds)
    }
  }
}

