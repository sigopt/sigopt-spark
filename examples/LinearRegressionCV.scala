import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.util.MLUtils

import com.sigopt.spark.SigOptCrossValidator

object LinearRegressionCV {
  def main(args: Array[String]): Unit ={
    var clientToken = args.headOption.getOrElse("FAKE_CLIENT_TOKEN")
    val conf = new SparkConf().setAppName("SigOpt Example").setMaster("local")
    val spark = new SparkContext(conf)
    val sqlContext = new SQLContext(spark)
    import sqlContext.implicits._

    val cv = new SigOptCrossValidator("123")
    val lr = new LinearRegression()
    cv.setEstimator(lr)
    cv.setNumFolds(5)
    cv.setNumIterations(10)
    cv.setClientToken(clientToken)
    cv.setEvaluator(new RegressionEvaluator())

    // If your experiment has already been created, you can just set the ID instead
    // cv.setExperimentId("4866")
    cv.createExperiment(
      "SigOpt CV Example",
      List(("elasticNetParam", 0.0, 1.0, "double"), ("regParam", 0.0, 1.0, "double"))
    )
    val training = MLUtils.loadLibSVMFile(spark, "examples/data/sample_linear_regression_data.txt").toDF()
    cv.fit(training)
    spark.stop()
  }
}
