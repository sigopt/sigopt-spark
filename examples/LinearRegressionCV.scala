import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.ml.tuning.SigOptCrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.util.MLUtils

object LinearRegressionCV {
  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("SigOpt Example").setMaster("local")
    val spark = new SparkContext(conf)
    val training = MLUtils.loadLibSVMFile(spark, "data/mllib/sample_linear_regression_data.txt")
    val cv = new SigOptCrossValidator("123")
    val lr = new LinearRegression()
    cv.setEstimator(lr)
    cv.setNumFolds(5)
    cv.setNumIterations(10)
    cv.setEvaluator(new RegressionEvaluator())
    cv.createExperiment("SigOpt CV Example",args(0), 10, Array(("elasticNetParam", 1.0, 0.0, "double"), ("regParam",1.0,0.0, "double")))
    cv.askSuggestion(cv.getEstimator)
    cv.fit(training)
    spark.stop()
  }
}
