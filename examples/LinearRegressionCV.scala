import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.ml.tuning.SigOptCrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.mllib.util.MLUtils

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
    cv.setEvaluator(new RegressionEvaluator())
    cv.createExperiment(
      "SigOpt CV Example",
      clientToken,
      10,
      List(("elasticNetParam", 0.0, 1.0, "double"), ("regParam", 0.0, 1.0, "double"))
    )
    val training = MLUtils.loadLibSVMFile(spark, "data/mllib/sample_linear_regression_data.txt").toDF()
    cv.fit(training)
    spark.stop()
  }
}
