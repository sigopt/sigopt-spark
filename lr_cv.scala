import org.apache.spark.sql
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}

object LinearRegressionCV { 

  def main(args: Array[String]): Unit ={
    val training = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
	val cv = new CrossValidator()

	val lr = new LinearRegression()

	cv.setEstimator(lr)
	cv.setNumFolds(5)
	cv.setEvaluator(new RegressionEvaluator)
	cv.setSigCV("Elastic Net 6_2_11",args(0), 10, Array(("elasticNetParam", 1.0, 0.0, "double"), ("regParam",1.0,0.0, "double")))
	cv.askSuggestion(cv.getEstimator)
	cv.fit(training)
    
    spark.stop()
	}
}
