# SigSpark

Compiles against the Spark 2.0 build.

SigSpark integrates SigOpt's functionality into Spark for performing Bayesian
optimization over hyperparameters. It is designed to automatically run
experiments in a manner that iteratively adjusts parameter values so as to
minimize some user-defined objective over a pre-defined state space in as few
runs as possible.

####Example Usage

	>>>import org.apache.spark.ml.tuning.CrossValidator
	>>>import org.apache.spark.ml.regression.LinearRegression
	>>>import org.apache.spark.ml.evaluation.RegressionEvaluator

	>>>val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	>>>val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")

	>>>val lr = new LinearRegression()
	>>>val cv = new CrossValidator()

	>>>cv.setNumFolds(10)
	>>>cv.setEstimator(lr)
	>>>cv.setEvaluator(new RegressionEvaluator)

	// Establish the experiment: (name: String, api_token: String, iteration: int, bounds)
	// Format of bounds is: Array((String,Double,Double,String))
	// (ParameterName :String, Max: Double, Min: Double, type: String)
	>>>val bounds = Array(("elasticNetParam", 1.0, 0.0, "double"), ("regParam",1.0,0.0, "double"))
	>>>cv.createExperiment("Timing", YOUR_CLIENT_TOKEN, 10, bounds)
	>>>cv.askSuggestion(lr)
	>>>cv.fit(data)
