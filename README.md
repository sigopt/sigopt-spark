# sigopt-spark

Compiles against the Spark 1.6.1 build on scala 2.10 and 2.11.

sigopt-spark integrates SigOpt's functionality into Spark for performing Bayesian
optimization over hyperparameters. You can use this as a drop-in replacement for
CrossValidator. Just provide your SigOpt client token and the number of iterations
you'd like to use.

####Example Usage

```scala
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val data = sqlContext.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")

val lr = new LinearRegression()
val cv = new SigOptCrossValidator()

cv.setNumFolds(10)
cv.setEstimator(lr)
cv.setEvaluator(new RegressionEvaluator)

// Establish the experiment: (name: String, api_token: String, iteration: int, bounds)
// Format of bounds is: Array((name: String, min: Double, max: Double, type: String))
val bounds = Array(("elasticNetParam", 0.0, 1.0, "double"), ("regParam", 0.0, 1.0, "double"))
cv.setGlobalClientToken(YOUR_CLIENT_TOKEN)
cv.createExperiment("Timing", 10, bounds)
cv.fit(data)

// If your experiment has already been created, you can just set the ID instead
cv.setGlobalClientToken(YOUR_CLIENT_TOKEN)
cv.setExperimentId("1")
cv.fit(data)
```
