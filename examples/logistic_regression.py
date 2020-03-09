# Get API token from https://app.sigopt.com/tokens
SIGOPT_API_TOKEN = 'PUT_YOUR_API_TOKEN_HERE'

from sigoptspark.estimator import SigOptEstimator

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Prepare training documents, which are labeled.
data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
training, test = data.randomSplit([0.8, 0.2])
lr = LogisticRegression(maxIter=10)

crossval = SigOptEstimator(estimator=lr, evaluator=RegressionEvaluator(), numFolds=3)
crossval.setApiToken(SIGOPT_API_TOKEN)
crossval.createExperiment(
    {
        lr.elasticNetParam: {'type': 'double', 'bounds': {'min': 0.0, 'max': 1.0}},
        lr.regParam: {'type': 'double', 'bounds': {'min': 0.0, 'max': 1.0}},
    },
    name='Pyspark Test',
    observation_budget=21,
)

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(training)

print("Best Model performance on test data:")
cvModel.bestModel.transform(test)
