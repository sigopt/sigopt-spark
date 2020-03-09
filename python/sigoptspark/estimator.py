# Adapted from pyspark.ml.tuning, https://github.com/apache/spark/tree/v2.4.5

import sigopt

from pyspark import keyword_only
from pyspark.ml import Estimator, Model
from pyspark.ml.param import Params, Param, TypeConverters
from pyspark.ml.param.shared import HasSeed
from pyspark.ml.util import MLReadable, MLWritable
from pyspark.ml.wrapper import JavaParams
from pyspark.ml.tuning import CrossValidator
from pyspark.sql.functions import rand


class _SigOptEstimatorParams(HasSeed):
    estimator = Param(Params._dummy(), "estimator", "Matches pyspark.ml.tuning.CrossValidator")
    evaluator = Param(Params._dummy(), "evaluator", "Matches pyspark.ml.tuning.CrossValidator")
    numFolds = Param(Params._dummy(), "numFolds", "Matches pyspark.ml.tuning.CrossValidator",
                     typeConverter=TypeConverters.toInt)
    apiToken = Param(Params._dummy(), "apiToken", "SigOpt API Token")
    experimentId = Param(Params._dummy(), "experimentId", "SigOpt Experiment ID")
    parameterMap = Param(Params._dummy(), "parameterMap", "Mapping from SigOpt parameter name to estimator Param")


class SigOptEstimator(Estimator, _SigOptEstimatorParams):
    @keyword_only
    def __init__(self, estimator=None, evaluator=None, numFolds=3, apiToken=None, experimentId=None, parameterMap=None):
        super(SigOptEstimator, self).__init__()
        self._setDefault(numFolds=3, parameterMap={})
        kwargs = self._input_kwargs
        self._set(**kwargs)

    @keyword_only
    def setParams(self, estimator=None, evaluator=None, numFolds=3, apiToken=None, experimentId=None, parameterMap=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def setApiToken(self, value):
        return self._set(apiToken=value)

    def setExperimentId(self, value):
        return self._set(experimentId=value)

    def setParameterMap(self, value):
        return self._set(parameterMap=value)

    def createExperiment(self, paramDefinitionMap, **experiment_kwargs):
        eva = self.getOrDefault(self.evaluator)
        if 'parameters' not in experiment_kwargs:
            experiment_kwargs['parameters'] = []
            for (param, definition) in paramDefinitionMap.items():
                definition['name'] = definition.get('name', param.name)
                experiment_kwargs['parameters'].append(definition)
        if 'metrics' not in experiment_kwargs:
            experiment_kwargs['metrics'] = [{
                'name': 'metric',
                'objective': ('maximize' if eva.isLargerBetter() else 'minimize'),
            }]
        apiToken = self.getOrDefault(self.apiToken)
        conn = sigopt.Connection(apiToken)
        experiment = conn.experiments().create(**experiment_kwargs)
        parameterMap = {}
        for (param, definition) in paramDefinitionMap.items():
            parameterMap[definition['name']] = param
        self.setExperimentId(experiment.id)
        self.setParameterMap(parameterMap)
        return experiment

    def _fit(self, dataset):
        estimator = self.getOrDefault(self.estimator)
        evaluator = self.getOrDefault(self.evaluator)

        seed = self.getOrDefault(self.seed)
        nFolds = self.getOrDefault(self.numFolds)
        h = 1.0 / nFolds
        randCol = self.uid + "_rand"
        df = dataset.select("*", rand(seed).alias(randCol))

        eid = self.getOrDefault(self.experimentId)
        apiToken = self.getOrDefault(self.apiToken)
        conn = sigopt.Connection(apiToken)
        experiment = conn.experiments(eid).fetch()
        if not experiment.observation_budget:
            raise ValueError('Error: Experiment must have `observation_budget`')

        while experiment.progress.observation_count < experiment.observation_budget:
            suggestion = conn.experiments(experiment.id).suggestions().create()
            metric_values = []
            for i in range(nFolds):
                validateLB = i * h
                validateUB = (i + 1) * h
                condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
                train = df.filter(~condition).cache()
                validation = df.filter(condition).cache()
                params = self._get_params_from_assignments(suggestion.assignments)
                model = estimator.fit(train, params)
                metric_values.append(evaluator.evaluate(model.transform(validation)))
                train.unpersist()
                validation.unpersist()

            mean = sum(metric_values) / nFolds
            stddev = sum([((value - mean) ** 2) for value in metric_values]) / nFolds
            conn.experiments(experiment.id).observations().create(
                suggestion=suggestion.id,
                value=mean,
                value_stddev=stddev,
            )
            experiment = conn.experiments(eid).fetch()

        bestObservation = conn.experiments(eid).best_assignments().fetch().data[0]
        params = self._get_params_from_assignments(bestObservation.assignments)
        bestModel = estimator.fit(dataset, params)
        return self._copyValues(SigOptOptimizedModel(bestModel))

    def _get_params_from_assignments(self, assignments):
      parameterMap = self.getOrDefault(self.parameterMap)
      ret = {}
      for (assignmentName, value) in assignments.items():
          param = parameterMap.get(assignmentName)
          if param is None:
              raise ValueError('No suggested value found for parameter: ' + str(assignmentName))
          ret[param] = value
      return ret

    def copy(self, extra=None):
        if extra is None:
            extra = dict()
        newEstimator = Params.copy(self, extra)
        if self.isSet(self.estimator):
            newEstimator.setEstimator(self.getEstimator().copy(extra))
        if self.isSet(self.evaluator):
            newEstimator.setEvaluator(self.getEvaluator().copy(extra))
        return newEstimator


class SigOptOptimizedModel(Model, _SigOptEstimatorParams):
    def __init__(self, bestModel):
        super(SigOptOptimizedModel, self).__init__()
        self.bestModel = bestModel

    def _transform(self, dataset):
        return self.bestModel.transform(dataset)

    def copy(self, extra=None):
        if extra is None:
            extra = dict()
        bestModel = self.bestModel.copy(extra)
        return SigOptOptimizedModel(bestModel)
