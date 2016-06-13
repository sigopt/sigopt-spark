/* THE RESPONSE OF GENERFATING THE EXPERIMENT RENDERS THE FOLLOWING JSON SCHEMA 

	{"name": "A new CTR experiment", 
    "parameters": 
	   [
	     {"default_value": null, 
	     "name": "distance", 
	     "object": "parameter", 
	     "bounds": 
	     	{"max": 50.0, 
	     	"object": "bounds", 
	     	"min": 0.0}, 
	     "tunable": true, 
	     "precision": null, 
	     "type": "double", 
	     "categorical_values": null}
	   ],
	 "created": 1465507401, 
	 "metric": null, 
	 "object": "experiment", 
	 "state": "active", 
	 "client": "1701", 
	 "progress": 
	 	{"first_observation": null, 
	 	"best_observation": null, 
	 	"observation_count": null, 
	 	"object": "progress", 
	 	"last_observation": null}, 
	 "type": "offline", 
	 "id": "4361", 
	 "metadata": null
	 }

	 For our purposes we need to extact the id of the experiment generated: 4361. 
*/


import scalaj.http._ 
import org.json4s._
import org.json4s.native.JsonMethods._
import native.Serialization.{read, write => swrite}
implicit val formats = native.Serialization.formats(NoTypeHints)

case class SigExperiment(name: String, team_id: String, token: String, parameters: List[SigParameters])

case class SigParameters(bounds: SigBounds, name: String, `type`: String)

case class SigBounds(max: Double, min:Double)

//val url : String = "https://api.sigopt.com/v1/clients/$team_id"  //1701
val experiment_id: String
val url : String = "https://api.sigopt.com/v1/clients/1701"
val post_url : String = "https://api.sigopt.com/v1/experiments"
val token: String = $SIG_OPT_API_TOKEN  //Class variable shared by any instance instantiated 

//IMplement some logic here for dealing with multiple parameters as a list of Parameter_name_max_min_types which will be just use the case classes
def generateNewExperiment(name: String, parameter_name_max_min_type:(String, Double, Double, String)): SigExperiment =  {
	val json_experiment:String = swrite(SigExperiment(s"$name", List(SigParameters(SigBounds(parameter_name_max_min_type._2,parameter_name_max_min_type._3), s"${parameter_name_max_min_type._1}", s"${parameter_name_max_min_type._4}"))))
    val experiment_response = Http(post_url).auth(token, "").postData(json_experiment).headers(Seq("content-type" -> "application/json")).asString.body
    val experiment_id = swrite(parse(experiment_reponse) \\ "id")
    //What do I want  from this message? 
}



//JSON FORMAT OF THE SUGGESTION  RESPONSE 
/*
     {"count":2,
     "paging":
     	{"after":null,"before":null},
     "object":"pagination",
     "data":
     	[
     	{"created":1465510565,
     	"object":"suggestion",
     	"assignments":
     		{"distance":13.377153205017562},
     	"state":"open",
     	"experiment":"4362",
     	"id":"1333013",
     	"metadata":null
     	},
     	{"created":1465510279,
     	"object":"suggestion",
     	"assignments":
     		{"distance":42.10641596542343},
     	"state":"open",
     	"experiment":"4362",
     	"id":"1333012",
     	"metadata":null
     	}
     	]
     }


*/

private[ml] trait SigOptParam extends Params {

  /**
   * Param for regularization parameter (>= 0).
   * @group param
   */
  var SigIntParam: IntParam = new IntParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0))
  var SigDoubleParam: DoubleParam = new DoubleParam(this, "regParam", "regularization parameter (>= 0)", ParamValidators.gtEq(0)) 

  /** @group getParam */
  final def getRegParam: Double = $(regParam)
}


case class EstimatorParameterTypes()

//IDEA// 
//It might be infeasible to change the values inside of the Pipeline. In this case we can do it on a model, by model basis, and re assimilate the pipeline 


//GOTTA IMPLEMENT TYPE HINTS VERY VERY SADLY




//USE SQL CONTEXT TO INFER THE SCHEMA OF A JSON FILE THAT YOU PLACE INTO  

//ALternative to clearing the map on each iteration is updating the json rdd using a set schema  and then grabbing from the table 

//lets pretend I have a pipeline in which i first want to do principle components to the 5 most important dimensions followed by a regression
//hyperparameters than can be chosen for are 1. {components: "int"} and 2. {regularization:"double"}
//this is going to be pretty sophisticated to generate and extract 

//Must define this to be of the type enlisted in the estimator for map return values
def generateSuggestionOne[A,B](experiment_id: String) = {
	var paramGrid = mutable.Map.empty[Param[_], Any]

	val suggest_url:String = "https://api.sigopt.com/v1/experiments/"+experiment_id.replace("\"", "")+"/suggestions"
	val suggestion_response = Http(suggest_url).auth(token, "").asString.body
	val suggest_json = parse(suggestion_response) 
	//var suggest_param = swrite((suggest_json \\ "data")(0) \\ "assignments")  // returns a string that we will have to convert
	var suggest_paramMap = ((suggest_json \\ "data")(0) \\ "assignments").extract[Map[A, Array[B]]]
	var suggestion_id = swrite(((suggest_json \\ "data")(0) \\ "id")).replace("\"", "")
	//assignments will be a mmapping from each parameter to it's associated value. 
	//what we have to do is capture that and convert it to a parametermap in the estimator

	//creating the grid with our suggestions
    for (suggestion_parameter, value <- suggest_paramMap)
    	paramGrid.put(suggestion_parameter, value)

//build phase
    var paramMaps = Array(new ParamMap)
    paramGrid.foreach(v =>  paramMaps.map(_.put(v._1.asInstanceOf[Param[Any]], v._2)))
    paramMaps

// RUN CV again and gather metric.


/*
 On the start, I'll grab the parameter types for each of the models in the pipeline. I'll save these so
 that further extraction is not required, but simple updating the values in the looop of setting the parameter grid by using 
 values extracted from the JSON entries.  In this way, I overwrite the parameterMaps on each iteration and re=run
 cross validation. 
*?

//EMPTY THE MAP 
	paramGrid.clear
	//LOOOP

   //estimator.setParam
   //issue here is what stage harbors which parameter and how to override these. 
   for(stage <- pipeline.stages){
   	for (param <- suggest_paramMap)
   		stage.set(stage.getParam(s"$param._1"), param._2)


	//new signals in terms of old ones using reactive programming



var newMetric: Double = estimator.evaluateMetric(paramMaps) // this is some metric from the model


/*JSON FORMAT OF THE OBSERVATION RESPONSE 
{"created": 1465513143, "object": "observation", "value_stddev": null, "value": 29, "failed": false, "assignments": { "numFeatures": 2000 ,"regParam": 42.10641596542343}, "experiment": "4362", "suggestion": "1333012", "id": "199272", "metadata": null}




*/
def generateObservation(suggestion_id : String, metric: Double)
// case class SigCategory(List[SigCatVals])

// case class SigCatVals(Map[category:String, value: String])

val suggest_url:String = "https://api.sigopt.com/v1/experiments/4362/suggestions"
