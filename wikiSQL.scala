import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.clustering.LDA
import org.json4s.jackson.JsonMethods._ 
import org.apache.spark.sql.Row


val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val json = sqlContext.read.parquet("s3a://wiki-parquet/en-s2-wikiparq/")
json.registerTempTable("wikiData")

val text_table = sqlContext.sql("""SELECT value1 AS text FROM wikiData WHERE predicate = 'document:text'""")
val rows = text_table.map(_.getString(0))
val corpus = rows.map(_.toLowerCase())
val docDF = corpus.rdd.zipWithIndex.toDF("value", "docId")
val tokenizer = new RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(4).setInputCol("value").setOutputCol("tokens")
val tokenized_df = tokenizer.transform(docDF)

val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
val filtered_df = remover.transform(tokenized_df)

val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
val countVectors = vectorizer.transform(filtered_df).select("docId", "features")

val numTopics = 100
val lda = new LDA().setK(numTopics).setMaxIterations(100)

val sigCV = new CrossValidator().setNumFolds(5).setEstimator(lda)
sigCV.setSigCV()
sigCV.askSuggestion(sigCV.getEstimator)4

val ldaModel = sigCV.fit(countVectors)
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
val vocabArray = cvModel.vocabulary
val topics = topicIndices.map { case (terms, termWeights) =>
  terms.map(vocabArray(_)).zip(termWeights)
}
println(s"$numTopics topics:")
topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}





