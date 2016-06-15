 
import org.apache.spark.sql.Row
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}

object RedditLDA {
    def main(args: Array[String]){
		val json = sqlContext.read.parquet("s3a://reddit-comments-parquet/year=2007/")
		json.registerTempTable("science")
		val science = sqlContext.sql("SELECT * FROM json where subreddit = science")
		science.registerTempTable("noname")
		val filtered_science = sqlContext.sql("SELECT author, body FROM noname where author not like '%[deleted]%'")
		filtered_science.select("author").distinct.count()
		val ldardd = filtered_science.select("body")

		val rows = ldardd.map(_.getString(0))
		val corpus = rows.map(_.toLowerCase())

		val tokenizer = new RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(4).setInputCol("corpus").setOutputCol("tokens")
		val tokenized_df = tokenizer.transform(corpus.toDF("corpus"))

		val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
		val filtered_df = remover.transform(tokenized_df)

		val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)
		val countVectors = vectorizer.transform(filtered_df).select("id", "features")


		val lda_countVector = countVectors.map {case Row(countVector: Vector) => (countVector) }
		val numTopics = 20
		val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(numTopics).setMaxIterations(3).setDocConcentration(-1).setTopicConcentration(-1) 
    }
}