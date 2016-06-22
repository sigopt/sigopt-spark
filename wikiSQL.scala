import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val json = sqlContext.read.parquet("s3a://wiki-paruqet/en-s2-wikiparq/")
json.registerTempTable("wikiData")

val text_table = sqlContext.sql("""SELECT value1 AS text FROM wikiData WHERE predicate = 'document:text'""")
val rows = text_table.map(_.getString(0))
val corpus = rows.map(_.toLowerCase())

val tokenizer = new RegexTokenizer().setPattern("[\\W_]+").setMinTokenLength(4).setInputCol("corpus").setOutputCol("tokens")
val tokenized_df = tokenizer.transform(corpus.toDF("corpus"))

val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("filtered")
val filtered_df = remover.transform(tokenized_df)

val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(50000).setMinDF(5).fit(filtered_df)
val countVectors = vectorizer.transform(filtered_df).select("features")

val lda_countVector = countVectors.map { case Row(countVector: Vector) => (countVector) }

val numTopics = 20

val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(0.8)).setK(numTopics).setMaxIterations(3).setDocConcentration(-1).setTopicConcentration(-1) 