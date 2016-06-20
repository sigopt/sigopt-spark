import java.io.File
val enwiki_s2 = sqlContext.read.parquet("s3a://wiki-paruqet//en-s2-wikiparq");
enwiki_s2.registerTempTable("enwiki")
sqlContext.sql("""SELECT uri, lang, value1 AS text FROM enwiki WHERE predicate = 'document:text'""").show() //extracting the text from english wkipedia
sqlContext.sql("""SELECT COUNT(*) FROM enwiki WHERE predicate = 'token:cpostag' AND value1 = 'NOUN'""").show() // # of nouns in english