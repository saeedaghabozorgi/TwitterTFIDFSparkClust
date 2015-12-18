import os
import sys
# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip')
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from pyspark.sql import SQLContext
from pyspark import SparkContext
sc  = SparkContext('local[4]', 'Social Panic Analysis')
sqlContext = SQLContext(sc)
sentenceData = sqlContext.createDataFrame([
  (0, "Hi I heard about Spark"),
  (0, "I wish Java could use case classes"),
  (1, "Logistic regression models are neat")
], ["label", "sentence"])
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurizedData = hashingTF.transform(wordsData)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)
for features_label in rescaledData.select("features", "label").take(3):
  print(features_label)