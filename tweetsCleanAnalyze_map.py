#http://marcobonzanini.com/2015/03/09/mining-twitter-data-with-python-part-2/
import pandas as pd
from nltk.tokenize import word_tokenize
import re
from nltk.corpus import stopwords
import string
from collections import Counter
from bs4 import BeautifulSoup
from sklearn.feature_extraction.text import CountVectorizer


import json
import pandas as pd
import sys
import os
# Path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/"
# Append the python dir to PYTHONPATH so that pyspark could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/')
# Append the python/build to PYTHONPATH so that py4j could be found
sys.path.append('/usr/local/Cellar/apache-spark/spark-1.5.2-bin-hadoop2.6/python/lib/py4j-0.8.2.1-src.zip')
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row


tweets_data_path = '../dataset/sexfood50.json'

def get_coord(post):
    coord = tuple()
    try:
        if post['coordinates'] == None:
            coord = post['place']['bounding_box']['coordinates']
            coord = reduce(lambda agg, nxt: [agg[0] + nxt[0], agg[1] + nxt[1]], coord[0])
            coord = tuple(map(lambda t: t / 4.0, coord))
        else:
            coord = tuple(post['coordinates']['coordinates'])
    except TypeError:
        #print ('error get_coord')
        coord=(0,0)
    return coord

def tokenize(text):
    tokens = []
    text = text.encode('ascii', 'ignore') #to decode
    text=re.sub('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text) # to replace url with ''
    text=text.lower()
    for word in word_tokenize(text):
        if word \
            not in stopwords.words('english') \
            and word not in string.punctuation \
            and len(word)>1 \
            and word != '``':
                tokens.append(word)

    return tokens



def plotit(tweets,clusterNum):
    from mpl_toolkits.basemap import Basemap
    import matplotlib.pyplot as plt
    from pylab import rcParams
    import numpy as np
    #usa
    llon = -130
    ulon = -60
    llat = 20
    ulat = 50
    # llon = -130
    # ulon = 140
    # llat = -10
    # ulat = 60
    rcParams['figure.figsize'] = (14,10)
    my_map = Basemap(projection='merc',
                resolution = 'l', area_thresh = 1000.0,
                llcrnrlon=llon, llcrnrlat=llat, #min longitude (llcrnrlon) and latitude (llcrnrlat)
                urcrnrlon=ulon, urcrnrlat=ulat) #max longitude (urcrnrlon) and latitude (urcrnrlat)

    my_map.drawcoastlines()
    my_map.drawcountries()
    my_map.drawmapboundary()
    my_map.fillcontinents(color = 'white', alpha = 0.3)
    my_map.shadedrelief()

    xs,ys = my_map(np.asarray(tweets['long']), np.asarray(tweets['lat']))
    tweets['xm'] = xs.tolist()
    tweets['ym'] =ys.tolist()

    # To create a color map
    colors = plt.get_cmap('jet')(np.linspace(0.0, 1.0, clusterNum))
    cenx=[]
    ceny=[]
    for i in range(clusterNum):
         clu = tweets[["xm","ym","Clus_km"]][tweets.Clus_km==i]
         cenx.append(np.mean(clu.xm))
         ceny.append(np.mean(clu.ym) )
         plt.text(cenx[i],ceny[i],str(i), fontsize=25, color='red',)
        #print "Cluster "+str(i)+', Avg Temp: '+ str(np.mean(cluster.Tm))

    #Visualization1
    for index,row in tweets.iterrows():
        my_map.plot(row.xm, row.ym,markerfacecolor =colors[np.float(row.Clus_km)],  marker='o', markersize= 5, alpha = 0.75)
        #plt.plot([cenx[int(row.Clus_km)],row.xm], [ceny[int(row.Clus_km)],row.ym],'-',color=colors[np.float(row.Clus_km)] )

    plt.show()

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError, e:
    return False
  return json_object

if __name__ == '__main__':


    sc  = SparkContext('local[4]', 'Social Panic Analysis')
    tweets_data = sc.textFile(tweets_data_path)
    tweets=tweets_data.map(lambda post: is_json(post))\
        .filter(lambda post: post != False)\
        .filter(lambda post: 'created_at' in post)\
        .map(lambda post: [get_coord(post)[0],get_coord(post)[1],tokenize(post['text'])])\
        .filter(lambda post: post[0] != 0)



    #direct tf-idf: https://spark.apache.org/docs/1.5.2/mllib-feature-extraction.html#tf-idf
    from pyspark.ml.feature import HashingTF,IDF, Tokenizer
    sqlContext=SQLContext(sc)
    tweets_rdd = tweets.map(lambda p: Row(long=float(p[0]), lat=float(p[1]), text=' '.join(p[2])))
    # Infer the schema, and register the DataFrame as a table.
    tweets_df = sqlContext.createDataFrame(tweets_rdd)
    tweets_df.registerTempTable("people")

    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(tweets_df)
    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    #rescaledData.show(5)
    # for features_label in rescaledData.select("words", "features").take(3):
    #     print(features_label)
    #convert Sparse vector to Vector
    x=rescaledData.select(rescaledData['features']).map(lambda r:r[0].toArray())

    from pyspark.mllib.feature import StandardScaler
    scaler = StandardScaler(withMean=True, withStd=True)
    scalerModel = scaler.fit(x)
    normalizedData = scalerModel.transform(x)
    #print normalizedData.take(5)

    # Build the model (cluster the data)
    from pyspark.mllib.clustering import KMeans, KMeansModel
    clusterNum = 5
    clusters = KMeans.train(normalizedData, clusterNum, maxIterations=10,
             initializationMode="random")
    #print clusters.centers

    clust=normalizedData.map(lambda x:clusters.predict(x))
    #clust.take(5)

    data= rescaledData.select(rescaledData['long'], rescaledData['lat'],rescaledData['words']).collect()
    labels=clust.collect()
    pdf = pd.DataFrame(data)
    pdf['Clus_km']=labels
    pdf.columns=['long','lat','words','Clus_km']
    print pdf.head()

    plotit(pdf,clusterNum)