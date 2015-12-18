import socket
import sys
from thread import *
import requests
import requests_oauthlib
import tweepy
from tweepy import OAuthHandler
import json
import oauth2 as oauth
from datetime import datetime
#Variables that contains the user credentials to access Twitter API
access_token = "582342005-QGM3VSdAL1cjAPzL6jaHebOHUfdqVtwddcHJhHBS"
access_token_secret = "keEVSlaNz5fegUq8ytMrTXq62paf41UI8KlH6aBH5DrWU"
consumer_key = "PjlYiBasD06wnMOH54cxwWnDO"
consumer_secret = "EXVZnDVb3wLA6KhwOfp9weBSngJEUi1TJxNvRZsW9yp3IJ3bL7"
auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_token_secret)
#usa
llon = -130
ulon = -60
llat = 20
ulat = 50
url='https://stream.twitter.com/1.1/statuses/filter.json'
data      = [('language', 'en'),('locations', str(llon)+','+str(llat)+','+str(ulon)+','+str(ulat)),('track','sex,food'),]
#,('track','ibm,google,microsoft')
#, ('locations', str(llon)+','+str(llat)+','+str(ulon)+','+str(ulat))
query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in data])
response  = requests.get(query_url, auth=auth, stream=True)
print(query_url, response) # 200 <OK>
count = 0
with open('../dataset/sexfood50.json', 'a') as f:
    #f.write('1'+'\n')
    for line in response.iter_lines():  # Iterate over streaming tweets
        try:
            if count > 50:
                break
                f.close()
            #post= json.loads(line.decode('utf-8'))
            #contents = [post['text'], post['coordinates'], post['place']]
            count+= 1
            #conn.send(line+'\n')
            print count
            f.write(line+'\n')
            #time.sleep(1)

            print (str(datetime.now())+' '+'count:'+str(count))
        except:
            e = sys.exc_info()[0]
            print( "Error: %s" % e )



