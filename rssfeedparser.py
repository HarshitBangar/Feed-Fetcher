from redis_interface import RedisInterface
import feedparser
import pickle
import MySQLdb
import rfc822
import nltk

# Open the redis interface
redisInterface = RedisInterface()

# Setting up the db connection
db = MySQLdb.connect(host="localhost", user="harshit", passwd="bangar",db="news",use_unicode=True,charset='UTF8')
cursor = db.cursor()

# Populating the dictionary with latest time-stamps
timestamp_dict = {}
cursor.execute("select * from latest_timestamps")
timestamp_db = cursor.fetchall()
for r in timestamp_db:
	timestamp_dict[r[0]] = r[1]

# Getting the last index in links table
cursor.execute("select max(id) from links")
last_id = cursor.fetchone()[0]
if (last_id):	
	curId = int(last_id) + 1
else:
	curId = 1
print "Current cursor id:" + str(curId)

# Getting the RSS Feed from selected news-site
def parsingFeeds(**rssUrlList):
    global curId
    for key, rssUrl in rssUrlList.iteritems():
	    print rssUrl
	    feed = feedparser.parse(rssUrl)
	    print key
	    last_timestamp = timestamp_dict.get(key)
	    if (last_timestamp):
		last_timestamp = float(last_timestamp)	
	    else:
		last_timestamp = 0
		cursor.execute("insert into latest_timestamps values ('%s', '%s')"% (key, str(last_timestamp)))
	    print "Last Timestamp:" + str(last_timestamp) 
		
	    newLast_timestamp = last_timestamp
	    for entry in feed['entries']:
		# Pushing to both redis and DB
		if (entry['title'] and entry['published'] and entry['summary']):
			newsTimeStamp = rfc822.mktime_tz(rfc822.parsedate_tz(entry['published']))
			if(newsTimeStamp<last_timestamp):
				continue
			if(newLast_timestamp<newsTimeStamp):
				newLast_timestamp = newsTimeStamp
			stringnewsTimeStamp = str(newsTimeStamp)
			print curId;
			cleaned_summary = nltk.clean_html(entry['summary'])

			# Inseting into DB
			print "insert into links values ('%s', '%s', '%s', '%s', '%s',%s)"%(
				curId, entry['title'].replace("'", r"\'"), entry['link'].replace("'", r"\'"), 
				stringnewsTimeStamp, cleaned_summary.replace("'", r"\'"), key)

			cursor.execute("insert into links values ('%s', '%s', '%s', '%s', '%s', '%s')"%(
				curId, entry['title'].replace("'", r"\'"), entry['link'].replace("'", r"\'"), 
				stringnewsTimeStamp, cleaned_summary.replace("'", r"\'"), key))
			# Save to redis
			# Separator for extracting out the key
		        redisInterface.saveArticleData(key+"::"+entry['link'], 'feed_data')

			curId = curId + 1
	
		#Updating the latest time-stamp for the particular key
		cursor.execute("update latest_timestamps set latpublished='%s' where newssource='%s'" %(newLast_timestamp, key))	
 		db.commit()

newsSiteDictionary = { 'ibn-national':'http://ibnlive.in.com/ibnrss/rss/india/india.xml', 'hindu':'http://www.thehindu.com/news/national/?service=rss', 'toi':'http://timesofindia.feedsportal.com/c/33039/f/533916/index.rss', 'ht':'http://feeds.hindustantimes.com/HT-India'}

# Entry Point
parsingFeeds(**newsSiteDictionary)
