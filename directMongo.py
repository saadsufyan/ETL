import tweepy
import sys
import pymongo
from pymongo import MongoClient
from odo import odo

#db connection
connection = MongoClient('localhost', 27017)
db = connection.hello


c=db.feeds.aggregate([
	{
		'$project':
			{
				"accounts":"$entities.user_mentions.screen_name",
				"tags":"$entities.hashtags.text",
                                "start_date": "$created_at"
			}
	},
	{
		'$out':"hashtags"}
]);

#user application credentials
consumer_key="n2BI7yxtje6o2i00ZlkPd3EqQ"
consumer_secret="wVXN3KlUZBknkadLENWctrNh6xTRN4IqLrZiPjK1hkROaznMEI"

access_token="4877003716-NeQvJE4xgpJODavDUGmcu8oPcerfEQL03mSePwW"
access_token_secret="N5aekqDKGfGHEslVlqoOqIxHndx3TGtZRFKEjPF9bYYgX"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().hello

    def on_status(self, status):
        print status.text.encode('utf8') , "\n"

        data ={}
        data['id'] = status.id
        data['text'] = status.text
        data['created_at'] = status.created_at
        data['geo'] = status.geo
        data['source'] = status.source
        data['in_reply_to_screen_name'] = status.in_reply_to_screen_name
        data['entities'] = status.entities
        data['favorited'] = status.favorited
        data['retweeted'] = status.retweeted
        data['retweet_count'] = status.retweet_count
        data['user'] = status.user[name,screen_name]

        self.db.feeds.insert(data)

	#handle errors without closing stream:
    def on_error(self, status_code):
        print >> sys.stderr, 'Encountered error with status code:', status_code
        return True

    def on_timeout(self):
        print >> sys.stderr, 'Timeout...'
        return True

twitterStream = tweepy.streaming.Stream(auth, CustomStreamListener(api))
twitterStream.filter(track=['#PAKvWI','@englandcricket','#INDvNZ','@ESPNcricinfo'])


 

