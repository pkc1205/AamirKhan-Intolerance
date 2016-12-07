from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time 
ckey =  'YDvMtLCSLnslXX3m4pY4z2aDb'
csecret = 'cOi1FkNVgibru1rp1Rtcy3sdUjzujc5JAGowxk0YkFmbq0Otmu'
atoken =  '3534109580-EnBKBt17Y2XEuaZc8cmUXPLNRgIfZN1vM0NBB6w'
asecret =  'UavesZwDDMEDmkWgutZXGvBDynE4qB4QxktiuHbIKtw0W'
class listener(StreamListener):
	def on_data(self,data):
			print data
			saveFile = open('Intolerance4.txt' ,'a')
			saveFile.write(data)
			saveFile.write('\n')
			saveFile.close()
			return True
	def on_error(self, status):
		print status
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth,listener())
twitterStream.filter(track = ["amirkhan","snapdeal","Godrej","samsung","aamirkhan","intolerance","INTOLERANCE","AAMIRKHAN","SRKINTOLERANCE","tolerance","Intolerant","tolerant","tolerance"])
 
