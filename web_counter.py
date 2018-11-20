from mrjob.job import MRJob, MRStep
from langdetect import detect
import re
from urllib.parse import urlparse

class MRWebDetect(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)        ]

    def mapper(self, _, line):
    
#ID, CONTENT, CREATEDAT, RETWEET, RTID, RTUSERNAME, OPEN_DATE, USERID, USERNAME, FULLNAME, DESCRIPTION, GEO_ENABLED, LATITUDE, LONGITUDE, LOCATION, TIMEZONE, LANGUAGE, FOLLOWERS, FRIENDS, RTFOLLOWERS, TRANSLATOR, STATUS_COUNT, PROFILE_IMAGE, BACKGROUND_IMAGE, BANNER_IMAGE, IS_FRIEND, IS_FOLLOWER, IS_DEFAULT_PROFILE, SIZE, LAST_UPDATED, INREPLYTO, SOURCE
        data = line.split(",")
        tweet = data[1]
        try:
            urls = re.search("(?P<url>https?://[^\s]+)", tweet).group("url")
        except AttributeError:
            return

        if type(urls) == list:
         for url in urls:
          base = urlparse(url).netloc
          yield base, 1
        else:
          base = urlparse(urls).netloc
          yield base, 1

    def reducer(self, key, values):

        yield key, sum(values)

    def another_reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWebDetect.run()
