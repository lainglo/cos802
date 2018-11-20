from mrjob.job import MRJob, MRStep
from langdetect import detect

class MRLangDetect(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)        ]

    def mapper(self, _, line):
    
#ID, CONTENT, CREATEDAT, RETWEET, RTID, RTUSERNAME, OPEN_DATE, USERID, USERNAME, FULLNAME, DESCRIPTION, GEO_ENABLED, LATITUDE, LONGITUDE, LOCATION, TIMEZONE, LANGUAGE, FOLLOWERS, FRIENDS, RTFOLLOWERS, TRANSLATOR, STATUS_COUNT, PROFILE_IMAGE, BACKGROUND_IMAGE, BANNER_IMAGE, IS_FRIEND, IS_FOLLOWER, IS_DEFAULT_PROFILE, SIZE, LAST_UPDATED, INREPLYTO, SOURCE

        data = line.split(",")
        tweet = data[1]
        lang = detect(tweet)
        
        yield lang, 1

    def reducer(self, key, values):

        yield key, sum(values)

    def another_reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRLangDetect.run()
