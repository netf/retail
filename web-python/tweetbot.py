__author__ = 'EMEA'

import sys
import tweepy


def main():
   consumer_key = "sv7DjZk2HGHN5yjgYSmUGY4oY"
   consumer_secret = "zc8jRJOj7CEH4ywbkAKPE0XOmyUf95Xs1DPJXqs7Cvwz7HJgIS"
   access_token = "3874146432-2RpBAzi99McYRiVCMutqk8BArIqNjZCDfYJBy4J"
   access_token_secret = "4rhKB05RzFfZoIz29JPq0zAbVtFD3BiDwIjnE9L3S7cxV"

   auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
   auth.set_access_token(access_token, access_token_secret)

   api = tweepy.API(auth)

   api.update_status("Test 123")



if __name__ == '__main__':
   main()
   sys.exit(0)

