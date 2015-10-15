#!/usr/bin/env python
__author__ = 'EMEA'

import sys
import time
import stompy
import tweepy
import logging
from daemonizer import Daemon
from stompy.simple import Client

# logger settings
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - '
                              '%(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.propagate = False
logger.addHandler(ch)

consumer_key = "sv7DjZk2HGHN5yjgYSmUGY4oY"
consumer_secret = "zc8jRJOj7CEH4ywbkAKPE0XOmyUf95Xs1DPJXqs7Cvwz7HJgIS"
access_token = "3874146432-2RpBAzi99McYRiVCMutqk8BArIqNjZCDfYJBy4J"
access_token_secret = "4rhKB05RzFfZoIz29JPq0zAbVtFD3BiDwIjnE9L3S7cxV"


class StompClient(object):
    def __init__(self, host='localhost', port=61613):
        self.stomp = Client(host=host, port=port)
        self.stomp.connect()

    def subscribe(self, queue):
        self.stomp.subscribe(queue, ack='auto')

    def get(self):
        return self.stomp.get()


class TweetClient(object):
    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)

    def say(self, message):
        if 0 < len(message) <= 140:
            self.api.update_status(message)
        else:
            logger.error("Message too long")


class MyDaemon(Daemon):
    def run(self):
        amq = StompClient()
        amq.subscribe('/topic/HotProducts')
        tweeter = TweetClient(consumer_key, consumer_secret, access_token, access_token_secret)
        while True:
            m = amq.get()
            try:
                tweeter.say(m.body)
                logger.info("Tweeted: %s" % m.body)
            except tweepy.error.TweepError:
                logger.error("Error tweeting: %s" % m.body)
            time.sleep(5)


def main():
    daemon = MyDaemon('/tmp/tweetbot.pid', stdout='/tmp/tweetbot.log', stderr='/tmp/tweetbot.log')
    if len(sys.argv) == 2:
        if 'start' in sys.argv[1]:
            daemon.start()
        elif 'stop' in sys.argv[1]:
            daemon.stop()
        elif 'restart' in sys.argv[1]:
            daemon.restart()
        else:
            print "Unknown command"
            sys.exit(1)
    else:
        print "Usage: %s start|stop|restart" % sys.argv[0]

if __name__ == '__main__':
   main()
   sys.exit(0)

