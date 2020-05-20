import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import socket
import json

# Source:
# https://towardsdatascience.com/hands-on-big-data-streaming-apache-spark-at-scale-fd89c15fa6b0

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''


class TweetsListener(StreamListener):

    def __init__(self, client_socket):
        self.client_socket = client_socket

    def on_data(self, data):

        # receives data from tweets stream and
        # send process them to client socket
        try:
            message = json.loads(data)
            print(message['text'].encode('utf-8'))
            self.client_socket.send(message['text'].encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def if_error(self, status):
        print(status)
        return True


def send_tweets(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # creating stream object with new object as a listener
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    # start a stream
    twitter_stream.filter(languages=["en"], track=['coronavirus'])


if __name__ == "__main__":
    new_socket = socket.socket()
    host = "localhost"
    port = 5599
    new_socket.bind((host, port))
    print("Now listening on port: %s" % str(port))

    new_socket.listen(5)
    connection_socket, address = new_socket.accept()

    print("Received request from: " + str(address))
    send_tweets(connection_socket)
