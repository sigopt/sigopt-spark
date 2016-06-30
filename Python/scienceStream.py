import sys
import requests
import json
from kafka.client import SimpleClient
from kafka.producer import KeyedProducer

class Producer(object):

  def __init__(self, addr):
    self.client = SimpleClient(addr)
    self.producer = KeyedProducer(self.client)

  def stream_science_posts(self, key):
    r = requests.session()
    header = {"User-Agent": "anisotropix Science"}
    s = r.get('https://www.reddit.com/r/science/new/.json?limit=100', stream=True, headers=header)
    for post in s.iter_lines():
      if post:
        self.producer.send_messages('Science_posts', key,  post)
        print (post)

if __name__ == "__main__":
  args = sys.argv
  partition_key = str(args[1])
  prod = Producer()
  while(True):
    prod.stream_science_posts(partition_key)
