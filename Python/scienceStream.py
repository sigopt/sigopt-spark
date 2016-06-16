import requests
import json 

def stream_science_posts():
	r = requests.session()
	header = {"User-Agent": "anisotropix Science"}
	s = r.get('https://www.reddit.com/r/science/new/.json?limit=100', stream = True, headers =header, timeout = 5)#tream = True, timeout = 2)
	for post in s.iter_lines():
		if post:
			print (post)

if __name__ == "__main__":
	while(True):
         stream_science_posts()
