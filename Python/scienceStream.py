import requests
import json 

def stream_science_posts():
	r = requests.get('https://www.reddit.com/r/science/new/posts/.json?limit=10', stream = True, timeout = 10)
	for post in r.iter_lines():
		if post:
			json = json.loads(post)
			yield post

if __name__ == "__main__":
    stream_science_posts()
