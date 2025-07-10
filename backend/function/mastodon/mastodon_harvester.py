### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: VIANE DORTHEA TIWA

from mastodon import Mastodon
import json
from datetime import datetime, timezone
from tqdm import tqdm
from flask import current_app
import requests
import os
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # to work with sentiment score calculation
from textblob import TextBlob # to work with subjectivity score calculation
import threading # to work with threading processing
import redis


API_BASE_URL = "https://mastodon.au"
DEFAULT_STATE = {"last_max_id": None}

#redis state configuration
STATE_FILE = "/tmp/mastodon_state.json"

redis_client = redis.Redis(host='redis-headless.redis.svc.cluster.local', port=6379, decode_responses=True)

STATE_KEY = "mastodon:state:last_max_id"

def load_state():
    """
    Load the last_max_id from Redis.
    Returns: {"last_max_id": int or None}
    """
    try:
        last_id = redis_client.get(STATE_KEY)
        if last_id is None:
            return {"last_max_id": None}
        return {"last_max_id": int(last_id)}
    except Exception as e:
        current_app.logger.error(f"Redis read error for key {STATE_KEY}: {e}")
        return {"last_max_id": None}

def save_state(last_max_id):
    try:
        redis_client.set(STATE_KEY, str(last_max_id))
        print(f"Saved to Redis {STATE_KEY} = {last_max_id}") 
        current_app.logger.info(f"Saved last_max_id to Redis: {last_max_id}")
    except Exception as e:
        current_app.logger.error(f"Redis write error for {STATE_KEY}: {e}")

# quick test
try:
    print("Redis says:", redis_client.ping())
except redis.exceptions.AuthenticationError:
    print("Redis auth failed—check your password!")

def config(k):
    """
    To read mastodon-config file retrieving CLIENT_ID and CLIENT_SECRET for security

    Return:
    - (str) the value of parameter k from config.yaml
    """
    with open(f"/configs/default/mastodon-config/{k}", 'r') as f: # open the file in cluster
        return f.read() # return the value that have been read

def secret(k):
    """
    To read mastodon-secret file retrieving ACCESS_TOKEN for security

    Return:
    - (str) the value of parameter k from secret.yaml
    """
    with open(f"/secrets/default/mastodon-secret/{k}", 'r') as f: # open the file in cluster
        return f.read() # return the value that have been read


# Initialize Mastodon client with saved credentials
mastodon = Mastodon(
    access_token=secret("ACCESS_TOKEN"),
    api_base_url=API_BASE_URL
)

# def load_state():
#     """
#     Load state if file exists and contains valid content.
#     Otherwise, return default empty state.
#     """
#     # if there is no state file, create it with DEFAULT_STATE
#     try:
#         if not os.path.exists(STATE_FILE):
#             with open(STATE_FILE, "w", encoding="utf-8") as f:
#                 json.dump(DEFAULT_STATE, f, ensure_ascii=False, indent=2)
#             return DEFAULT_STATE.copy()
    
#         # otherwise read that file & return it
#         with open(STATE_FILE, "r", encoding="utf-8") as f:
#             return json.load(f)
        
#     except (json.JSONDecodeError, IOError):
#         print("Warning: State file unreadable or invalid JSON. Starting fresh.")
#         return {"last_max_id": None}

# def save_state(last_max_id):
#     with open(STATE_FILE, "w") as f:
#         json.dump({
#             "last_max_id": last_max_id,
#         }, f)

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def send_to_unify(record):
    try:
        requests.get('http://router.fission/data-unify-2', json=record)
    except Exception as e:
        current_app.logger.error(f"Error sending to data-unify: {e}")

def count_harvested_posts(path="mastodon_posts.jsonl") -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        # count only non-empty lines
        return sum(1 for line in f if line.strip())

# Harvest posts from public timeline
def harvest_public_posts(finish_date, max_posts, state):
    last_max_id  = state.get("last_max_id")
    
    # pick the older post than the last trigger's oldest post
    max_id       = last_max_id - 1 if last_max_id is not None else None
    
    cutoff_ts    = finish_date.replace(tzinfo=timezone.utc).timestamp()

    collected    = 0
    oldest_id    = None
    reached_cut  = False
    threads = []

    while collected < max_posts and not reached_cut:
        batch = mastodon.timeline_public(limit=40, max_id=max_id, local=True)
        if not batch:
            # current_app.logger.info("No batch received")
            break
            
        for status in batch:
            ts = status.created_at.replace(tzinfo=timezone.utc).timestamp()
            # stop for reaching before start_date
            if ts < cutoff_ts:
                reached_cut = True
                break

            # convert to int and track the smallest (oldest) ID
            this_id = int(status.id)
            if oldest_id is None or this_id < oldest_id:
                oldest_id = this_id

            record = clean_post(status)

            # spawn a thread per HTTP call
            t = threading.Thread(target=send_to_unify, args=(record,))
            t.daemon = True
            t.start()
            threads.append(t)

            collected += 1
            if collected >= max_posts:
                break

        # page older than the oldest we just harvest
        max_id = (oldest_id - 1) if oldest_id is not None else None
        
    for t in threads:
         t.join()

    if oldest_id is not None:
        save_state(oldest_id)

    if reached_cut:
        print(f"Reached cutoff date {start_date.date()}; no more posts to harvest.")

    return collected

# Clean post
def clean_post(status):
    """Convert a Mastodon.Status into JSON + scores dict."""
    content = status.content
    favs = status.favourites_count
    reblogs = status.reblogs_count
    replies = getattr(status, 'replies_count', 0)

    return {
        'id': status.id,
        'source': 'mastodon',
        'created_at': status.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'account': status.account.acct,
        'content': content,
        'language': status.language,
        'num_favourites': favs,
        'num_reblogs': reblogs,
        'num_replies': replies,
        'sentiment': sentiment(content)
    }


def main():
    state = load_state()
    print("load_state() result: Last harvested post ID is", state)
    
    finish_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    max_posts = 100
    
    result = harvest_public_posts(finish_date, max_posts, state)
    
    print("Harvest up to post with ID", state)
    print(f"Total posts harvested so far: {result} posts")

    print("Finished")

    return 'Finished MAIN'

if __name__ == '__main__':
    main()


def sentiment(text):
    """
    To calculate sentiment score and subjective score based on the text have been created.

    Parameters
    text (str) : text from reddit, combination of title and selftext

    Returns:
    - JSON sentiment and subjectivity scores
    """
    
    sia = SentimentIntensityAnalyzer() # firstly set the sentiment analyser
    
    try: # if successful
        
        if not text: # if text does not exist
            print(">>> Warning: No text provided in input.") # get log message
            return {"error": "No text provided."}, 200 # return error value in JSON with code 200

        vader_scores = sia.polarity_scores(text) # calculate vader sentiment score
        textblob_score = TextBlob(text).sentiment.subjectivity # get subjectivity scores
        # print(">>> TextBlob subjectivity calculated") # print log message

        
        # store the response
        response = {
            "negative": round(vader_scores['neg'], 4), # negative sentiment score
            "neutral": round(vader_scores['neu'], 4), # neutral sentiment score
            "positive": round(vader_scores['pos'], 4), # positive sentiment score
            "compound": round(vader_scores['compound'], 4), # compound sentiment score
            "subjectivity": round(textblob_score, 4) # subjectivity score
        }

        # print(">>> Final sentiment response calculated") # print log message
        return response # return the result to processor like this

    except Exception as e: # handling error
        print(">>> ERROR in sentiment analysis:", str(e)) # print log message
        return {"error": f"Sentiment error: {str(e)}"}  # return data into error one