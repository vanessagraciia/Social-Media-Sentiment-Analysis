### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: MUHAMMAD SHAFARYANTORO

from flask import request, current_app  # to work with request flask
import json  # to work with json
from datetime import datetime  # to work with date and time conversion
import requests  # to work with HTTP API requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # to work with sentiment score calculation
from textblob import TextBlob # to work with subjectivity score calculation
import logging
from typing import Dict, List, Any
from elasticsearch8 import Elasticsearch

def secret(k):
    """
    Reads the Reddit secret from the mounted secret path.

    Args:
    - k (str): The key name (e.g., 'CLIENT_ID', 'CLIENT_SECRET')

    Returns:
    - (str): The value read from the secret file
    """
    with open(f"/secrets/default/elastic-secret/{k}", 'r') as f:
        return f.read().strip()  # Remove any newline or trailing spaces

def elastic(posts):
    """
    To store filtered post directly to ElasticSearch because we already create different function but it is flooding the pods in kubernets

    Parameter:
    - filtered_post (json) : data from reddit to be stored in JSON

    Returns:
    - OK if succeded, error message if failed.
    """
    try:
        # Read environment variables of elastic search
        es_client: Elasticsearch = Elasticsearch('https://elasticsearch-master.elastic.svc.cluster.local:9200',
                                                 verify_certs = False, # do not verify certificat
                                                 ssl_show_warn = False, # no show warning
                                                 basic_auth = (secret('USERNAME'), secret('PASSWORD'))
                                                 ) # set the authentication 
        index_response = es_client.index( # get index response
            index = 'reddit', # get index of reddit-new from elastic search
            id = posts['id'], # use identification as id
            body = posts # get posts as body
        )
        current_app.logger.info( # to print log info
            f"Indexed observation {posts['id']} - " # print indexed observation
            f"Version: {index_response['_version']}" # print version
        )
        return 'OK' # return ok if finished
    except Exception as e: # if exception exists
        return f"Error indexing posts to Elasticsearch: {str(e)}" # print error message
 

def sentiment(text):
    """
    To calculate sentiment score and subjective score based on the text have been created.

    Parameters
    text (str) : text from reddit, combination of title and selftext

    Returns:
    - JSON sentiment and subjectivity scores
    """
    print(">>> Starting sentiment analysis function") # print log message for initialisation
    
    sia = SentimentIntensityAnalyzer() # firstly set the sentiment analyser
    
    try: # if successful
        
        if not text: # if text does not exist
            print(">>> Warning: No text provided in input.") # get log message
            return {"error": "No text provided."}, 200 # return error value in JSON with code 200

        print(">>> Calculating VADER sentiment...") # log message 
        vader_scores = sia.polarity_scores(text) # calculate vader sentiment score
        print(">>> VADER scores calculated") # print log message again

        print(">>> Calculating TextBlob subjectivity...") # print log message 
        textblob_score = TextBlob(text).sentiment.subjectivity # get subjectivity scores
        print(">>> TextBlob subjectivity calculated") # print log message

        
        # store the response
        response = {
            "negative": round(vader_scores['neg'], 4), # negative sentiment score
            "neutral": round(vader_scores['neu'], 4), # neutral sentiment score
            "positive": round(vader_scores['pos'], 4), # positive sentiment score
            "compound": round(vader_scores['compound'], 4), # compound sentiment score
            "subjectivity": round(textblob_score, 4) # subjectivity score
        }

        print(">>> Final sentiment response calculated") # print log message
        return response # return the result to processor like this

    except Exception as e: # handling error
        print(">>> ERROR in sentiment analysis:", str(e)) # print log message
        return {"error": f"Sentiment error: {str(e)}"}  # return data into error one


def main():
    """
    Extract selected features from Reddit posts.

    This function is designed for Fission and Kubernetes cloud. It receives Reddit post data
    as JSON via HTTP request and returns only specific fields from each post.

    Returns:
    - List of posts, each containing only:
      ['id', 'title', 'selftext', 'subreddit', 'created_utc', 'author', 'url',
       'num_comments', 'num_upvotes', 'upvote_ratio', 'sentiment', 'source']
    """
    print(">>> Starting reddit processor")

    req = request.get_json(force = True)  # Force parsing of JSON from the request
    print(">>> Received request")  # print log message

    if req and "data" in req and "children" in req["data"]: # if exists data and children in req
        posts = req["data"]["children"] # get posts within data and children

        for post_wrapper in posts: # iterate over post
            post_data = post_wrapper.get("data", {}) # get data 

            text = f"{post_data.get('title', '')} {post_data.get('selftext', '')}".strip() # set text as parameter to reddit-sentiment-2

            # Sentiment analysis call
            try:
                sentiment_result = sentiment(text) # get sentiment from function above
            except Exception as e: # handling error
                sentiment_result = {"error": f"Sentiment error: {str(e)}"} # store the sentiment as error, with error message
                print(">>> ERROR in sentiment:", str(e)) # print log messages

            
            try: # Timestamp conversion
                created_time = datetime.utcfromtimestamp(post_data.get("created_utc")).strftime("%d-%m-%y %H:%M:%S") # set created time as in ISO format
            except Exception as e: # handling error
                created_time = None # set none
                print(">>> ERROR in timestamp conversion:", str(e)) # print log message

            filtered_post = { # get filtered post containing these variables
                "id": post_data.get("id"), # id
                "title": post_data.get("title"), # title of reddit
                "selftext": post_data.get("selftext"), # selftext of reddit
                "subreddit": post_data.get("subreddit"), # subreddit name
                "created_at": created_time, # post created in ISO
                "author": post_data.get("author"), # get author
                "url": post_data.get("url"), # get url
                "num_comments": post_data.get("num_comments"), # get number of comments
                "num_upvotes": post_data.get("ups"), # get number of upvote 
                "upvote_ratio": post_data.get("upvote_ratio"), # get upvote ratio
                "sentiment": sentiment_result, # get sentiment
                "source": "reddit" # set source from reddit 
            }

            # Send to reddit-elastic-2
            try:
                elastic_res = elastic(filtered_post) # send the output to reddit elastic search
                print(">>> Sent to reddit elasticsearch database") # show log message
            except Exception as e: # handling error
                print(">>> ERROR sending to database:", str(e)) # show log message

            # Send to data-unify-2
            try:
                unify_res = requests.get('http://router.fission/data-unify-2', json = filtered_post) # send to data unify for standardisation
                print(">>> Sent to data-unify-2, status:", unify_res.status_code) # show log message
            except Exception as e:
                print(">>> ERROR sending to data-unify-2:", str(e)) # print log messages for debugging and handling error

    else:
        print(">>> Invalid request format: missing 'data' or 'children'") # if error occurs from the beginning, show this message

    print(">>> Reddit processor completed") # if process completed, show this message
    return "OK" # show OK if completed
