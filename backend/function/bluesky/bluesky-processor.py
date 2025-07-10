### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: MUHAMMAD BAYU PRAKOSO AJI

from flask import request, current_app
import requests
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer # to work with sentiment score calculation
from textblob import TextBlob # to work with subjectivity calculation
import json # to work with json file
from datetime import datetime, timezone
print("import ok")

# === CONFIG ===
SESSION_ENDPOINT = "https://bsky.social/xrpc/com.atproto.server.createSession"
UNIFY_URL = "http://router.fission/data-unify-2"

# === SESSION AUTH ===
def authenticate(username, password):
    res = requests.post(SESSION_ENDPOINT, json={"identifier": username, "password": password})
    res.raise_for_status()
    return res.json()["accessJwt"]

# === CALCULATE SENTIMENT ===
def cal_sentiment(text):

    """
    To calculate sentiment score and subjective score based on the text have been created.
    
    Parameters
    text (str) : text from reddit, combination of title and selftext
    
    Returns:
    - JSON sentiment and subjectivity scores
    """
    #print(">>> Starting sentiment analysis function") # print log message for initialisation
    
    sia = SentimentIntensityAnalyzer() # firstly set the sentiment analyser
    
    try: # if successful
        
        if not text: # if text does not exist
            #(">>> Warning: No text provided in input.") # get log message
            return {"error": "No text provided."}, 200 # return error value in JSON with code 200
    
        #print(">>> Calculating VADER sentiment...") # log message 
        vader_scores = sia.polarity_scores(text) # calculate vader sentiment score
        #print(">>> VADER scores calculated") # print log message again
    
        #print(">>> Calculating TextBlob subjectivity...") # print log message 
        textblob_score = TextBlob(text).sentiment.subjectivity # get subjectivity scores
        #print(">>> TextBlob subjectivity calculated") # print log message
    
        
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
        


# === POST SCORING ===
def extract_and_score(post):
    try:
        record = post.get("record", {})
        author = post.get("author", {})

        text = record.get("text", "")
        sentiment = cal_sentiment(text)
        
        iso_time = record.get("createdAt")
        iso_time = iso_time.replace('+00:00', 'Z')
        print(f"isotime :{iso_time}") 
        dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        formatted = dt.strftime("%d-%m-%y %H:%M:%S")

        return {
            "uri": post.get("uri"),
            "createdAt": formatted,
            "text": text,
            "sentiment": sentiment,
            "source" : "bluesky"
        }
    except Exception as e:
        current_app.logger.error(f"Post processing error: {e}")
        return None

print("function stored")
# === MAIN FUNCTION ===
def main():
    print("main")
    try:
        data = request.get_json(force=True)
        posts = data.get("posts", [])
        current_app.logger.info(f"Received {len(posts)} posts")        
        filtered = []
      
        for post in posts:
            scored = extract_and_score(post)
            if scored:
                # === Forward to Unify
                try:
                    # Respond to unifying data
                    requests.get('http://router.fission/data-unify-2', json=scored)
                    current_app.logger.info("Sent to data-unify-2")
                except Exception as e:
                    current_app.logger.error(f"Error sending to unify: {e}")

        return {"statusCode": 200, "body": "process finish"}

    except Exception as e:
        current_app.logger.error(f"Main processor error: {e}")
        return {"statusCode": 500, "body": str(e)}

