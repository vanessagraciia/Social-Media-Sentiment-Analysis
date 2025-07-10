### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: MUHAMMAD SHAFARYANTORO



from flask import request, current_app # to work with request and current app
from datetime import datetime # to work with time format
import requests # to work with request API
import json # to work with JSON
import logging # to work with logging message
from typing import Dict, List, Any # to work with any type of data class
from elasticsearch8 import Elasticsearch # to work with elastic search

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
            index = 'data-unify', # get index of reddit-new from elastic search
            id = posts["unique_id"], # use identification as id
            body = posts # get posts as body
        )
        current_app.logger.info( # to print log info
            f"Indexed observation {posts['unique_id']} - " # print indexed observation
            f"Version: {index_response['_version']}" # print version
        )
        return 'OK' # return ok if finished
    except Exception as e: # if exception exists
        return f"Error indexing posts to Elasticsearch: {str(e)}" # print error message

def main(req: json):
    """
    Fission-compatible HTTP API to unify social post data across Reddit, Bluesky, and Mastodon.
    Input: Source-specific fields with a 'source' indicator.
    Output: {
        "unique_id": str,
        "created_at": str,
        "text": str,
        "sentiment": dict,
        "source": str
    }
    """
    req = request.get_json(force = True) # get json with forcing
    source = req.get("source", "").lower().strip() # get source from each data
    text, created_at = "", None # define the fallback condition

    try:
        if source == "reddit": # if source is reddit
            unique_id = req.get("id", "").strip() # get id from id
            title = req.get("title", "").strip() # get title from title
            selftext = req.get("selftext", "").strip() # get selftext
            text = f"{title} {selftext}".strip() # get text as combination of title and selftext
            created_at = req.get("created_at") # get created_at
            sentiment = req.get("sentiment") # get sentiment

        elif source == "mastodon": # if from mastodon
            unique_id = req.get("id", "").strip() # get id from id
            text = req.get("content", "").strip() # get text from content
            created_at_raw = req.get("created_at")  # expected ISO 8601 string
            created_at = datetime.strptime(created_at_raw, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%d-%m-%y %H:%M:%S") # convert it to equivalent format
            sentiment = req.get("sentiment") # get sentiment

        elif source == "bluesky": # if from bluesky
            unique_id = req.get("uri", "").strip() # get id from cid
            text = req.get("text", "").strip() # get text from text
            created_at = req.get("createdAt") # get created_at from createdAt
            sentiment = req.get("sentiment") # get sentiment

        else:
            return json.dumps({"error": f"Unsupported or missing source: {source}"}), 400 # otherwise return error of missing source

        if not text: # if not text
            return json.dumps({"error": "Text content is empty"}), 400 # text is empty

        data_unified = { # return the result as json
            "unique_id": unique_id, # unique id
            "created_at": created_at, # created at data
            "text": text, # text data
            "sentiment": sentiment, # sentiment data
            "source": source # source data
        }
        respon_elastic = elastic(data_unified)
        return respon_elastic # return the response

    except Exception as e: # exception if not works
        return json.dumps({"error": str(e)}) # return error