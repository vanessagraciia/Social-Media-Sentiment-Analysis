### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: MUHAMMAD SHAFARYANTORO

from flask import current_app, request # this is to work with log and request log
import requests # this is to work with HTTP API requests
import time # this is to work with timestamp
import json # this is to work with JSON data processing
from datetime import datetime # this is to work with datetime formatting
import os # to work with operating system
import threading # to work with threading processing

last_timestamp = None # Global in-memory last timestamp (this resets when the pod restarts)

def get_australian_cities_from_overpass():
    """
    Retrieves Australian cities from Overpass API, formatted as subreddit-friendly names.
    """
    overpass_url = "https://overpass-api.de/api/interpreter" # the url for overpass API
    # Define query for retrieving list cities (admin_level 2) in Australia (format AU), with placing name city.
    query = '''
    [out:json];
    area["ISO3166-1"="AU"][admin_level=2];
    node(area)["place"~"city"];
    out;
    '''
    # Handling error
    try:
        response = requests.get(overpass_url, params = {"data": query}, timeout = 20) # get requests from overpass API using parameters of queried data and timeout of 20 seconds
        if response.status_code != 200: # If response is outside 200
            print(f"Failed Overpass API call: {response.status_code}") # print fail message log
            return [] # return no city
        data = response.json() # otherwise get data as json format
        return sorted(set( # return list of cities that is sorted in set (no duplicate)
            element["tags"]["name"].lower().replace(" ", "") # within the data structure of tags and name
            for element in data.get("elements", []) # for each element
            if "tags" in element and "name" in element["tags"] # condition
        ))
    except Exception as e: # if error occurs
        print(f"Error fetching cities from Overpass: {e}") # print the exception
        return [] # return empty city list.

def secret(k):
    """
    Reads the Reddit secret from the mounted secret path.

    Args:
    - k (str): The key name (e.g., 'CLIENT_ID', 'CLIENT_SECRET')

    Returns:
    - (str): The value read from the secret file
    """
    with open(f"/secrets/default/reddit-secret/{k}", 'r') as f: # open file from the secrets filepath
        return f.read().strip()  # Remove any newline or trailing spaces

def fire_and_forget_get(url, json = None):
    """
    This is for threading processing so it does not wait each other
    """
    def get():
        """
        To get the requests from threading processing
        """
        try: # handling error 
            requests.get(url, json = json) # get request with parameter url and json
        except Exception as e: # if error occurs
            print("Request failed: {e}") # exception print if failed
            
    threading.Thread(target = get).start() # do threading process for designated process.

def harvest_subreddit(subreddit, headers, req):
    """
    This is to harvest subreddit so that it can be harvested in parallel work i.e. threading technique

    Parameters:
    - subreddit (str) : the current subreddit
    - headers (json) : formatted from token
    - req (json) : JSON requests from Reddit API

    Return None
    """
    
    print(f"Progress at subreddit {subreddit}") # print the log message
    after = req.get("after", {}).get(subreddit) # get the after as checkpoint indicator

    params = { # set parameters for accesing Reddit data such as
        "limit": 100, # limit maximum from subreddit is 100
        "sort": "top", # sort from the most top posts
        "t": "all", # from all period
        "after": after # with after is the after value on data
    }

    url = f"https://oauth.reddit.com/r/{subreddit}/hot" # we use this link for oauth and get hot posts, that is the most discussed and talked reddit posts (upvote * click)
    r = requests.get(url, headers = headers, params = params) # requests access to reddit

    if r and r.ok: # if request valid and ok value valid
        posts = r.json() # the post is stored to json
        if posts.get("data", {}).get("dist", 0) > 0: # if there exists data after this posts
            fire_and_forget_get('http://router.fission/reddit-processor-2', json = posts) # continue to processor with threading process as well
            last_name = posts.get("data", {}).get("children", [])[-1]["data"]["name"] # get last name
            req["after"] = {**req.get("after", {}), subreddit: last_name} # get after value
            print(f"Harvest completed at subreddit {subreddit}") # print log message that harvesting is completed.
            
def main():
    """
    Main control point to harvest data from Reddit using HTTP API.

    Returns OK if harvesting is successfully working. 
    """
    print(">>> Starting reddit harvester")
    global last_timestamp # set the timestamp
    client_id = secret("CLIENT_ID") # get client id from secret function reading pods
    client_secret = secret("CLIENT_SECRET") # get client secret from secret function reading pods
    
    # Attempt to read request JSON, fallback to secret pod
    try:
        req = request.get_json(silent = True) or {} # request json or empty dictionary
    except Exception:
        req = {} # if error exception return empty dictionary

    if not client_id or not client_secret: # if no id and secret available, print message
        return "Missing CLIENT_ID or CLIENT_SECRET. Please pass them in the request or set in secret"

    # we will retrieve some subreddits as agreed topic in australia
    subreddits = get_australian_cities_from_overpass()
    # add new subreddits from possible australian-related
    subreddits += ["australia", "aus", "victoria", "vic", "nsw", "queensland", "qld", "tasmania", "tas", "act", "southaustralia", "WesternAustralia", "northernterritory", "australian", "syd", "gaza", "ukraine", "war", "genocide"]
    headers = {"User-Agent": "reddit_harvester/0.1"} # get headers of http request
    auth = requests.auth.HTTPBasicAuth(client_id, client_secret) # get authentication using id and secret

    # OAuth token request
    data = {"grant_type": "client_credentials"} # get data of grant type and credential
    res = requests.post("https://www.reddit.com/api/v1/access_token", auth = auth, data = data, headers = headers) # get responses from reddit using authentication and data
    res_data = res.json() # get response data to json
    token = res_data.get("access_token") # get access token 
    if not token:
        return f"Failed to get access token: {res_data}" # if not available, return no token

    headers["Authorization"] = f"bearer {token}" # set token as authorisation
    result = [] # initialise empty list for result if necessary

    now_utc = int(time.time()) # set timestamp of where this code runs
    fetch_start = last_timestamp or now_utc # set fetch start from last timestamp, or if not, from current time
    threads = []
    
    for subreddit in subreddits: # iterate over subreddit
        thread = threading.Thread(target = harvest_subreddit, args = (subreddit, headers, req)) # do threading for each subreddit with target function is harvest subreddit and args are parameters
        thread.start() # start threading
        threads.append(thread) # append all threading processes
                
    return "OK" # return ok if finished