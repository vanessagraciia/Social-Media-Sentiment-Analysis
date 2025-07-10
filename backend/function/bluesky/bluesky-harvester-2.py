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
import os
import json
import time
from datetime import datetime
from dateutil.parser import parse
import redis

redis_client = redis.Redis(host='redis-headless.redis.svc.cluster.local', port=6379, decode_responses=True)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
SEARCH_ENDPOINT = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
SESSION_ENDPOINT = "https://bsky.social/xrpc/com.atproto.server.createSession"
PROFILE_ENDPOINT = "https://bsky.social/xrpc/app.bsky.actor.getProfile"
PROCESSOR_URL = "http://router.fission/bluesky-processor"
LIMIT_PER_PAGE = 50

def build_state_key(mode: str, query: str, date: str) -> str:
    return f"bluesky:day_state:{mode}:{query.lower().replace(' ', '_')}:{date}"

def build_job_key(mode: str, query: str, date: str) -> str:
    return f"bluesky:queue_stat:{mode}:{query.lower().replace(' ', '_')}:{date}"


def authenticate(username, password):
    res = requests.post(SESSION_ENDPOINT, json={"identifier": username, "password": password})
    res.raise_for_status()
    return res.json()["accessJwt"]

def load_state(mode, query, date):
    key = build_state_key(mode, query, date)
    try:
        return redis_client.get(key)
    except Exception as e:
        current_app.logger.error(f"Redis read error for key {key}: {e}")
        return None
        
def save_state(mode, query, date, cursor):
    key = build_state_key(mode, query, date)
    try:
        redis_client.set(key, cursor)
        current_app.logger.info(f"Saved {mode} cursor for '{date}' ? {cursor}")
    except Exception as e:
        current_app.logger.error(f"Redis write error for key {key}: {e}")
        
def del_state(mode, query, date):
    key = build_state_key(mode, query, date)
    try:
        redis_client.delete(key)
        current_app.logger.info(f"Saved {mode} cursor for '{date}' ? {cursor}")
    except Exception as e:
        current_app.logger.error(f"Redis write error for key {key}: {e}")
        
def save_job(mode, query, date, job):
    key = build_job_key(mode, query, date)
    try:
        redis_client.set(key, job)
        current_app.logger.info(f"Saved Job {mode} cursor for '{date}' ? {job}")
    except Exception as e:
        current_app.logger.error(f"Redis write error for key {key}: {e}")
        
        
# === AUSTRALIAN USER CHECK ===
def is_australian_user(author, profile) -> bool:
    handle = author.get("handle", "").lower()
    display = author.get("displayName", "").lower()
    description = profile.get("description", "").lower() if profile else ""

    australia_keywords = [
        "australia", "australian", "aussie", "oz","au","down under",
        "sydney", "syd", "melbourne", "melb", "brisbane", "perth", "adelaide", "hobart",
        "darwin", "canberra", "gold coast", "geelong", "wollongong", "townsville",
        "queensland", "new south wales", "nsw", "victoria", "vic", "tasmania",
        "south australia", "western australia", "wa", "nt", "act",
        "australian government", "ausgov", "auspol", "ausnews", "australian politics", "australian news",
        "uq", "unimelb", "unsw", "rmit",
        "abc news", "sbs", "the guardian au", "the conversation au", "crikey", "9news", "7news"
    ]

    combined_fields = f"{handle} {display} {description}"
    return any(keyword in combined_fields for keyword in australia_keywords)


def secret(k):
    """
    Reads the Reddit secret from the mounted secret path.

    Args:
    - k (str): The key name (e.g., 'CLIENT_ID', 'CLIENT_SECRET')

    Returns:
    - (str): The value read from the secret file
    """
    with open(f"/secrets/default/bluesky-secret/{k}", 'r') as f: # open file from the secrets filepath
        #print(f.read().strip())
        return f.read().strip()  # Remove any newline or trailing spaces

def isendofday(oldest_ts: str, until_ts: str) -> bool:
    try:
        oldest_date = parse(oldest_ts).date()
        until_date = parse(until_ts).date()
        return oldest_date < until_date
    except Exception as e:
        print(f"[ERROR] Failed to parse date: {e}")
        return False

def main():
    
    print("harvester trigger start")
    query = request.get_json(force = True)
    print(query)
    q = query.get("q", "the, and, a")
    #print(q)
    mode = query.get("mode", "backday")
    #print(mode)
    until = query.get("until", None)
    print(f"until : {until}")
    since = query.get("since", None)
    print(f"since : {since}")
    limit = int(query.get("limit", LIMIT_PER_PAGE))
    #print(limit)
    
    cursor = load_state(mode, q, until)
    print(f"state cursore : {cursor}")
    
    # === Authenticate ===
    
    client = secret("CLIENT_ID") # get client id from secret function reading pods
    asecret = secret("CLIENT_SECRET") # get client secret from secret function reading pods
    try:
        token = authenticate(
            username= client ,
            password= asecret
        )
    except Exception as e:
        current_app.logger.error(f"Auth error: {e}")
        return {"statusCode": 401, "body": "Authentication failed"}

    headers = {"Authorization": f"Bearer {token}"}

    if cursor != "finish":
        while True:
            params = {
                "q": q,
                "limit": limit,
                "sort": "top",
                "lang" : "en"
            }
            if since:
                params["since"] = since
            if until:
                params["until"] = until
            if cursor:
                params["cursor"] = cursor

            try:
                res = requests.get(SEARCH_ENDPOINT, headers=headers, params=params)
                res.raise_for_status()
                data = res.json()
                current_app.logger.info(f"Get from API param: {params}")
            except Exception as e:
                save_state(mode, q, until, cursor)
                save_job(mode, q, until, "paused")
                current_app.logger.error(f"Fetch error: {e}")
                break

            posts = data.get("posts", [])
            au_post = []
            if not posts:
                del_state(mode, q, until)
                save_job(mode, q, until, "finish")
                current_app.logger.info(f"XXX  finish no Data Left")
                break
                
            for post in posts:
                author = post.get("author", {})
                did = author.get("did")
            
            # Get author profile for description
                try:
                    profile_res = requests.get(PROFILE_ENDPOINT, params={"actor": did}, headers=headers, timeout=5)
                    profile = profile_res.json()
                except Exception as e:
                    current_app.logger.warning(f"Failed to enrich profile for {did}: {e}")
                    profile = {}

                if is_australian_user(author, profile):
                    au_post.append(post)
            current_app.logger.info(f"XXX  get {len(au_post)} Aupost ")
                
            # === Forward to processor ===
            if au_post:
                try:
                    forward_payload = {"posts": au_post}
                    requests.get(PROCESSOR_URL, json=forward_payload, timeout=3)
                    current_app.logger.info(f"Forwarded {len(au_post)} posts to bluesky-processor")
                except Exception as e:
                    None
          
            cursor = data.get("cursor")
            save_state(mode, q, until, cursor)
            oldest = posts[-1]["record"]["createdAt"]
            lenpost = cursor
            """
            if oldest :
                if isendofday(oldest, until):
                    save_state(mode, q, until, "finish")
                    save_job(mode, q, until, "finish")
                    current_app.logger.info(f"XXX  finish no Data Left")
                    break
            """
            if int(cursor) >= 1500:
                del_state(mode, q, until)
                save_job(mode, q, until, "finish")
                current_app.logger.info(f"XXX  finish on Last Page")
                break
                
            if not cursor:
                del_state(mode, q, until)
                save_job(mode, q, until, "finish")
                current_app.logger.info(f"XXX  finish on Last Page")
                break

            time.sleep(0.3)

    else :
       current_app.logger.info(f"Jobs already finish")
    
    current_app.logger.info(f"Harvested {lenpost} posts(q={q})")
    
    return { "statusCode": 200, "body": "Harvester Trigger Finish"}

