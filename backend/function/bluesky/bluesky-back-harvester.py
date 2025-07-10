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
import time
import redis
from datetime import datetime, timedelta

# === Config ===

DISPATCH_URL = "http://router.fission/bluesky-harvester"
LIMIT_PER_PAGE = 50

redis_client = redis.Redis(host='redis-headless.redis.svc.cluster.local', port=6379, decode_responses=True)


def build_day(mode: str, query: str) -> str:
    return f"bluesky:last_d_harvest:{mode}:{query.lower().replace(' ', '_')}"


def load_day(mode, query):
    key = build_day(mode, query)
    try:
        return redis_client.get(key)
    except Exception as e:
        current_app.logger.error(f"Redis read error for key {key}: {e}")
        return None

def save_day(mode, query, day):
    key = build_day(mode, query)
    try:
        redis_client.set(key, day)
        current_app.logger.info(f"Saved day harvested {mode} cursor for '{day}' ?")
    except Exception as e:
        current_app.logger.error(f"Redis write error for key {key}: {e}")

def main():
    
    mode = "backday"
    q = "the, a, and"
    print(f"mode and key : {mode, q}")
    day_harvested = load_day(mode, q) or "2025-04-27"
    print(f"day harvested : {day_harvested}")
    day_dt = datetime.strptime(day_harvested, "%Y-%m-%d").date()
    day_to_harvest = day_dt - timedelta(days=1)
    print(f"day to harvest : {day_to_harvest}")
    save_day(mode, q, day_to_harvest.strftime("%Y-%m-%d"))
    since  = f"{day_to_harvest:%Y-%m-%d}T00:00:00.0001Z"
    until  = f"{day_to_harvest:%Y-%m-%d}T23:59:59.9999Z"
    
    print(f"since, until : {since, until}")
    
    body={ "q": q, "mode": mode, "since": since, "until": until}
    
    
    try:
        res = requests.get(DISPATCH_URL, json = body)
        print(f"send req : {body}")
        return {"statusCode": 200, "body": "Harvester triggered with params"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}