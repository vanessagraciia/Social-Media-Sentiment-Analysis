### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: MUHAMMAD BAYU PRAKOSO AJI

import requests
import time
from datetime import datetime

# === Config ===

DISPATCH_URL = "http://router.fission/bluesky-harvester"
LIMIT_PER_PAGE = 50


def main():
    
    now = datetime.utcnow()
    body={
        "q": "the, a, and",
        "mode": "new",
        "since": f"{now:%Y-%m-%d}T00:00:00.0000Z",
        "until": f"{now:%Y-%m-%d}T23:59:59.9999Z"
    }
    
    
    try:
        res = requests.get(DISPATCH_URL, json = body)
        print(f"send req : {body}")
        return {"statusCode": 200, "body": "Harvester triggered with params"}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}