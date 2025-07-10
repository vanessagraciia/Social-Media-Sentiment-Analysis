### CLUSTER AND CLOUD COMPUTING ASSIGNMENT 2 ###
### TEAM 20 -------------------------------  ###
### Lachlan Rowles (1081611) --------------  ###
### Muhammad Shafaryantoro (1397084) ------  ###
### Vanessa Gracia Tan (1297696) ----------  ###
### Viane Dorthea Tiwa (1413279) ----------  ###
### Muhammad Bayu Prakoso Aji (1696174) ---  ###
### PERSON IN CHARGE: LACHLAN ROWLES

import logging
import json
from typing import Optional, Dict, Any
from flask import current_app, request
from elasticsearch8 import Elasticsearch
from string import Template
from collections import defaultdict, Counter
import re
from bs4 import BeautifulSoup

import nltk
nltk.download('stopwords')

from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer


DATA_SIZE = 500
TERMS = ["gaza", "israel", "ukraine", "russia"]

# Elasticsearch query template for initial query in paginated search for
# posts containing the given keyword `term` from the given `platform`, taking
# `data_size` at a time
init_term_expr: Template = Template('''{
  "size": "${data_size}",
  "query": {
    "bool": {"must": [
        {"match": {"source": "${platform}"}},
        {"match": {"text": 
                    {"query": "${term}",
                    "fuzziness": 1}
                   }
        }
    ]}
  },
  "sort": [{"unique_id": "asc"}]
}''')

# Elasticsearch query template for subsequent queries in paginated search for
# posts containing the given keyword `term` from the given `platform`, taking
# `data_size` at a time. Taking posts starting from `search_after` unique_id
after_term_expr: Template = Template('''{
  "size": "${data_size}",
  "query": {
    "bool": {"must": [
        {"match": {"source": "${platform}"}},
        {"match": {"text": 
                    {"query": "${term}",
                    "fuzziness": 1}
                   }
        }
    ]}
  },
  "search_after": ["${after}"],
  "sort": [{"unique_id": "asc"}]
}''')

# Elasticsearch query template for initial query in paginated search for
# posts from the given `platform`, taking `data_size` at a time
init_platform_expr: Template = Template('''{
  "size": "${data_size}",
  "query": {
    "match": {
      "source": "${platform}"
    }
  },
  "sort": [{"unique_id": "asc"}]
}''')

# Elasticsearch query template for subsequent queries in paginated search for
# posts containing from the given `platform`, taking `data_size` at a time. 
# Taking posts starting from `search_after` unique_id
after_platform_expr: Template = Template('''{
  "size": "${data_size}",
  "query": {
    "match": {
      "source": "${platform}"
    }
  },
  "search_after": ["${after}"],
  "sort": [{"unique_id": "asc"}]
}''')



def main() -> Dict[str, Any]:
    """
    Calculates the word frequencies found in posts from a given platform, 
    returning the top 300 word frequencies. 
    
    If useterms is selected, this will be done by filtering posts by whether 
    they contain a keyword in `TERMS`, and collecting the word frequencies for 
    posts which contain each keyword.

    Handles:
    - Platform filtering with optional keyword filtering
    - Elasticsearch aggregation queries
    - Header parameter extraction

    Returns:
        JSON response containing:
            EITHER
        - List of 300 word frequencies as tuples [(word, freq), ...]
            OR
        - Dictionary of terms and word frequencies: 
            {"${TERM[0]}": [(word, freq), ...], "${TERM[1]}": [(word, freq), ...], ...}

    Raises:
        KeyError: If required date parameter is missing
        ElasticsearchException: For query failures
    """
    # Extract and validate headers
    req: Request = request
    platform: Optional[str] = req.headers.get('X-Fission-Params-Platform')
    use_terms: Optional[str] = req.headers.get('X-Fission-Params-UseTerms')

    # Initialize Elasticsearch client with type annotation
    es_client: Elasticsearch = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=(secret('USERNAME'), secret('PASSWORD'))
    )

    # If use_terms is not desired, get general word cloud for all posts on a platform
    if not use_terms:
        freqDict = defaultdict(int)

        # Build query based on platform, initial query for pagination
        query_body: Dict[str, Any] = json.loads(init_platform_expr.substitute(
            data_size = DATA_SIZE,
            platform=platform))
        amount_read, last_id = countQuery(query_body, freqDict, es_client, platform)
        if amount_read == 0:
            return "No hits"
        
        # While there are still pages left, continue taking
        while (True):
            query_body: Dict[str, Any] = json.loads(after_platform_expr.substitute(
                data_size = DATA_SIZE,
                after=last_id,
                platform=platform))
            amount_read, last_id = countQuery(query_body, freqDict, es_client, platform)
            if amount_read == 0:
                break    # Break once we run out

        return json.dumps(Counter(freqDict).most_common(300), default=str)
    
    else:
        term_dict = dict()

        # For each term in the list, build our frequencies
        for term in TERMS:
            freqDict = defaultdict(int)
            # Build query based on platform and term, initial query for pagination
            query_body: Dict[str, Any] = json.loads(init_term_expr.substitute(
                data_size = DATA_SIZE,
                platform=platform,
                term=term))
            amount_read, last_id = countQuery(query_body, freqDict, es_client, platform)
            if amount_read == 0:
                return "No hits"

            # While there are still pages left, continue taking
            while (True):
                query_body: Dict[str, Any] = json.loads(after_term_expr.substitute(
                    data_size = DATA_SIZE,
                    after=last_id,
                    platform=platform,
                    term=term))
                amount_read, last_id = countQuery(query_body, freqDict, es_client, platform)
                if amount_read == 0:
                    break      # Break once we run out
            term_dict[term] = Counter(freqDict).most_common(300)

        return json.dumps(term_dict, default=str)



def countFreq(text: str, freqDict: Dict[str, int], platform=None):
    """
    Update the `freqDict` based on the words present in `text`, stripping
    certain elements and ignoring stopwords
    """
    stop_words = set(stopwords.words('english'))
    stop_words.add('like')
    tknzr = RegexpTokenizer(r'\w+')

    # If platform is Mastodon, try to remove HTML tags
    if platform == "mastodon":
        soup = BeautifulSoup(text, "html.parser")
        text = soup.get_text()

    # Remove usernames and hyperlinks
    text = re.sub('@[^\s]+','',text)
    text = re.sub(r'https?S*|www\.\S*', "", text) # remove URLS
    
    words = tknzr.tokenize(text)

    # Remove stopwords
    for word in words:
        if not word.lower() in stop_words:
            freqDict[word.lower()] += 1

    return 




def countQuery(query_body: json, freqDict: dict, es_client: Elasticsearch, platform: str):
    """
    Executes the given `query_body`, checking how many hits were made and updating
    `freqDict` for the retrieved posts. Returns the amount of hits, and the last
    ID for pagination purposes.
    """

    # Execute search with type annotated result
    res: Dict[str, Any] = es_client.search(
        index="data-unify", # Could also read from unify-database
        body=query_body
    )

    amount_read = len(res['hits']['hits'])
    if amount_read <= 0:
        return (0,-1)
    
    last_id = res['hits']['hits'][amount_read-1]['sort'][0]
    current_app.logger.info(f'Collected {amount_read} posts')

    for hit in res['hits']['hits']:
        countFreq(hit["_source"]["text"], freqDict, platform)

    return (amount_read, last_id)


def secret(k):
    """
    Reads the elastic secret from the mounted secret path.

    Args:
    - k (str): The key name (e.g., 'CLIENT_ID', 'CLIENT_SECRET')

    Returns:
    - (str): The value read from the secret file
    """
    with open(f"/secrets/default/elastic-secret/{k}", 'r') as f:
        return f.read().strip()  # Remove any newline or trailing spaces
