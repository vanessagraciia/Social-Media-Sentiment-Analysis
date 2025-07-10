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
from datetime import date, timedelta


# Keywords to query posts
TERMS = ["gaza", "israel", "ukraine", "russia"]

# Elasticsearch query template to find the average sentiment and subjectivity
# across all posts which contain a given keyword and are from a given platform.
# Allows fuzziness of 1, so minor typos still match.
term_expr: Template = Template('''
    {"query": {
        "bool": {
            "filter": [ {
                "bool": {
                    "must": [ 
                    {"match": {"text": 
                               {"query": "${term}",
                                "fuzziness": 1}
                               }
                    },
                    {"match": {"source": "${platform}"}}
                    ]
                }
            } ]
        }
    },
    "aggs": {
      "avg_sentiment": {"avg": {"field": "sentiment.compound"}},
      "avg_subjectivity": {"avg": {"field": "sentiment.subjectivity"}}
    }
  }''')

    

def main() -> Dict[str, Any]:
    """
    For each keyword in the list `TERMS`, calculates the average sentiment and 
    subjectivity across all posts containing this keyword from a certain
    platform in the Elasticsearch database.

    Handles:
    - Platform filtering, collecting for each term in `TERMS`
    - Elasticsearch aggregation queries
    - Header parameter extraction

    Returns:
        JSON response containing:
        - A list of average sentiment and subjectivity for each keyword in the 
          form:
            [{'avg_subjectivity': {'value': `val1`}, 
              'avg_sentiment': {'value': `val2`}, 
              'term': `term`}]

    Raises:
        KeyError: If required date parameter is missing
        ElasticsearchException: For query failures
    """
    # Extract headers
    req: Request = request
    platform: Optional[str] = req.headers.get('X-Fission-Params-Platform')

    if platform:
        current_app.logger.debug(f'Platform: {platform}')

    # Initialize Elasticsearch client, using secret for credentials
    es_client: Elasticsearch = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=(secret('USERNAME'), secret('PASSWORD'))
    )

    # Aggregate data for each term to be returned
    agg_data = []

    # For each term in the list, calculate the aggregates
    for term in TERMS:
        # Substitute in current term and platform
        query_body: Dict[str, Any] = (
            json.loads(term_expr.substitute(term = term, platform=platform)))

        #current_app.logger.debug(query_body)

        current_app.logger.info(
            f'Executing sentiment aggregation for '
            f'Term: {term}' +
            (f'Platform: {platform}' if platform else '')
        )

        # Execute search with type annotated result
        res: Dict[str, Any] = es_client.search(
            index="data-unify", 
            body=query_body
        )

        #current_app.logger.debug(res)

        # Extract and format results
        agg_datum: Dict[str, Any] = res.get('aggregations', {})
        agg_datum["term"] = term
        agg_data.append(agg_datum)

    return json.dumps(agg_data, default=str)
        


def daterange(start_date: date, end_date: date):
    """
    Returns an iterable daterange object from start_date to end_date
    """
    days = int((end_date - start_date).days)
    for n in range(days):
        yield start_date + timedelta(n)


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