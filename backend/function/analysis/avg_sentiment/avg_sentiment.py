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

# Template for getting all posts on a certain date
days_expr: Template = Template('''{
    "range": {
        "created_at": {
            "gte": "${date} 00:00:00",
            "lte": "${date} 23:59:59"
        }
    }
}''')

# Template for getting all posts on a certain date from a certain platform
platform_expr: Template = Template('''{
    "bool": {
        "must": [
            {"match": {"source": "${platform}"}},
            {"range": {
                "created_at": {
                    "gte": "${date} 00:00:00",
                    "lte": "${date} 23:59:59"
                }
            }}
        ]
    }
}''')


def main() -> Dict[str, Any]:
    """Calculates daily sentiment and subjectivity averages between a given 
    date-range from the Elasticsearch database, possibly specifying the platform

    Handles:
    - Date range with optional platform filtering
    - Elasticsearch aggregation queries
    - Header parameter extraction

    Returns:
        JSON response containing:
        - A list of average sentiment and subjectivity for each date in the 
          form:
            [{'avg_subjectivity': {'value': `val1`}, 
              'avg_sentiment': {'value': `val2`}, 
              'date': `date`}]

    Raises:
        KeyError: If required date parameter is missing
        ElasticsearchException: For query failures
    """
    # Extract and validate headers
    req: Request = request
    dateFromStr: Optional[str] = req.headers.get('X-Fission-Params-DateFrom')
    dateToStr:   Optional[str] = req.headers.get('X-Fission-Params-DateTo')
    platform: Optional[str] = req.headers.get('X-Fission-Params-Platform')

    if not (dateFromStr or dateToStr):
        current_app.logger.error('Missing required date parameter')
        return {'error': 'Date parameter required'}, 400

    if platform:
        current_app.logger.debug(f'Platform: {platform}')

    dateFrom = date.fromisoformat(dateFromStr)
    dateTo   = date.fromisoformat(dateToStr)

    # Initialize Elasticsearch client with type annotation, authorising via secret
    es_client: Elasticsearch = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=(secret('USERNAME'), secret('PASSWORD'))
    )

    # The list of aggregated data to be returned
    agg_data = []

    # Loop over all the dates in the date range
    for date_i in daterange(dateFrom, dateTo):
        # Build dynamic query based on parameters
        query_body: Dict[str, Any] = {
            "query": {
                "bool": {
                    "filter": [
                        json.loads(platform_expr.substitute(date = date_i.strftime("%d-%m-%y"),
                                                            platform=platform))
                        if platform else
                        json.loads(days_expr.substitute(date=date_i.strftime("%d-%m-%y")))
                    ]
                }
            },
            "aggs": {
                "avg_sentiment": {"avg": {"field": "sentiment.compound"}},
                "avg_subjectivity": {"avg": {"field": "sentiment.subjectivity"}}
            }
        }

        #current_app.logger.debug(query_body)

        current_app.logger.info(
            f'Executing sentiment aggregation for '
            f'Date: {date_i}' +
            (f'Platform: {platform}' if platform else '')
        )

        # Execute search with type annotated result
        res: Dict[str, Any] = es_client.search(
            index="data-unify", 
            body=query_body
        )

        #current_app.logger.debug(res)

        # Append results for this date to final result array, storing this date
        agg_datum: Dict[str, Any] = res.get('aggregations', {})
        agg_datum["date"] = date_i
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