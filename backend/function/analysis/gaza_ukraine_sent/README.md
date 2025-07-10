# Function gaza-ukraine

For each keyword in the list ["Gaza", "Israel", "Ukraine", "Russia"], calculates the average sentiment and subjectivity across all posts containing this keyword from a certain platform in the Elasticsearch database.

Handles:
- Date range with optional platform filtering
- Elasticsearch aggregation queries
- Header parameter extraction

Returns a JSON response containing:
- A list of average sentiment and subjectivity for each keyword in the 
form:

    `[{'avg_subjectivity': {'value': val1}, 
    'avg_sentiment': {'value': val2}, 
    'term': term}, ...]`


Raises:
- KeyError: If required date parameter is missing
- ElasticsearchException: For query failures


# ReST API:

Set up port forward for fission:
`kubectl port-forward service/router -n fission 9090:80`

To take from a specific platform:
`curl "http://localhost:9090/gaza-ukraine/platform/{platform:[a-zA-Z0-9]+}"`





# COMMANDS USED TO APPLY TO CREATE THE SPECS:

`
fission package create --spec --name gaza-ukraine-sent \
    --source ./gaza_ukraine_sent/__init__.py \
    --source ./gaza_ukraine_sent/gaza_ukraine_sent.py \
    --source ./gaza_ukraine_sent/requirements.txt \
    --source ./gaza_ukraine_sent/build.sh \
    --env python \
    --buildcmd './build.sh'
`

`
fission fn create --spec --name gaza-ukraine-sent \
    --pkg gaza-ukraine-sent \
    --env python \
    --entrypoint "gaza_ukraine_sent.main" \
    --secret "elastic-secret"
`

`
fission route create --spec --name gazaukraine --function gaza-ukraine-sent \
    --method GET \
    --url '/gaza-ukraine/platform/{platform:[a-zA-Z0-9]+}'
`

`
fission spec apply --specdir specs --wait
`






GET _search 
  {"query": {
        "bool": {
            "filter": [ {
                "bool": {
                    "must": [ 
                    {"match": {"text": 
                               {"query": "gaza",
                                "fuzziness": 1}
                               }
                    },
                    {"match": {"source": "reddit"}}
                    ]
                }
            } ]
        }
    },
    "aggs": {
      "avg_sentiment": {"avg": {"field": "sentiment.compound"}},
      "avg_subjectivity": {"avg": {"field": "sentiment.subjectivity"}}
    }
  }