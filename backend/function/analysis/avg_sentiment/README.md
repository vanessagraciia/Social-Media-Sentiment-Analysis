# Function: avg-sentiment

Calculates daily sentiment and subjectivity averages between a given date-range from the Elasticsearch database, possibly specifying the platform

Handles:
- Date range with optional platform filtering
- Elasticsearch aggregation queries
- Header parameter extraction

Returns a JSON response containing:

- A list of average sentiment and subjectivity for each date in the form:
    
    `[{'avg_subjectivity': {'value': val1}, 
          'avg_sentiment': {'value': val2}, 
          'date': date}, ...]`

Raises:
- KeyError: If required date parameter is missing
- ElasticsearchException: For query failures


## ReST API:

Set up port forward for fission:
`kubectl port-forward service/router -n fission 9090:80`

To take from all platforms:
`curl "http://localhost:9090/sentiment/days/{dateFrom: yyyy-mm-dd}/{dateto: yyyy-mm-dd}"`

To take from a specific platform:
`curl "http://localhost:9090/sentiment/days/{dateFrom: yyyy-mm-dd}/{dateTo: yyyy-mm-dd}/platform/{platform: name}"`





## Commands to create the specs:

`
fission package create --spec --name avg-sentiment \
    --source ./avg_sentiment/__init__.py \
    --source ./avg_sentiment/avg_sentiment.py \
    --source ./avg_sentiment/requirements.txt \    
    --source ./avg_sentiment/build.sh \
    --env python \
    --buildcmd './build.sh'
`

`
fission fn create --spec --name avg-sentiment \
    --pkg avg-sentiment \
    --env python \
    --entrypoint "avg_sentiment.main" \
    --secret "elastic-secret"
`

`
fission route create --spec --name avgsentday --function avg-sentiment \
    --method GET \
    --url '/sentiment/days/{dateFrom:[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]}/{dateTo:[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]}'
`

`
fission route create --spec --name avgsentdayplatform --function avg-sentiment \
    --method GET \
    --url '/sentiment/days/{dateFrom:[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]}/{dateTo:[0-9][0-9][0-9][0-9]-[0-1][0-9]-[0-3][0-9]}/platform/{platform:[a-zA-Z0-9]+}'
`

## To then apply the spec to the cluster:
`
fission spec apply --specdir specs --wait
`