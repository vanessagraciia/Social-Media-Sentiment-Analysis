# Function word-frequency

Calculates the word frequencies found in posts from a given platform, returning the top 300 word frequencies. 
    
If useterms is selected, this will be done by filtering posts by whether they contain a keyword in `TERMS`, and collecting the word frequencies for posts which contain each keyword.

Handles:
- Platform filtering with optional keyword filtering
- Elasticsearch aggregation queries
- Header parameter extraction

Returns JSON response containing:
- EITHER: List of 300 word frequencies as tuples `[(word, freq), ...]`
- OR: Dictionary of terms and word frequencies: 
        `{"${TERM[0]}": [(word, freq), ...], "${TERM[1]}": [(word, freq), ...], ...}`

Raises:
- KeyError: If required date parameter is missing
- ElasticsearchException: For query failures



## ReST API:

Set up port forward for fission:
`kubectl port-forward service/router -n fission 9090:80`

To take from all platforms:
`curl "http://localhost:9090/wordfreq/platform/{platform: platform}"`





## Commands to create specs

`fission package create --spec --name word-frequency \
    --env python \
    --source ./word_frequency/__init__.py \
    --source ./word_frequency/word_frequency.py \
    --source ./word_frequency/requirements.txt \
    --source ./word_frequency/build.sh \
    --buildcmd './build.sh'
`

`
fission fn create --spec --name word-frequency \
    --pkg word-frequency \
    --env python \
    --entrypoint "word_frequency.main" \
    --secret "elastic-secret"
`

`
fission route create --spec --name wordfreqplatform --function word-frequency \
    --method GET \
    --url '/wordfreq/platform/{platform:[a-zA-Z0-9]+}'
`

`
fission route create --spec --name wordfreqplatformterms --function word-frequency \
    --method GET \
    --url '/wordfreq/platform/{platform:[a-zA-Z0-9]+}/useterms/{useterms:t|f}'
`

## Commands to apply spec to cluster
`
fission spec apply --specdir specs --wait
`