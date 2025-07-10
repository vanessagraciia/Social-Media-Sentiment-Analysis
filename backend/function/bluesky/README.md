# Bluesky Harvester

This is directory to harvest bluesky data, filter Australian User, process filtered data such as retrieving important features, calculating sentiment score, resimplify data to get features to be combined from Mastodon and Reddit data, and store harvested data and unified data to ElasticSearch. 

## Features
- Send job to harvest newest today data to send jobs with body message of `q`, `mode`: new, `since`: today 00:00:00.0001Z, `until`: today T23:59:59.0000Z using `bluesky-new-harvester.py`
- Send job to harvest historical data to send jobs with body message of `q`, `mode`: backday, `since`: today 00:00:00.0001Z, `until`: today T23:59:59.0000Z using `bluesky-back-harvester.py`
- Harvests posts as job request and filter post from australian likely user 
- Process harvested and filterd data to only retrieve certain features and calculate sentiment score
- Calculate sentiment score using `Vader` - the Natural Language Tool Kit (NLTK) from Python, and `TextBlob` for sentiment subjectivity.
- Unify data using standardised format in `function\reddit\data_unify.py`
- Each of python file has its own `requirements.txt` documents and `build.sh` script to be run for efficiency of process
- `CLIENT_ID` and `CLIENT_SECRET` are stored in `reddit-config.yaml` and be passed on `bluesky_harvester.py`

## How to Run
Run `reddit-run.sh`, that is all commands to create package and function of each python file

## Dependencies
- Vader
- TextBlob
- flask
- ElasticSearch
- redis