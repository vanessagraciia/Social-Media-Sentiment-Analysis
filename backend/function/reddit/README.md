# Reddit Harvester

The file contents of this repository are all about functionalities to harvest Reddit data, process reddit data such as retrieving important features, calculating sentiment score, standardise data to get features to be combined from Mastodon and Bluesky data, and store harvested reddit to ElasticSearch. 

## Features
- `reddit_harvester.py` is to work with fission function `reddit-harvester-2`, that is to get credentials from secret file, harvest 73 Australian cities to become subreddits indicator, request JSON file and receive JSON data format of Reddit data. This function works in parallel using threading processing such that all subreddits do not have to wait until previous subreddit is finished. 
- `reddit_processor.py` is python function to work with fission function `reddit-processor-2`. This function is to calculate sentiment scores, select important features such as: `id`, `title`, `selftext`, `subreddit`, `created_at`, `author`, `url`, `num_comments`, `num_upvotes`, `upvote_ratio`, `sentiment`, `source`, and send these data to `data-unify-2`. Sentiment score is calculated using `Vader` - the Natural Language Tool Kit (NLTK) from Python, and `TextBlob` for sentiment subjectivity.
- `data_unify.py` is the base function for `data-unify-2` fission function. This is our strategy to handle extension such that it can work with other social media platform. This function will receive data from Reddit, Mastodon, and Bluesky, variable names and format will be standardised, and directly stored to `ElasticSearch` database name `data-unify`.

## How to Run

To run the functionalities, `reddit-harvester-2` is sufficient to trigger any functions as it uses HTTP `route.fission` trigger. Therefore, to run `reddit-harvester-2`, simply run these snippet codes to terminal.
```{terminal}
chmod +x reddit-new.sh
./reddit-new.sh
```
The shell script will redeploy all packages, all functions, timer cron jobs, apply specs files, and finally test the function.

## Dependencies
- Vader
- TextBlob
- flask
- ElasticSearch