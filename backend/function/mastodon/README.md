# Mastodon Harvester

This is a function to harvest posts from Mastodon using its streaming and timeline APIs. It implements features such as incremental harvesting, persistent tracking of harvested posts, retrieveing important features, calculationg sentiment score, standardise data to get features to be combined from Mastodon and Bluesky data, and store harvested reddit to ElasticSearch.

## Features

- `mastodon_harvester.py`: This script uses the Mastodon API to retrieve public posts. It uses reverse pagination with `max_id` to collect older posts incrementally. Redis is used for persistent storage of the last harvested post ID, for enabling scheduled harvest continuity and avoiding duplicates.
- It collects and processes data such as: `id`, `created_at`, `account`, `content`, `language`, `num_favourites`, `num_reblogs`, `num_replies` `sentiment`, `source`, and send these data to `data-unify-2`. Sentiment score is calculated using `Vader` - the Natural Language Tool Kit (NLTK) from Python, and `TextBlob` for sentiment subjectivity.
- Supports regional filtering, such as focusing on Australian posts, by using `.au` domains.
- Data is saved to `.jsonl` file format for further ingestion into Elasticsearch or analysis pipelines.

## How to Run

1. **Environment Setup**:
   - Ensure `mastodon-config` and `mastodon-secret` files are available at `/configs/default/mastodon-config/` and `/configs/default/mastodon-secret/`.
   - These store the `ACCESS_TOKEN`.

2. **Using with Fission**:
   - If deploying with Fission, register the function and schedule using:
     ```bash
     fission fn create --name mastodonharvester --env python --code mastodon_harvester.py
     
     fission timer create --name mastodon-timer --function mastodon-harvester --cron "0 * * * *" 
     ```

3. **Run the Harvester**:
   ```
   fission spec apply --specdir specs --wait
   fission fn update --name mastodon-harvester --ft 1500
   fission function test --name mastodon-harvester --timeout=25m0s
   ```

4. **Log Output**:
   ```
   fission function log --name mastodonharvester
   ```

## Dependencies

- `Mastodon.py` (official Mastodon Python wrapper)
- `VaderSentiment`
- `TextBlob`
- `Flask`
- `Redis`
- `Requests`
- `TQDM`