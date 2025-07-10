# Database Index Creation

This folder contains the mapping for data-unify index in ElasticSearch and the shell script to create the index.

## Prerequisites
Use the `port-forward` command of `kubectl` in a new terminal window. 
  ```bash
  kubectl port-forward service/elasticsearch-master -n elastic 9200:9200
  ```
Use the `port-forward` command of `kubectl` in another terminal window. 
  ```bash
  kubectl port-forward service/kibana-kibana -n elastic 5601:5601
  ```

1. Elasticsearch (Kibana user interface) running by pointing the browser to `https://127.0.0.1:5601/`  
2. An elastic user password stored in a Kubernetes ConfigMap:
  ```bash
  export ES_PASSWORD=$(
    kubectl get configmap es-credentials \
    -n elastic \ 
    -o 'jsonpath={.data.password}).
  ```

## Indices & Mappings

### data-unify
Index name: `data-unify`

Mapping file: `mappings/data-unify.json`

Create the index with:
```bash
chmod +x create_index.sh
./create_index.sh
  ```
This script will:
- Start port-forwarding Elasticsearch
- Retrieve the password from Kubernetes 
- Create the data-unify index using the mapping

This index has:
- **3** primary shards, **1** replica  
- `unique_id`: keyword  
- `created_at`: date (with format `dd-MM-yy HH:mm:ss`)  
- `text`: full-text field  
- `sentiment`: object with floats (negative, neutral, positive, compound, subjectivity)  
- `source`: keyword  

## Queries
Queries folder contains all the queries used to retrieve data from main database, ElasticSearch. Those data retrieved is used for further analysis and visualisations for frontend.

Queries: `wordfreq_term_after.json`, `wordfreq_term_initial.json`, `wordfrequency_after.json`, `wordfrequency_initial.json` are all used in the function `word-frequency` found in `analysis/word_frequency`

Query: `gazaukraine_sent.json` is used in the function `gaza-ukraine` which is found in `analysis/gaza_ukraine_sent`

Queries: `avgsentiment_days.json`, `avgsentiment_platform.json` are used in the function `avg-sentiment` found in `analysis/avg_sentiment`