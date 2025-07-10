#!/bin/sh

#configuration
MAPPING_FILE="mappings/data-unify.json"
INDEX_NAME="data-unify"
NAMESPACE="elastic"
SERVICE="elasticsearch-master"

#port-forward Elasticsearch
echo "Starting Elasticsearch port-forward..."
kubectl port-forward service/${SERVICE} -n ${NAMESPACE} 9200:9200 >/dev/null 2>&1 &

#get password from Kubernetes ConfigMap
echo "Retrieving Elasticsearch password from Kubernetes ConfigMap..."
export ES_PASSWORD=$(kubectl get configmap es-credentials -n ${NAMESPACE} -o 'jsonpath={.data.password}')

#create Elasticsearch index
echo "Creating index '${INDEX_NAME}' using mapping at '${MAPPING_FILE}'..."
curl -XPUT -k "https://127.0.0.1:9200/${INDEX_NAME}" \
  --header "Content-Type: application/json" \
  --data @"${MAPPING_FILE}" \
  --user "elastic:${ES_PASSWORD}" | jq '.'
