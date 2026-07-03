# End-to-end data pipeline for arXiv Papers ingestion

The purpose of this work to ingest arXiv papers daily, enriching them with an LLM to serve insights via an API

## Methodology 
Get paper information from Arxiv, then save to the database
### Prerequisites
Airflow
Postgres + vector


## NOTE: Minio
Since you are talking to MinIO, hence you use update on the airflow UI the connections and add on the json the endpoint URL
`
{"endpoint_url": "http://minio:9000}
`