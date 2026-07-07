# arXiv Data Ingestion Pipeline

An automated Extract, Load, and Transform (ELT) pipeline that fetches daily medical imaging and deep learning research papers from the arXiv API, archives the raw JSON in MinIO, and normalizes the data into a PostgreSQL relational schema.

## Architecture Overview

This project is built using an ELT architecture, prioritizing data preservation.

* **Data Source:** [arXiv API](https://info.arxiv.org/help/api/index.html)
* **Orchestration:** Apache Airflow 
* **Data Lake (Bronze Layer):** MinIO (S3-compatible object storage for raw JSON, and PDF files)
* **Data Warehouse (Gold Layer):** PostgreSQL (3NF normalized relational database)

### Pipeline Flow
1. **Fetch:** Airflow queries the arXiv API for the latest papers in targeted categories.
2. **Lake Ingestion:** Raw responses are immediately saved to a dynamic `raw/arxiv/{{ds}}/papers.json` path in MinIO.
3. **Normalization:** A Python TaskFlow process pulls the raw JSON from MinIO, cleans strings using Pandas, and loads the flat data into a PostgreSQL `staging_papers` table.
4. **Distribution:** A transactional PL/pgSQL script distributes the staging data across normalized tables.
5. **Logging:** An Airflow-native task records execution metrics (run status, papers fetched, new papers inserted) into an `ingestion_runs` table.

## Database Schema

The PostgreSQL database separates entities to support efficient querying and future ML enrichments.

* **Core Entities:**
  * `papers`: Core metadata (arXiv ID, title, abstract, published dates).
  * `authors`: Unique authors.
  * `categories`: arXiv category codes (e.g., `cs.CV`, `cs.LG`).
* **Junction Tables:**
  * `paper_authors`: Links papers to authors (includes authorship `position`).
  * `paper_categories`: Links papers to categories (includes `is_primary` boolean).
* **ML Ready:**
  * `paper_embeddings`: Designed to store vector embeddings of abstracts.
  * `paper_enrichments`: Designed to store LLM-generated summaries and dataset extraction.
* **Operational:**
  * `ingestion_runs`: Logs pipeline metadata and execution statistics.

## Prerequisites

Before running this project, ensure you have the following installed on your machine:
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) or Docker Engine
* Git

## Local Setup & Installation

**1. Clone the repository**
```bash
git clone [https://github.com/yourusername/arxiv-data-pipeline.git](https://github.com/yourusername/arxiv-data-pipeline.git)
cd arxiv-data-pipeline
```

**2. Configure environment variables**: You must create a .env file in the root directory for the Docker containers to initialize properly.
```bash
cat << EOF > .env
AIRFLOW_UID=$(id -u)"
POSTGRES_USER=papers
POSTGRES_PASSWORD=papers
POSTGRES_DB=papers
POSTGRES_HOST_AUTH_METHOD=trust
EOF
```
**3. Start the infrastructure (Airflow, PostgreSQL and MinIO) using Docker Compose**
```bash
docker-compose up -d
```

**4. Access the services**
- Airflow UI: http://localhost:8080
- MinIO Console: http://localhost:9001

**5. Configure Airflow Connections**
- Connection 1: `MinIO`
    - Connection ID: minio
    - Connection Type: aws (Amazon Web Services)
    - Extra: {"endpoint_url": "http://minio:9000"}
- Connection 2: PostgreSQL
    - Connection ID: papers_db
    - Connection Type: postgres
    - Host: papers_db
    - Schema: papers
    - Login: papers
    - Password: papers
    - Port: 5432

## Future Improvements
1. Vector Search: Generate embeddings for paper abstracts and store them on `paper_embeddings` table
2. LLM: Add a downstream task to pass new papers to a LLM to extract methodologies and datasets into `paper_enrichments` table
