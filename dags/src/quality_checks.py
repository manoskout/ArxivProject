from airflow.providers.amazon.aws.hooks.s3 import S3Hook

QUALITY_CHECKS = {
    "duplicate_papers": 
        "SELECT count(*) - count(DISTINCT id) FROM papers",
    "no_null_arxiv_ids":
        "SELECT count(*) FROM papers WHERE arxiv_id IS NULL",
    "no_orphaned_paper_authors":
        """SELECT count(*) FROM paper_authors pa
           LEFT JOIN papers p ON p.id = pa.paper_id WHERE p.id IS NULL""",
    "no_duplicate_embeddings":
        """SELECT count(*) - count(DISTINCT embedding::text)
           FROM paper_embeddings""",
    "embeddings_not_exceed_papers":
        """SELECT GREATEST((SELECT count(*) FROM paper_embeddings)
           - (SELECT count(*) FROM papers), 0)""",
    "enrichment_json_parseable":
        """SELECT count(*) FROM paper_enrichments
           WHERE methods IS NULL AND datasets IS NULL AND summary IS NULL""",
}

def verify_minio_connection():
    s3_hook = S3Hook(aws_conn_id='minio')
        
    client = s3_hook.get_conn()
    response = client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    
    print(f"Successfully connected! Current buckets in MinIO: {buckets}")

