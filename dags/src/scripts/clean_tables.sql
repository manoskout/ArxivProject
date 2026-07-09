TRUNCATE TABLE 
    staging_papers,
    papers,
    authors,
    categories,
    paper_authors,
    paper_categories,
    paper_embeddings,
    paper_enrichments,
    ingestion_runs,
    quality_checks
RESTART IDENTITY CASCADE; 

