SELECT
    paper_id,
    category_id,
    is_primary
FROM {{ source('arxiv' , 'paper_categories') }}