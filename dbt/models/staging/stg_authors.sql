SELECT
    id   AS author_id,
    name
FROM {{ source('arxiv', 'authors') }}