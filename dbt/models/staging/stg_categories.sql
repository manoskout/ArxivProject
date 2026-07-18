select
    id as category_id,
    code,
    name
from {{ source('arxiv', 'categories') }}