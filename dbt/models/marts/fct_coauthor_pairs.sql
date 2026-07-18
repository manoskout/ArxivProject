SELECT
    a1.name as author_1,
    a2.name as author_2,
    COUNT(*) as n_collabs
from {{ ref('stg_paper_authors') }} pa1
join {{ ref('stg_paper_authors') }} pa2 on pa1.paper_id = pa2.paper_id and pa1.author_id < pa2.author_id
join {{ ref('stg_authors') }} a1 on a1.author_id = pa1.author_id
join {{ ref('stg_authors') }} a2 on a2.author_id = pa2.author_id
group by 1, 2
