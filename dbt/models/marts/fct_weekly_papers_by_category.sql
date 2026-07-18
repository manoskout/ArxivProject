select
    date_trunc('week', p.published_at)::date as week,
    c.code as category,
    COUNT(*) as n_papers
FROM {{ ref('stg_papers') }} p
JOIN {{ ref('stg_paper_categories') }} pc ON pc.paper_id = p.paper_id
JOIN {{ ref('stg_categories') }} c ON c.category_id = pc.category_id
-- where pc.is_primary = true
group by 1, 2
order by 1, 2