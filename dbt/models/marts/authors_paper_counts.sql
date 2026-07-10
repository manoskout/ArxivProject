SELECT a.name, COUNT(*) AS n_papers
FROM {{ref('stg_authors')}} a
JOIN {{ref('stg_paper_authors')}} pa ON pa.author_id = a.author_id
GROUP BY a.name
ORDER BY n_papers DESC

