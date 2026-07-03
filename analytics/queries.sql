-- 1. Trending topics per week (window functions + JSONB)
SELECT
    date_trunc('week', p.published_at) AS week,
    topic,
    COUNT(*) AS n_papers,
    RANK() OVER (PARTITION BY date_trunc('week', p.published_at)
                 ORDER BY COUNT(*) DESC) AS topic_rank
FROM papers p
JOIN paper_enrichments e ON e.paper_id = p.id,
     jsonb_array_elements_text(e.custom_topics) AS topic
GROUP BY 1, 2;

-- 2. Most frequent co-author pairs (self-join)
SELECT a1.name, a2.name, COUNT(*) AS n_collabs
FROM paper_authors pa1
JOIN paper_authors pa2
  ON pa1.paper_id = pa2.paper_id AND pa1.author_id < pa2.author_id
JOIN authors a1 ON a1.id = pa1.author_id
JOIN authors a2 ON a2.id = pa2.author_id
GROUP BY 1, 2
ORDER BY n_collabs DESC
LIMIT 20;
