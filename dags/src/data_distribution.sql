-- Insert papers
insert into papers(
    arxiv_id, 
    title, 
    abstract, 
    published_at, 
    pdf_url, 
    updated_at,
    ingested_at
)
select DISTINCT
    arxiv_id, 
    title, 
    abstract, 
    published_at, 
    pdf_url, 
    now(), 
    now()
from staging_papers
on conflict (arxiv_id) do nothing;

-- select * from papers;

-- Insert authors, all the authors from each paper, 
-- but only distinct authors across all papers 
insert into authors(
    name 
)
select distinct
    unnest(authors) as name
from staging_papers
on conflict (name) do nothing;

select count(*) from authors;

-- Insert categories
-- TODO: name is probably useless
insert into categories(
    code
)
select distinct
    unnest(all_categories) from staging_papers
on conflict (code) do nothing;

select * from categories;

-- Link Paper authors
insert into paper_authors(paper_id, author_id, position)
select 
    p.id, 
    a.id,
    o.author,
    p.arxiv_id, 
    o.ord 
from staging_papers stage
cross join lateral unnest(stage.authors) with ordinality as o(author, ord)
join papers p on stage.arxiv_id = p.arxiv_id
join authors a on a.name = o.author
on conflict do nothing;

-- SELECT * FROM paper_authors;
select * from categories;
-- Link Paper categories
insert into paper_categories(paper_id, category_id, is_primary)
select p.id, c.id, (c.code = sp.primary_category)
from staging_papers sp
join papers p on p.arxiv_id = sp.arxiv_id
join categories c on c.code = any(sp.all_categories)
on conflict do nothing;