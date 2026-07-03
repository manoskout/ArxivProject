import urllib.parse
import requests
import time  # Added for rate limiting
from lxml import etree
def data_fetcher():
    '''Fetches papers from arXiv API based on specified categories and returns a list of paper details.
    Each paper detail is a tuple containing:
    (arxiv_id, title, authors, abstract, primary_category, all_categories, group_name)
    '''
    base_url = "http://export.arxiv.org/api/query?{parameters}"
    query_groups = {
        "nlp_and_core_ai": {"categories": ["cs.CL", "cs.AI"]},
        "ml_and_foundations": {"categories": ["cs.LG", "stat.ML"]},
        "multimodal_and_vision": {"categories": ["cs.CV"]},
        "safety_and_agents": {"categories": ["cs.CR", "cs.RO"]}
    }

    all_papers = []

    for group_name, group_data in query_groups.items():
        print(f"Fetching daily update for: {group_name}")
        cat_string = " OR ".join([f"cat:{cat}" for cat in group_data["categories"]])
        
        parameters = urllib.parse.urlencode({
            "search_query": f"({cat_string}) AND all:LLM",
            "max_results": 100,  # Top 100 newest is plenty for a single day's volume
            "sortBy": "submittedDate",
            "sortOrder": "descending"
        })
        
        response = requests.get(base_url.format(parameters=parameters))
        root = etree.fromstring(response.content)
        namespaces = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}

        entries = root.findall('atom:entry', namespaces)
        for entry in entries:
            arxiv_id = entry.find('atom:id', namespaces).text.split('/')[-1]
            title = entry.find('atom:title', namespaces).text.strip()
            abstract = entry.find('atom:summary', namespaces).text.strip()
            # Inside your extraction loop:
            published_at = entry.find('atom:published', namespaces).text
            updated_at = entry.find('atom:updated', namespaces).text

            # Get the PDF URL
            pdf_url = None
            for link in entry.findall('atom:link', namespaces):
                if link.get('title') == 'pdf':
                    pdf_url = link.get('href')
            
            # authors list into a comma-separated string for easier database insertion
            authors = ", ".join([author.text for author in entry.findall('atom:author/atom:name', namespaces)])
            
            cat_node = entry.find('arxiv:primary_category', namespaces)
            primary_cat = cat_node.get('term') if cat_node is not None else None
            
            all_categories = ", ".join([node.get('term') for node in entry.findall('atom:category', namespaces)])
            
            all_papers.append({
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "primary_cat": primary_cat,
                "all_categories": all_categories,
                "group_name": group_name,
                "link": pdf_url,
                "published_at": published_at,
                "updated_at": updated_at
            })
        
        time.sleep(3) # arXiv rate limits

    print(f"Daily pull complete. Extracted {len(all_papers)} total papers.")
    return all_papers