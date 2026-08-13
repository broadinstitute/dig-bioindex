from typing import List

import fastapi
import requests
from pydantic import BaseModel

# create web server
router = fastapi.APIRouter()

ENRICHR_BASE_URL = 'https://maayanlab.cloud/Enrichr'

# upstream Enrichr calls occupy a thread-pool slot; bound how long
UPSTREAM_TIMEOUT_S = 30

# positional column names of an Enrichr enrichment result row
RESULT_KEYS = [
    'Rank',
    'Term name',
    'P-value',
    'Odds ratio',
    'Combined score',
    'Overlapping genes',
    'Adjusted p-value',
    'Old p-value',
    'Old adjusted p-value',
]


class EnrichrRequest(BaseModel):
    gene_set_library: str = 'KEGG_2015'
    gene_list: List[str]
    gene_list_desc: str


@router.post('/enrichr')
def api_enrichr(req: EnrichrRequest):
    """
    Proxy a gene list to the public Enrichr service and return the
    enrichment results for the requested gene-set library.
    """
    add_list = {
        'list': '\n'.join(req.gene_list),
        'description': req.gene_list_desc,
    }
    resp = requests.post(f'{ENRICHR_BASE_URL}/addList', files=add_list, timeout=UPSTREAM_TIMEOUT_S)
    if not resp.ok:
        raise fastapi.HTTPException(status_code=502, detail='Enrichr failed to analyze the gene list')

    user_list_id = resp.json()['userListId']

    resp = requests.get(
        f'{ENRICHR_BASE_URL}/enrich?userListId={user_list_id}&backgroundType={req.gene_set_library}',
        timeout=UPSTREAM_TIMEOUT_S,
    )
    if not resp.ok:
        raise fastapi.HTTPException(status_code=502, detail='Enrichr failed to fetch enrichment results')

    rows = resp.json()[req.gene_set_library]
    return [dict(zip(RESULT_KEYS, row)) for row in rows]
