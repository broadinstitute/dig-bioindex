import json
from typing import List

import fastapi
from pydantic import BaseModel

from ..lib import config
import requests

CONFIG = config.Config()

# create web server
router = fastapi.APIRouter()

ENRICHR_BASE_URL = 'https://maayanlab.cloud/Enrichr'


class EnrichrRequest(BaseModel):
    gene_set_library: str = "KEGG_2015"
    gene_list: List[str]
    gene_list_desc: str

@router.post('/enrichr')
async def api_enrichr(req: EnrichrRequest):
    gene_str = "\n".join(req.gene_list)
    add_list_json = {
        "list": gene_str,
        "description": req.gene_list_desc
    }
    response = requests.post(f"{ENRICHR_BASE_URL}/addList", files=add_list_json)
    if not response.ok:
        raise Exception('Error analyzing gene list')

    user_list_id = json.loads(response.text)['userListId']

    response = requests.get(f"{ENRICHR_BASE_URL}/enrich?userListId={user_list_id}&backgroundType={req.gene_set_library}")
    if not response.ok:
        raise Exception('Error fetching enrichment results')

    keys = [
        "Rank",
        "Term name",
        "P-value",
        "Odds ratio",
        "Combined score",
        "Overlapping genes",
        "Adjusted p-value",
        "Old p-value",
        "Old adjusted p-value"
    ]
    enrichr_data = json.loads(response.text)[req.gene_set_library]
    return [{key: value for key, value in zip(keys, row)} for row in enrichr_data]




