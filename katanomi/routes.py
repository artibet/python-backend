from fastapi import APIRouter, Request
from katanomi.lib.documents.protogenes import get_protogenes
from katanomi.lib.documents.tekmiriomeno import get_tekmiriomeno
from katanomi.lib.documents.prosklisi import get_prosklisi
from katanomi.lib.documents.anathesi import get_anathesi
from katanomi.lib.documents.symvasi import get_symvasi
from katanomi.lib.stats.aitimata.aitimata_cpv_stats import get_aitimata_cpv_stats

# Create the router
router = APIRouter (
  prefix='/katanomi',
  tags=['katanomi']
)

# -----------------------------------------------------------------------------
# /aitima-documents
# -----------------------------------------------------------------------------
@router.post('/aitima-document')
async def aitimaDocument(request: Request):
  
  # get aitima_id and tag from request
  body = await request.json()
  aitima_id = body.get('aitima_id')
  tag = body.get('tag')

  # Call tag-based function to create and return the document
  if tag == 'protogenes':
    return get_protogenes(aitima_id)
  elif tag == 'tekmiriomeno':
    return get_tekmiriomeno(aitima_id)
  elif tag == 'prosklisi':
    return get_prosklisi(aitima_id)
  elif tag == 'anathesi':
    return get_anathesi(aitima_id)
  elif tag == 'symvasi':
    return get_symvasi(aitima_id)
  else:
    return 'Not implemented'
  
# -----------------------------------------------------------------------------
# Cpv stats
# -----------------------------------------------------------------------------
@router.post('/stats/aitimata/cpvs')
async def stats_cpvs(request: Request):

  # get period_id from request
  body = await request.json()
  period_id = body.get('period_id')

  # Call the function to create and return the user stats
  return get_aitimata_cpv_stats(period_id)
  
  

