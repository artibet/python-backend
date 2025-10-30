from fastapi import APIRouter, Request
from katanomi.lib.documents.protogenes import get_protogenes
from katanomi.lib.documents.tekmiriomeno import get_tekmiriomeno
from katanomi.lib.documents.prosklisi import get_prosklisi
from katanomi.lib.documents.anathesi import get_anathesi
from katanomi.lib.documents.symvasi import get_symvasi

# Create the router
router = APIRouter (
  prefix='/katanomi',
  tags=['katanomi']
)

# -----------------------------------------------------------------------------
# symvasi.docx
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
  
  

