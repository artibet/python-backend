from fastapi import APIRouter, Request
from agriman.functions.symvasi import symvasi

# Create the router
router = APIRouter (
  prefix='/agriman',
  tags=['agriman']
)

# -----------------------------------------------------------------------------
# symvasi.docx
# -----------------------------------------------------------------------------
@router.post('/symvasi')
async def symvasi(request: Request):
  
  # get application_id from request
  body = await request.json()
  application_id = body.get('application_id')

  # Call the function to create and return the symvasi .docx
  return symvasi(application_id)
