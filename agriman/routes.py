from fastapi import APIRouter, Request
from agriman.lib.symvasi import get_symvasi
from agriman.lib.symvasifarmB import get_symvasi_farm_b
from agriman.lib.stats_users import get_stats_users
from agriman.lib.pagia import get_pagia
from agriman.lib.economics import get_economics
from agriman.lib.sitiresio import get_sitiresio

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

  # Call the function to create and return the symvasi.docx
  return get_symvasi(application_id)

# -----------------------------------------------------------------------------
# symvasi-farm-b.docx
# -----------------------------------------------------------------------------
@router.post('/symvasi-farm-b')
async def symvasiFarmB(request: Request):
  
  # get application_id from request
  body = await request.json()
  application_id = body.get('application_id')

  # Call the function to create and return the symvasi_farmB.docx
  return get_symvasi_farm_b(application_id)

# -----------------------------------------------------------------------------
# symvasi-farm-b.docx
# -----------------------------------------------------------------------------
@router.post('/sitiresio')
async def sitiresio(request: Request):
  
  # get application_id from request
  body = await request.json()
  application_id = body.get('application_id')

  # Call the function to create and return the sitiresio.docx
  return get_sitiresio(application_id)

# -----------------------------------------------------------------------------
# users stats
# -----------------------------------------------------------------------------
@router.post('/stats/users')
async def stats_users(request: Request):

  # get period_id from request
  body = await request.json()
  period_id = body.get('period_id')

  # Call the function to create and return the user stats
  return get_stats_users(period_id)

# -----------------------------------------------------------------------------
# economics stats
# -----------------------------------------------------------------------------
@router.post('/stats/economics')
async def stats_economics(request: Request):

  # get period_id from request
  body = await request.json()
  period_id = body.get('period_id')

  # Call the function to create and return the user stats
  return get_stats_economics(period_id)

# -----------------------------------------------------------------------------
# pagia.docx
# -----------------------------------------------------------------------------
@router.post('/pagia')
async def symvasi(request: Request):
  
  # get application_id from request
  body = await request.json()
  customer_id = body.get('customer_id')
  period_id = body.get('period_id')

  # Call the function to create and return the pagia.docx
  return get_pagia(customer_id, period_id)

# -----------------------------------------------------------------------------
# economics.docx
# -----------------------------------------------------------------------------
@router.post('/economics')
async def economics(request: Request):
  
  # get application_id from request
  body = await request.json()
  application_id = body.get('application_id')

  # Call the function to create and return the economics.docx
  return get_economics(application_id)