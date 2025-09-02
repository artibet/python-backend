from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

def get_stats_users(period_id):
  
  # Create a sample stats array of dictionaries 
  stats = [
    {
      'book_number': 100,
      'afm_count': 131,
      'proxeires': 130,
      'oristikes': 1,
      'parcel_count': 2730,
      'cattles': 716,
      'animals_total': 1466
    },
    {
      'book_number': 1014,
      'afm_count': 42,
      'proxeires': 42,
      'oristikes': 0,
      'parcel_count': 579,
      'cattles': 0,
      'animals_total': 100
    },
    {
      'book_number': 104,
      'afm_count': 312,
      'proxeires': 308,
      'oristikes': 4,
      'parcel_count': 6254,
      'cattles': 2499,
      'animals_total': 33212
    },
    
  ]

  # Return stats as json 
  return stats
