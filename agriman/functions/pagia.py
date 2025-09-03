from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

import pandas as pd
from datetime import date
from docxtpl import DocxTemplate
from agriman.database import get_engine

def get_pagia(application_id):

  engine = get_engine()

  # Continue here.....



  # Return the parsed pagia.docx (see symvasi.py)