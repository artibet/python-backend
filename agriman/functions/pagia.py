from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

import pandas as pd
from datetime import date
from docxtpl import DocxTemplate
from agriman.database import get_engine

def get_pagia(application_id):

  engine = get_engine()

  # Remove this temporary template
  doc = Document()
  doc.add_heading('ΥΠΟ ΚΑΤΑΣΚΕΥΗ', level=0)
  doc.add_paragraph('Το πρότυπο της πάγιας εντολής θα είναι διαθέσιμο σύντομα...')
  buffer = BytesIO()
  doc.save(buffer)
  buffer.seek(0)

  # Continue here.....



  # Return the parsed pagia.docx (see symvasi.py)
  # return as a downloadable file
  return StreamingResponse(
    buffer,
    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    headers={
       "Content-Disposition": f"attachment; filename=pagia_{application_id}.docx"
    }
  )