from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

import pandas as pd
from datetime import date
from docxtpl import DocxTemplate
from agriman.database import get_engine

def get_symvasi_farm_b(application_id):

  engine = get_engine()

  query = f"""
  SELECT
    applications.afm,
    applications.firstname,
    applications.lastname,
    applications.year
  FROM applications 
  WHERE applications.id = {application_id}
  """
  df=pd.read_sql(query, con=engine)

  doc = DocxTemplate("./agriman/templates/symvasifarmB.docx")
  context ={
    'firstname' : df.loc[0,'firstname'],
    'lastname' : df.loc[0,'lastname'],
    'afm' : df.loc[0,'afm'],
    'year' : df.loc[0,'year'],
    'date' : date.today().strftime("%d/%m/%Y")
  }
  doc.render(context)

  # Save in memory
  buffer = BytesIO()
  doc.save(buffer)
  buffer.seek(0)

  # return as a downloadable file
  return StreamingResponse(
    buffer,
    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    headers={
       "Content-Disposition": f"attachment; filename=symvasi_farmB_{application_id}.docx"
    }
  )
