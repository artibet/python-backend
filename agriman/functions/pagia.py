from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

import pandas as pd
from datetime import date
from docxtpl import DocxTemplate
from agriman.database import get_engine

def get_pagia(customer_id, period_id):

  engine = get_engine()

  query = f"""
  SELECT
    applications.afm,
    applications.firstname,
    applications.lastname,
    applications.fathername,
    applications.idno,
    applications.phone,
    applications.mobil,
    applications.year,
    applications.iban,
    banks.code AS bankscode
  FROM applications
  JOIN customers ON customers.afm = applications.afm
  JOIN banks ON banks.id = applications.bank_id
  WHERE applications.period_id = {period_id} AND
        customers.id = {customer_id}
  """
  df=pd.read_sql(query, con=engine)
  
##  if 
  doc = DocxTemplate("./agriman/templates/pagiaTP.docx")
  context ={
    'firstname' : df.loc[0,'firstname'],
    'lastname' : df.loc[0,'lastname'],
    'fathername' : df.loc[0,'fathername'],
    'afm' : df.loc[0,'afm'],
    'idno' : df.loc[0,'idno'],
    'phone' : df.loc[0,'phone'],
    'mobil' : df.loc[0,'mobil'],
    'iban' : df.loc[0,'iban'],
    'bankscode' : df.loc[0,'bankscode'],
    'year' : df.loc[0,'year'],
    'date' : date.today().strftime("%d/%m/%Y")
  }
  doc.render(context)







  # Remove this temporary template
##  doc = Document()
##  doc.add_heading('ΥΠΟ ΚΑΤΑΣΚΕΥΗ', level=0)
##  doc.add_paragraph('Το πρότυπο της πάγιας εντολής θα είναι διαθέσιμο σύντομα...')
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
       "Content-Disposition": f"attachment; filename=pagia_{customer_id}.docx"
    }
  )
