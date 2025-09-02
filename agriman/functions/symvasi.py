from docx import Document
from io import BytesIO
from fastapi.responses import StreamingResponse

def get_symvasi(application_id):
  
  # Create a sample document
  doc = Document()
  doc.add_heading("Symvasi Document", level=1)
  doc.add_paragraph(f"application_id = {application_id}")
  doc.add_paragraph("This document was generated dynamically!")

  # Save in memory
  buffer = BytesIO()
  doc.save(buffer)
  buffer.seek(0)

  # return as a downloadable file
  return StreamingResponse(
    buffer,
    media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    headers={
       "Content-Disposition": f"attachment; filename=symvasi_{application_id}.docx"
    }
  )
