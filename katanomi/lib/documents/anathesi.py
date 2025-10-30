from katanomi.database import get_engine
from docxtpl import DocxTemplate
from io import BytesIO
from fastapi.responses import StreamingResponse

def get_anathesi(aitima_id):
  engine = get_engine()

  # DB staff here ....

  # Context to be substituted
  context = {
    'name': 'dipae'
    # ...
  }

  # Return it
  doc = DocxTemplate("./katanomi/templates/anathesi.docx")
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
       "Content-Disposition": f"attachment; filename=anathesi_{aitima_id}.docx"
    }
  )