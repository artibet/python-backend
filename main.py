from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from agriman.routes import router as agriman_router

# Create and initialize fastapi server
app = FastAPI()
origins = ["*"]
app.add_middleware(
  CORSMiddleware,
  allow_origins=origins,
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"]
)

# load the routers
app.include_router(agriman_router)