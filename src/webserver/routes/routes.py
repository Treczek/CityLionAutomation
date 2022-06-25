from fastapi import APIRouter
from fastapi.responses import JSONResponse

root = APIRouter()


@root.get("/health")
async def health_check():
    return JSONResponse({"Healthy": True})
