from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime


app = FastAPI()

class Payload(BaseModel):
    data: dict

@app.post("/thongpham/lob")
async def thongpham_lob(payload: Payload):
    return await upload_to_s3(payload.dict(), "thongpham")

@app.post("/thongpham/lob/file")
async def thongpham_lob_file(file: UploadFile = File(...)):
    return await upload_to_s3({"filename": file.filename, "content": await file.read()}, "thongpham")

@app.post("/terrence/lob")
async def terrence_lob(payload: Payload):
    return await upload_to_s3(payload.dict(), "terrence")

async def upload_to_s3(data: dict, user: str):
    now = datetime.now()
    prefix = f"{user}/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
    key = f"{prefix}webhook-{int(now.timestamp())}.json"
    
    try:
        s3.put_object(
            Bucket=BUCKET, 
            Key=key, 
            Body=json.dumps(data).encode()
        )
        return JSONResponse({"message": "Uploaded", "key": key})
    except Exception as e:
        raise HTTPException(500, str(e))
