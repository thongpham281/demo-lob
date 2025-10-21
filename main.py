import os
import json
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import boto3

# --- Configuration ---
S3_BUCKET_NAME = "thongpham-lob-demo"

# We assume the FastAPI app is running on an EC2 instance 
# with an IAM Role that has S3 write permissions to S3_BUCKET_NAME.
# boto3 will automatically pick up these credentials.
s3_client = boto3.client('s3')

app = FastAPI(title="S3 LOB Upload Service")

# --- Pydantic Data Model ---
# Define the structure of the incoming JSON payload (LOB = Line of Business data)
class LOBPayload(BaseModel):
    """
    Generic model for the Line of Business payload. 
    It expects a dictionary that can contain any valid JSON data.
    """
    data: dict

# --- Core Logic Function ---
def upload_to_s3(user_name: str, payload: dict):
    """
    Constructs the S3 key (path) and uploads the JSON payload.
    """
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    timestamp = now.strftime("%Y%m%d%H%M%S%f")
    
    # Prefix structure: {user}/year={Y}/month={M}/day={D}/
    s3_prefix = f"{user_name}/year={year}/month={month}/day={day}/"
    
    # Filename: {user}-{timestamp}.json
    file_name = f"{user_name}-{timestamp}.json"
    s3_key = s3_prefix + file_name
    
    try:
        # Convert the Pydantic model dict to a JSON string
        json_data = json.dumps(payload, ensure_ascii=False, indent=2)
        
        # Upload the JSON string to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json_data.encode('utf-8'), # Encode for safe transport
            ContentType='application/json'
        )
        
        print(f"Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
        
        return {
            "status": "success",
            "s3_path": f"s3://{S3_BUCKET_NAME}/{s3_key}",
            "message": "Payload uploaded successfully."
        }
        
    except Exception as e:
        print(f"S3 Upload Error: {e}")
        # Raise an HTTPException for the FastAPI caller
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")


# --- FastAPI Endpoints ---

@app.post("/thongpham/lob")
def thongpham_lob_upload(payload: LOBPayload):
    """
    Receives payload for Thong Pham's LOB and uploads it to S3.
    Prefix: thongpham/year=/month=/day=/
    """
    user = "thongpham"
    return upload_to_s3(user, payload.data)


@app.post("/terrence/lob")
def terrence_lob_upload(payload: LOBPayload):
    """
    Receives payload for Terrence's LOB and uploads it to S3.
    Prefix: terrence/year=/month=/day=/
    """
    user = "terrence"
    return upload_to_s3(user, payload.data)


@app.get("/")
def read_root():
    return {"message": "FastAPI S3 Service Running"}
