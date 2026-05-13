from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks, Cookie, Request
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from azure.storage.blob.aio import BlobServiceClient
import os
import re
import json
import hmac
import secrets
import hashlib
import aiofiles
import asyncio
from datetime import datetime, timedelta, date
from pathlib import Path
import io
import zipfile
from processor import process_audio
from labelstudio_client import (
    push_to_labelstudio,
    push_jewelry_to_labelstudio,
    push_form_to_labelstudio,
    push_clickstream_to_labelstudio,
    push_text_transcript_to_labelstudio,
    generate_sas_url
)
from runpod_client import run_runpod_inference
from redactor import mask_text_data
from ocr_fallback import local_ocr_scan
from clickstream_parser import parse_clickstream_logs
from export_handler import check_annotation_status, export_and_deliver
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("ALLOWED_ORIGINS", "").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")

CLIENTS_FILE = Path(__file__).parent / "clients.json"

LANGUAGE_MAP = {
    'hi': 'Hindi',
    'ta': 'Tamil',
    'te': 'Telugu',
    'gu': 'Gujarati',
    'kn': 'Kannada',
    'ml': 'Malayalam',
    'bn': 'Bengali',
    'mr': 'Marathi',
    'pa': 'Punjabi',
    'ur': 'Urdu',
    'en': 'English',
}

_INVALID_LINK_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vaicore — Invalid Access Link</title>
    <link rel="stylesheet" href="/static/style.css">
</head>
<body>
    <div class="container" style="display:flex;align-items:center;justify-content:center;min-height:100vh;">
        <div class="card" style="max-width:480px;text-align:center;">
            <h1 class="logo">Vaicore</h1>
            <p style="color:#ff6b6b;font-size:1.1rem;margin-top:20px;">
                This access link is invalid or has expired.
            </p>
            <p style="color:#8A8A9A;margin-top:10px;">
                Please contact your Vaicore account manager.
            </p>
        </div>
    </div>
</body>
</html>"""


def load_clients() -> dict:
    if not CLIENTS_FILE.exists():
        return {}
    with open(CLIENTS_FILE) as f:
        return json.load(f)


def save_clients(clients: dict):
    with open(CLIENTS_FILE, "w") as f:
        json.dump(clients, f, indent=2)


def get_valid_client_codes() -> set:
    return {v["client_code"] for v in load_clients().values() if v.get("active")}


def _admin_token() -> str:
    return hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()


def _check_admin(cookie: str | None):
    if not ADMIN_PASSWORD or not cookie or cookie != _admin_token():
        raise HTTPException(status_code=401, detail="Admin authentication required")


async def verify_session_client(client_code: str, session_cookie: str | None) -> dict:
    """Enforce strict separation: validating the active cookie belongs to the requested client."""
    if not session_cookie:
        raise HTTPException(status_code=401, detail="Session cookie required")
    clients = load_clients()
    entry = clients.get(session_cookie)
    if not entry or not entry.get("active") or entry["client_code"] != client_code:
        raise HTTPException(status_code=403, detail="Access denied: Client code mismatch")
    return entry


async def verify_client_or_admin(
    client_code: str,
    vaicore_session: str | None = None,
    vaicore_admin: str | None = None,
):
    """Allow access if the request is from the specific client or an authorized admin."""
    try:
        _check_admin(vaicore_admin)
        return  # Admin is always authorized
    except HTTPException:
        pass

    await verify_session_client(client_code, vaicore_session)




def _next_client_code(clients: dict) -> str:
    existing = {v["client_code"] for v in clients.values()}
    n = 1
    while True:
        code = f"CLIENT{n:03d}"
        if code not in existing:
            return code
        n += 1


@app.get("/health")
async def health_check():
    return {"status": "healthy"}


@app.get("/access/{token}")
async def access(token: str):
    """Legacy internal access — redirects to Project Manager dashboard."""
    clients = load_clients()
    entry = clients.get(token)
    if not entry or not entry.get("active"):
        return HTMLResponse(content=_INVALID_LINK_HTML, status_code=403)
    response = RedirectResponse(url="/dashboard", status_code=302)
    response.set_cookie(
        key="vaicore_session",
        value=token,
        httponly=True,
        samesite="lax",
        max_age=86400 * 30,
    )
    return response


@app.get("/upload/{upload_token}")
async def client_upload_page(upload_token: str):
    """Client-facing upload page — no login required, just a valid upload token."""
    clients = load_clients()
    # Search all clients for this upload_token
    matched = None
    for token, entry in clients.items():
        if entry.get("upload_token") == upload_token and entry.get("active"):
            matched = entry
            break
    if not matched:
        return HTMLResponse(content=_INVALID_LINK_HTML, status_code=403)
    return FileResponse("static/client_upload.html")


@app.get("/api/upload-token-info/{upload_token}")
async def upload_token_info(upload_token: str):
    """Returns client info for a given upload token (used by client_upload.html)."""
    clients = load_clients()
    for token, entry in clients.items():
        if entry.get("upload_token") == upload_token and entry.get("active"):
            return {
                "client_code": entry["client_code"],
                "client_name": entry["client_name"],
                "upload_token": upload_token,
            }
    raise HTTPException(status_code=403, detail="Invalid or expired upload link")


@app.get("/download/{download_token}")
async def client_download_file(download_token: str):
    """Admin-generated download link. Validates token and serves the SAS redirect."""
    clients = load_clients()
    for token, entry in clients.items():
        dl_tokens = entry.get("download_tokens", {})
        if download_token in dl_tokens:
            blob_path = dl_tokens[download_token]["blob_path"]
            # Generate a 1-hour SAS URL for download
            sas_token = generate_blob_sas(
                account_name=AZURE_STORAGE_ACCOUNT_NAME,
                container_name="client-delivery",
                blob_name=blob_path,
                account_key=AZURE_STORAGE_ACCOUNT_KEY,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(hours=1)
            )
            sas_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/client-delivery/{blob_path}?{sas_token}"
            return RedirectResponse(url=sas_url, status_code=302)
    return HTMLResponse(content=_INVALID_LINK_HTML, status_code=403)


@app.post("/api/admin/clients/{token}/generate-upload-link")
async def generate_upload_link(token: str, vaicore_admin: str = Cookie(None)):
    """Generate or regenerate a client's upload token."""
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    upload_token = secrets.token_urlsafe(32)
    clients[token]["upload_token"] = upload_token
    save_clients(clients)
    return {"upload_token": upload_token, "upload_url": f"/upload/{upload_token}"}


@app.post("/api/admin/clients/{token}/generate-download-link")
async def generate_download_link(
    token: str,
    blob_path: str = Form(...),
    label: str = Form(""),
    vaicore_admin: str = Cookie(None)
):
    """Generate a one-time download link for a specific delivered file."""
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    download_token = secrets.token_urlsafe(32)
    if "download_tokens" not in clients[token]:
        clients[token]["download_tokens"] = {}
    clients[token]["download_tokens"][download_token] = {
        "blob_path": blob_path,
        "label": label or blob_path.split("/")[-1],
        "created_at": datetime.utcnow().isoformat(),
    }
    save_clients(clients)
    return {"download_token": download_token, "download_url": f"/download/{download_token}"}


@app.get("/api/me")
async def get_me(vaicore_session: str = Cookie(None)):
    if not vaicore_session:
        raise HTTPException(status_code=401, detail="Not authenticated")
    clients = load_clients()
    entry = clients.get(vaicore_session)
    if not entry or not entry.get("active"):
        raise HTTPException(status_code=401, detail="Invalid session")
    return {"client_code": entry["client_code"], "client_name": entry["client_name"]}


@app.post("/api/logout")
async def logout():
    response = RedirectResponse(url="/", status_code=302)
    response.delete_cookie("vaicore_session")
    return response


@app.post("/api/upload-via-token")
async def upload_file_via_token(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    upload_token: str = Form(...),
    language: str = Form("en"),
    category: str = Form("auto"),
):
    """Client-facing anonymous upload — authenticated via upload_token, not session cookie."""
    clients = load_clients()
    matched_entry = None
    for token, entry in clients.items():
        if entry.get("upload_token") == upload_token and entry.get("active"):
            matched_entry = entry
            break
    if not matched_entry:
        raise HTTPException(status_code=403, detail="Invalid or expired upload link")
    client_code = matched_entry["client_code"]

    # Re-use the same upload logic by calling through the internal pipeline
    # We pass client_code as a verified value, bypassing session check
    return await _run_upload_pipeline(background_tasks, file, client_code, language, category)


@app.post("/api/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_code: str = Form(...),
    language: str = Form("en"),
    category: str = Form("auto"),
    vaicore_session: str = Cookie(None),
):
    await verify_session_client(client_code, vaicore_session)
    return await _run_upload_pipeline(background_tasks, file, client_code, language, category)


async def _run_upload_pipeline(background_tasks: BackgroundTasks, file: UploadFile, client_code: str, language: str, category: str = "auto"):


    allowed_types = [
        "audio/mpeg",
        "audio/wav",
        "audio/x-wav",
        "audio/x-m4a",
        "audio/ogg",
        "audio/flac",
        "audio/mp4",
        "application/octet-stream",
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "image/jpeg",
        "image/png",
        "application/json",
        "text/csv",
        "text/plain",
        "application/zip",
        "application/x-zip-compressed",
    ]

    safe_filename = os.path.basename(file.filename)
    print(f"DEBUG: File content_type: {file.content_type}")
    print(f"DEBUG: File filename (sanitized): {safe_filename}")

    audio_extensions = ['.wav', '.mp3', '.m4a', '.ogg', '.flac', '.mp4']
    zip_extensions = ['.zip']
    is_audio_by_extension = any(safe_filename.lower().endswith(ext) for ext in audio_extensions)
    is_zip_by_extension = any(safe_filename.lower().endswith(ext) for ext in zip_extensions)

    if file.content_type not in allowed_types and not is_audio_by_extension and not is_zip_by_extension:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": f"File type not allowed: {file.content_type}"},
        )

    print(f"DEBUG: Reading file content...")
    content = await file.read()
    file_size = len(content)
    print(f"DEBUG: File size: {file_size} bytes")

    if file_size > 500 * 1024 * 1024:
        return JSONResponse(
            status_code=400,
            content={"success": False, "message": "File too large (max 500MB)"},
        )

    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"DEBUG: Connecting to Azure (Attempt {attempt + 1})...")
            async with BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING,
                connection_timeout=600,
                read_timeout=600,
            ) as blob_service_client:
                timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                blob_name = f"{client_code}/{timestamp}_{safe_filename}"
                blob_client = blob_service_client.get_blob_client(
                    container="client-intake", blob=blob_name
                )
                print(f"DEBUG: Uploading to Azure blob: {blob_name}...")
                # Removed max_concurrency to avoid broken pipe issues on fragile connections
                await blob_client.upload_blob(content, overwrite=True)
                print(f"DEBUG: Azure upload complete.")
                break
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"WARNING: Azure upload attempt {attempt + 1} failed: {str(e)}. Retrying...")
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"ERROR: Azure upload failed after {max_retries} attempts: {str(e)}")
                return JSONResponse(
                    status_code=500,
                    content={"success": False, "message": f"Azure upload failed: {str(e)}"},
                )


    is_zip = is_zip_by_extension or file.content_type in ["application/zip", "application/x-zip-compressed"]
    batch_id = f"BATCH_{timestamp}" if is_zip else None

    log_entry = {
        "client_code": client_code,
        "filename": safe_filename,
        "file_size": file_size,
        "timestamp": timestamp,
        "status": "Processing ZIP" if is_zip else "Uploaded",
    }
    if is_zip:
        log_entry["batch_id"] = batch_id
        log_entry["is_batch"] = True

    logs = []
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            log_content = await f.read()
            logs = json.loads(log_content)
    except (FileNotFoundError, json.JSONDecodeError):
        logs = []

    logs.append(log_entry)
    async with aiofiles.open("upload_log.json", "w") as f:
        await f.write(json.dumps(logs, indent=2))
    print(f"DEBUG: Logging complete.")

    audio_types = [
        "audio/mpeg", "audio/wav", "audio/x-m4a", "audio/ogg", "audio/flac", "audio/mp4",
    ]
    image_types = [
        "image/jpeg", "image/png", "image/webp", "image/gif", "image/tiff"
    ]
    doc_types = [
        "application/pdf", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ]

    filename_lower = safe_filename.lower()
    
    # 1. Check for ZIP batch
    if is_zip:
        background_tasks.add_task(
            run_zip_batch_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            original_filename=safe_filename,
            timestamp=timestamp,
            batch_id=batch_id,
            category=category,
        )
    # 2. Check for EXPLICIT category overrides from upload dropdown selector
    elif category == "jewelry" or category == "housing" or category == "business":
        background_tasks.add_task(
            run_jewelry_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            original_filename=safe_filename,
            timestamp=timestamp,
            force_project_type=category,
        )
    elif category == "audio":
        background_tasks.add_task(
            run_full_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            language=language,
            original_filename=safe_filename,
            timestamp=timestamp,
        )
    elif category == "form":
        background_tasks.add_task(
            run_form_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            original_filename=safe_filename,
            timestamp=timestamp,
        )
    elif category == "transcript":
        background_tasks.add_task(
            run_transcript_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            original_filename=safe_filename,
            timestamp=timestamp,
        )
    elif category == "clickstream":
        background_tasks.add_task(
            run_clickstream_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            original_filename=safe_filename,
            timestamp=timestamp,
        )
    # 3. Fallback to AUTO-DETECTION based on extension and filename keywords
    else:
        # A. Dispatch Audio Files
        if file.content_type in audio_types or is_audio_by_extension:
            background_tasks.add_task(
                run_full_pipeline,
                blob_filename=blob_name,
                client_code=client_code,
                language=language,
                original_filename=safe_filename,
                timestamp=timestamp,
            )
        # B. Dispatch Secure Form Scans / Documents
        elif (
            file.content_type in doc_types 
            or any(ext in filename_lower for ext in [".pdf", ".doc", ".docx"])
            or any(keyword in filename_lower for keyword in ["form", "invoice", "aadhaar", "pan", "kyc", "personal"])
        ):
            background_tasks.add_task(
                run_form_pipeline,
                blob_filename=blob_name,
                client_code=client_code,
                original_filename=safe_filename,
                timestamp=timestamp,
            )
        # C. Dispatch Text Transcripts
        elif (
            any(keyword in filename_lower for keyword in ["transcript", "conversation", "dialogue", "chat", "talk"])
            or filename_lower.endswith(".txt")
        ):
            background_tasks.add_task(
                run_transcript_pipeline,
                blob_filename=blob_name,
                client_code=client_code,
                original_filename=safe_filename,
                timestamp=timestamp,
            )
        # D. Dispatch Clickstream Logs
        elif (
            file.content_type in ["application/json", "text/csv"]
            or any(ext in filename_lower for ext in [".json", ".csv"])
            or "clickstream" in filename_lower
        ):
            background_tasks.add_task(
                run_clickstream_pipeline,
                blob_filename=blob_name,
                client_code=client_code,
                original_filename=safe_filename,
                timestamp=timestamp,
            )
        # E. Dispatch Images
        elif file.content_type in image_types or any(ext in filename_lower for ext in [".jpg", ".jpeg", ".png", ".webp"]):
            background_tasks.add_task(
                run_jewelry_pipeline,
                blob_filename=blob_name,
                client_code=client_code,
                original_filename=safe_filename,
                timestamp=timestamp,
                force_project_type="auto",
            )

    return {"success": True, "file_name": safe_filename, "upload_id": blob_name, "batch_id": batch_id}


async def update_log_status(client_code: str, filename: str, timestamp: str, status: str, **kwargs):
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            content = await f.read()
            logs = json.loads(content)

        updated = False
        for log in logs:
            if (
                log.get("client_code") == client_code
                and log.get("filename") == filename
                and log.get("timestamp") == timestamp
            ):
                log["status"] = status
                for key, value in kwargs.items():
                    log[key] = value
                updated = True
                break

        if updated:
            async with aiofiles.open("upload_log.json", "w") as f:
                await f.write(json.dumps(logs, indent=2))
            print(f"DEBUG: Status updated to '{status}' for {filename}")
    except Exception as e:
        print(f"ERROR: Failed to update log status: {str(e)}")


@app.get("/api/files/{client_code}")
async def get_files(
    client_code: str,
    vaicore_session: str = Cookie(None),
    vaicore_admin: str = Cookie(None),
):
    await verify_client_or_admin(client_code, vaicore_session, vaicore_admin)

    status_map = {}
    logs = []
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            log_content = await f.read()
            logs = json.loads(log_content)
            for log in logs:
                if log.get("client_code") == client_code:
                    status_map[log["filename"]] = log.get("status", "Received")
    except Exception:
        pass

    async with BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    ) as blob_service_client:
        container_client_intake = blob_service_client.get_container_client("client-intake")
        container_client_delivery = blob_service_client.get_container_client("client-delivery")
        container_client_processing = blob_service_client.get_container_client("processing")

        transcript_blobs = []
        async for blob in container_client_processing.list_blobs(name_starts_with=f"{client_code}/"):
            transcript_blobs.append(blob.name)

        delivery_files = []
        async for blob in container_client_delivery.list_blobs(name_starts_with=f"{client_code}/"):
            name = blob.name.split("/")[-1]
            delivery_files.append(
                {"name": name, "completed_on": blob.last_modified.isoformat()}
            )

        intake_files = []
        async for blob in container_client_intake.list_blobs(name_starts_with=f"{client_code}/"):
            full_name = blob.name.split("/")[-1]
            parts = full_name.split("_", 2)
            original_name = parts[2] if len(parts) >= 3 else full_name

            log_entry = next(
                (l for l in reversed(logs) if l.get("client_code") == client_code and l.get("filename") == original_name),
                {},
            )
            status = log_entry.get("status", "Received")
            lang_code = log_entry.get("language")
            language = LANGUAGE_MAP.get(lang_code, "Detecting...") if lang_code else "Detecting..."
            batch_id = log_entry.get("batch_id")
            parent_zip = log_entry.get("parent_zip")
            is_batch = log_entry.get("is_batch", False)

            transcript_name = f"{client_code}/{full_name}_transcript.json"
            transcript_available = transcript_name in transcript_blobs

            intake_files.append(
                {
                    "name": original_name,
                    "full_name": full_name,
                    "size": blob.size,
                    "uploaded_on": blob.last_modified.isoformat(),
                    "status": status,
                    "language": language,
                    "transcript_available": transcript_available,
                    "batch_id": batch_id,
                    "parent_zip": parent_zip,
                    "is_batch": is_batch
                }
            )

    return {"intake": intake_files, "delivery": delivery_files}


@app.get("/api/transcript/{client_code}/{full_name}")
async def get_transcript(
    client_code: str,
    full_name: str,
    vaicore_session: str = Cookie(None),
    vaicore_admin: str = Cookie(None),
):
    await verify_client_or_admin(client_code, vaicore_session, vaicore_admin)

    async with BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    ) as blob_service_client:
        blob_name = f"{client_code}/{full_name}_transcript.json"
        blob_client = blob_service_client.get_blob_client(
            container="processing", blob=blob_name
        )

        if not await blob_client.exists():
            raise HTTPException(status_code=404, detail="Transcript not found")

        blob_data = await blob_client.download_blob()
        content = await blob_data.readall()
        transcript_data = json.loads(content)

    return transcript_data


@app.get("/api/status/{client_code}/{filename}")
async def get_annotation_status(
    client_code: str,
    filename: str,
    vaicore_session: str = Cookie(None),
    vaicore_admin: str = Cookie(None),
):
    await verify_client_or_admin(client_code, vaicore_session, vaicore_admin)

    project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
    if not project_id:
        return {"status": "error", "message": "LABEL_STUDIO_JEWELRY_PROJECT_ID not configured"}

    status = await asyncio.to_thread(check_annotation_status, client_code, project_id, filename)
    return status


@app.post("/api/export/{client_code}/{filename}")
async def export_results(
    client_code: str,
    filename: str,
    internal: bool = False,
    vaicore_session: str = Cookie(None),
    vaicore_admin: str = Cookie(None),
):
    await verify_client_or_admin(client_code, vaicore_session, vaicore_admin)

    project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
    if not project_id:
        return {"status": "error", "message": "LABEL_STUDIO_JEWELRY_PROJECT_ID not configured"}

    try:
        result = await asyncio.to_thread(export_and_deliver, client_code, filename, project_id, internal_export=internal)

        if result['status'] == 'success':
            async with aiofiles.open("upload_log.json", "r") as f:
                content = await f.read()
                logs = json.loads(content)

            for log in logs:
                if log.get("client_code") == client_code and log.get("filename") == filename:
                    log["status"] = "Delivered"
                    break
            
            try:
                async with aiofiles.open("upload_log.json", "w") as f:
                    await f.write(json.dumps(logs, indent=2))
            except Exception as e:
                print(f"Failed to update log to Delivered: {e}")
            
            # Use the actual exported filename (zip/xlsx) for the download link, not the raw media filename
            exported_filename = result.get("xlsx_filename", filename)
            return {
                "success": True,
                "message": result.get("message"),
                "blob_path": result.get("blob_path", f"{client_code}/{exported_filename}")
            }
        
        elif result['status'] == 'duplicate_warning':
            # Return structured duplicate alert to admin dashboard
            return {
                "success": False,
                "duplicate_warning": True,
                "matches": result.get("matches", []),
                "total_matches": result.get("total_matches", 0),
                "message": result.get("message", "Duplicate collateral detected"),
            }
        else:
            return {"success": False, "message": result.get("error", "Export failed")}

    except Exception as e:
        print(f"ERROR in export API: {str(e)}")
        return {"success": False, "message": str(e)}


# ── Selective delivery endpoint ───────────────────────────────────────────────────
@app.post("/api/admin/package-delivery")
async def package_delivery(
    request: Request,
    vaicore_admin: str = Cookie(None),
):
    """PM selects completed files → builds one ZIP from their LS exports → uploads to client-delivery → returns blob_path."""
    _check_admin(vaicore_admin)
    body = await request.json()
    client_code: str = body.get("client_code", "")
    filenames: list = body.get("filenames", [])

    if not client_code or not filenames:
        raise HTTPException(status_code=400, detail="client_code and filenames are required")

    project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")

    zip_buffer = io.BytesIO()
    exported_files = []
    errors = []

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename in filenames:
            try:
                # Export this file’s annotations from Label Studio
                result = await asyncio.to_thread(
                    export_and_deliver, client_code, filename, project_id
                )

                if result.get("status") == "success":
                    exported_fname = result.get("xlsx_filename", filename)
                    blob_name_in_delivery = f"{client_code}/{exported_fname}"

                    # Download the just-exported blob and add it to the zip
                    conn_str = AZURE_STORAGE_CONNECTION_STRING
                    try:
                        from azure.storage.blob import BlobServiceClient as SyncBSC
                        sync_client = SyncBSC.from_connection_string(conn_str)
                        blob_c = sync_client.get_blob_client(container="client-delivery", blob=blob_name_in_delivery)
                        blob_data = blob_c.download_blob().readall()
                        zf.writestr(exported_fname, blob_data)
                        exported_files.append(filename)
                    except Exception as dl_err:
                        errors.append(f"{filename}: download failed after export ({dl_err})")
                else:
                    errors.append(f"{filename}: {result.get('error', 'export failed')}")

            except Exception as e:
                errors.append(f"{filename}: {str(e)}")

    if not exported_files:
        return {"success": False, "message": "No files could be exported.", "errors": errors}

    zip_buffer.seek(0)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    zip_blob_name = f"{client_code}/delivery_package_{timestamp}.zip"

    # Upload the final ZIP to client-delivery
    async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as bsc:
        bc = bsc.get_blob_client(container="client-delivery", blob=zip_blob_name)
        await bc.upload_blob(zip_buffer.read(), overwrite=True)

    # Mark all packaged files as Delivered in the log
    async with aiofiles.open("upload_log.json", "r") as f:
        logs = json.loads(await f.read())

    for log in logs:
        if log.get("client_code") == client_code and log.get("filename") in exported_files:
            log["status"] = "Delivered"

    async with aiofiles.open("upload_log.json", "w") as f:
        await f.write(json.dumps(logs, indent=2))

    return {
        "success": True,
        "packaged": len(exported_files),
        "blob_path": zip_blob_name,
        "errors": errors,
        "message": f"Packaged {len(exported_files)} file(s) into {zip_blob_name}"
    }


@app.post("/api/export/{client_code}/{filename}/force")
async def export_results_force(
    client_code: str,
    filename: str,
    internal: bool = False,
    vaicore_admin: str = Cookie(None),
):
    """Force delivery despite duplicate collateral warnings (admin override)."""
    _check_admin(vaicore_admin)

    project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
    if not project_id:
        return {"status": "error", "message": "LABEL_STUDIO_JEWELRY_PROJECT_ID not configured"}

    try:
        result = await asyncio.to_thread(
            export_and_deliver, client_code, filename, project_id, force_delivery=True, internal_export=internal
        )

        if result['status'] == 'success':
            async with aiofiles.open("upload_log.json", "r") as f:
                content = await f.read()
                logs = json.loads(content)

            for log in logs:
                if log.get("client_code") == client_code and log.get("filename") == filename:
                    log["status"] = "Delivered (Override)"
                    break
            
            try:
                async with aiofiles.open("upload_log.json", "w") as f:
                    await f.write(json.dumps(logs, indent=2))
            except Exception as e:
                print(f"Failed to update log to Delivered (Override): {e}")
            
            return {"success": True, "message": result.get("message") + " [Admin Override]"}
        else:
            return {"success": False, "message": result.get("error", "Export failed")}

    except Exception as e:
        print(f"ERROR in force export API: {str(e)}")
        return {"success": False, "message": str(e)}



@app.get("/api/download/{client_code}/{filename}")
async def download_file(
    client_code: str,
    filename: str,
    vaicore_session: str = Cookie(None),
    vaicore_admin: str = Cookie(None),
):
    await verify_client_or_admin(client_code, vaicore_session, vaicore_admin)

    async with BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    ) as blob_service_client:
        blob_name = f"{client_code}/{filename}"
        blob_client = blob_service_client.get_blob_client(
            container="client-delivery", blob=blob_name
        )

        if not await blob_client.exists():
            raise HTTPException(status_code=404, detail="File not found")

    sas_token = generate_blob_sas(
        account_name=AZURE_STORAGE_ACCOUNT_NAME,
        container_name="client-delivery",
        blob_name=blob_name,
        account_key=AZURE_STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=72),
    )
    download_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/client-delivery/{blob_name}?{sas_token}"
    return {"download_url": download_url, "expires_in": "72 hours"}


@app.get("/")
async def root():
    return FileResponse("static/index.html")


@app.get("/dashboard")
async def dashboard():
    return FileResponse("static/dashboard.html")


@app.get("/admin")
async def admin_page():
    return FileResponse("static/admin.html")


@app.post("/admin/login")
async def admin_login(password: str = Form(...)):
    if not ADMIN_PASSWORD or password != ADMIN_PASSWORD:
        return JSONResponse(status_code=401, content={"success": False, "message": "Invalid password"})
    response = JSONResponse(content={"success": True})
    response.set_cookie(
        key="vaicore_admin",
        value=_admin_token(),
        httponly=True,
        samesite="lax",
        max_age=86400 * 7,
    )
    return response


@app.post("/admin/logout")
async def admin_logout():
    response = JSONResponse(content={"success": True})
    response.delete_cookie("vaicore_admin")
    return response


@app.get("/api/admin/pipeline")
async def admin_get_pipeline(vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            content = await f.read()
        logs = json.loads(content)
        
        # Sort logs by timestamp descending
        logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
        
        # Limit to last 50 for performance
        pipeline = []
        project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
        
        for entry in logs[:200]:
            client_code = entry.get("client_code")
            filename = entry.get("filename")
            status = entry.get("status")
            
            # Check LS status if it's in review
            completion = 0
            if status in ("In Review", "Completed", "Review Finished"):
                try:
                    ls_status = await asyncio.to_thread(
                        check_annotation_status, client_code, project_id, filename
                    )
                    completion = ls_status.get("completion_percentage", 0)
                except Exception:
                    pass
            
            pipeline.append({
                "client_code": client_code,
                "filename": filename,
                "status": status or "Unknown",
                "uploaded_on": entry.get("timestamp"),
                "language": entry.get("language"),
                "completion_percentage": completion,
                "batch_id": entry.get("batch_id"),
                "is_batch": entry.get("is_batch", False),
                "parent_zip": entry.get("parent_zip")
            })
            
        return pipeline
    except Exception as e:
        print(f"Admin pipeline error: {e}")
        return []


@app.delete("/api/admin/jobs/{client_code}/{filename}")
async def admin_delete_job(client_code: str, filename: str, vaicore_admin: str = Cookie(None)):
    """Deletes an old or failed job from the upload log, tidying up the operations center."""
    _check_admin(vaicore_admin)
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            logs = json.loads(await f.read())
            
        initial_len = len(logs)
        logs = [log for log in logs if not (log.get("client_code") == client_code and log.get("filename") == filename)]
        
        if len(logs) < initial_len:
            async with aiofiles.open("upload_log.json", "w") as f:
                await f.write(json.dumps(logs, indent=2))
            return {"success": True, "message": "Job deleted successfully"}
        else:
            return {"success": False, "message": "Job not found"}
            
    except Exception as e:
        print(f"Admin delete error: {e}")
        return {"success": False, "message": str(e)}

@app.delete("/api/admin/batches/{batch_id}")
async def admin_delete_batch(batch_id: str, vaicore_admin: str = Cookie(None)):
    """Deletes an entire batch from the pipeline."""
    _check_admin(vaicore_admin)
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            logs = json.loads(await f.read())
            
        initial_len = len(logs)
        logs = [log for log in logs if log.get("batch_id") != batch_id]
        
        if len(logs) < initial_len:
            async with aiofiles.open("upload_log.json", "w") as f:
                await f.write(json.dumps(logs, indent=2))
            return {"success": True, "message": "Batch deleted successfully"}
        else:
            return {"success": False, "message": "Batch not found"}
            
    except Exception as e:
        print(f"Admin delete batch error: {e}")
        return {"success": False, "message": str(e)}



@app.get("/api/admin/clients")
async def admin_list_clients(vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    clients = load_clients()
    return [
        {
            "token": token,
            "client_code": data["client_code"],
            "client_name": data["client_name"],
            "active": data["active"],
            "created_at": data["created_at"],
            "contact_email": data.get("contact_email", ""),
            "project_ids": data.get("project_ids", {}),
            "role_labels": data.get("role_labels", []),
        }
        for token, data in clients.items()
    ]


@app.post("/api/admin/clients")
async def admin_add_client(
    client_name: str = Form(...),
    contact_email: str = Form(""),
    vaicore_admin: str = Cookie(None),
):
    _check_admin(vaicore_admin)
    clients = load_clients()
    token = secrets.token_urlsafe(16)
    client_code = _next_client_code(clients)
    clients[token] = {
        "client_code": client_code,
        "client_name": client_name,
        "active": True,
        "created_at": str(date.today()),
        "contact_email": contact_email,
        "project_ids": {},
        "role_labels": [],
    }
    save_clients(clients)
    return {
        "token": token,
        "client_code": client_code,
        "client_name": client_name,
        "active": True,
        "created_at": str(date.today()),
        "contact_email": contact_email,
        "project_ids": {},
        "role_labels": [],
    }


@app.patch("/api/admin/clients/{token}")
async def admin_update_client(
    token: str,
    client_name: str = Form(None),
    contact_email: str = Form(None),
    project_ids_json: str = Form(None),
    role_labels_json: str = Form(None),
    vaicore_admin: str = Cookie(None),
):
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
        
    client = clients[token]
    if client_name is not None:
        client["client_name"] = client_name
    if contact_email is not None:
        client["contact_email"] = contact_email
        
    if project_ids_json is not None:
        try:
            client["project_ids"] = json.loads(project_ids_json)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid project_ids JSON: {e}")
            
    if role_labels_json is not None:
        try:
            # Let's clean and validate labels
            labels = json.loads(role_labels_json)
            if isinstance(labels, list):
                client["role_labels"] = [str(l).strip() for l in labels if str(l).strip()]
            else:
                raise ValueError("role_labels must be a list of strings")
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid role_labels JSON: {e}")
            
    save_clients(clients)
    return {"success": True, "client": client}


@app.patch("/api/admin/clients/{token}/toggle")
async def admin_toggle_client(token: str, vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    clients[token]["active"] = not clients[token]["active"]
    save_clients(clients)
    return {"active": clients[token]["active"]}


@app.post("/api/admin/clients/{token}/rotate")
async def admin_rotate_token(token: str, vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    new_token = secrets.token_urlsafe(16)
    clients[new_token] = clients.pop(token)
    save_clients(clients)
    return {"token": new_token}


@app.delete("/api/admin/clients/{token}")
async def admin_delete_client(token: str, vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    clients = load_clients()
    if token not in clients:
        raise HTTPException(status_code=404, detail="Client not found")
    del clients[token]
    save_clients(clients)
    return {"success": True}


def run_full_pipeline(
    blob_filename: str,
    client_code: str,
    language: str,
    original_filename: str,
    timestamp: str,
):
    try:
        print(f"Starting pipeline for {client_code}/{blob_filename}")

        def sync_update(status, **kwargs):
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(update_log_status(client_code, original_filename, timestamp, status, **kwargs))
            finally:
                loop.close()

        sync_update("Transcribing")

        result = process_audio(blob_filename, client_code, language)

        if result['status'] == 'success':
            print(f"Audio processing complete: {result['segments']} segments")
            sync_update("Reviewing", language=result.get('language'))

            try:
                ls_result = push_to_labelstudio(
                    result['processed_file'],
                    client_code,
                    original_filename,
                    processed_blob=result.get('processed_blob'),
                    language=result.get('language'),
                )
                print(f"Label Studio push result: {ls_result}")
                if ls_result.get('status') == 'success':
                    sync_update("In Review")
                else:
                    error_detail = ls_result.get('error', 'Unknown error')
                    print(f"Label Studio push failed: {error_detail}")
                    sync_update("Failed (Label Studio)", labelstudio_error=error_detail)
            except Exception as e:
                print(f"Error pushing to Labelbox: {str(e)}")
                sync_update("Failed (Labelbox)", labelbox_error=str(e))
        else:
            print(f"Audio processing failed: {result.get('error')}")
            sync_update("Failed (Audio)")

    except Exception as e:
        print(f"Pipeline error: {str(e)}")
        try:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(update_log_status(client_code, original_filename, timestamp, "Error"))
            loop.close()
        except Exception:
            pass


async def resolve_image_category(client_code: str, original_filename: str) -> str:
    """
    Intelligently resolves an image's project type (jewelry, housing, or business).
    Rules:
    1. Keyword-based matching on the filename.
    2. Client-specific project_ids defaults from clients.json.
       If a client ONLY has one image project configured (e.g. they only have housing, or only have business),
       we automatically default to that category.
    3. OpenAI visual classification as the ultimate high-fidelity AI fallback.
    4. Global default ("jewelry").
    """
    filename_lower = original_filename.lower()
    
    # Rule 1: Filename Keyword Checking
    is_housing = any(k in filename_lower for k in ["house", "housing", "facade", "building", "property", "home", "villa", "apartment"])
    is_business = any(k in filename_lower for k in ["business", "store", "shop", "sign", "signboard", "license", "registration", "office", "retail"])
    is_jewelry = any(k in filename_lower for k in ["jewelry", "jewel", "necklace", "ring", "bangle", "gold", "silver", "platinum", "earring", "bracelet", "gem"])
    
    if is_housing:
        print(f"Keyword-detected category 'housing' for filename: {original_filename}")
        return "housing"
    elif is_business:
        print(f"Keyword-detected category 'business' for filename: {original_filename}")
        return "business"
    elif is_jewelry:
        print(f"Keyword-detected category 'jewelry' for filename: {original_filename}")
        return "jewelry"

    # Rule 2: Client Default Mapping
    try:
        if CLIENTS_FILE.exists():
            with open(CLIENTS_FILE, 'r', encoding='utf-8') as f:
                clients = json.load(f)
            for entry in clients.values():
                if entry.get("client_code") == client_code:
                    project_ids = entry.get("project_ids", {})
                    # If client has specific project type mapping but not others, use it
                    has_jewelry = "jewelry" in project_ids
                    has_housing = "housing" in project_ids
                    has_business = "business" in project_ids
                    
                    # If they only have ONE image project mapped, default to it
                    image_projects = [k for k in ["jewelry", "housing", "business"] if k in project_ids]
                    if len(image_projects) == 1:
                        print(f"Client {client_code} only has {image_projects[0]} project mapped. Defaulting to {image_projects[0]}.")
                        return image_projects[0]
    except Exception as e:
        print(f"Error checking client-level default projects: {e}")

    # Rule 3: OpenAI Visual Classification Fallback (High-fidelity)
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key and not openai_key.startswith("mock"):
        try:
            # Generate the secure image URL to send to OpenAI Vision API
            image_url = generate_sas_url(original_filename, client_code)
            import requests
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {openai_key}"
            }
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Classify this image into exactly one of these categories: 'jewelry', 'housing', or 'business'. "
                                        "Reply with only the category name in lowercase (e.g., 'jewelry', 'housing', or 'business'). "
                                        "Housing refers to residential buildings/houses. Business refers to store fronts, office spaces, licenses, or documents."
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": image_url
                                }
                            }
                        ]
                    }
                ],
                "max_tokens": 10,
                "temperature": 0.0
            }
            print(f"Calling OpenAI Vision model to visually classify: {original_filename}...")
            r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=12)
            r.raise_for_status()
            res_val = r.json()["choices"][0]["message"]["content"].strip().lower()
            # Clean response text in case it returned punctuation or markdown
            for cat in ["jewelry", "housing", "business"]:
                if cat in res_val:
                    print(f"OpenAI visual classification successful: '{cat}'")
                    return cat
        except Exception as vision_err:
            print(f"OpenAI vision classification failed: {vision_err}")

    # Rule 4: Global Default Fallback
    print(f"Unresolved image category for {original_filename}, falling back to global default: jewelry")
    return "jewelry"


# ── Pre-annotation helpers ───────────────────────────────────────────────────

def _fetch_image_numpy(url: str):
    """Download image from URL and decode to BGR numpy array. Returns None on failure."""
    try:
        import requests as _req
        import numpy as np
        import cv2
        resp = _req.get(url, timeout=15)
        resp.raise_for_status()
        arr = np.frombuffer(resp.content, dtype=np.uint8)
        img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        return img
    except Exception as e:
        print(f"[PreAnnotation] Image fetch failed: {e}")
        return None


def _opencv_preannotations(image_np, project_type: str) -> list:
    """OpenCV contour detection → Label Studio polygon predictions. Never raises."""
    try:
        import cv2
        h, w = image_np.shape[:2]
        gray = cv2.cvtColor(image_np, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (7, 7), 0)
        edges = cv2.Canny(blurred, 30, 100)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (5, 5))
        closed = cv2.morphologyEx(edges, cv2.MORPH_CLOSE, kernel)
        contours, _ = cv2.findContours(closed, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        label = "house_facade" if project_type == "housing" else "business_signage"
        min_area = w * h * 0.02  # ignore regions < 2% of image
        predictions = []
        for cnt in sorted(contours, key=cv2.contourArea, reverse=True)[:5]:
            if cv2.contourArea(cnt) < min_area:
                continue
            epsilon = 0.01 * cv2.arcLength(cnt, True)
            approx = cv2.approxPolyDP(cnt, epsilon, True)
            if len(approx) < 3:
                continue
            points = [
                [round(float(pt[0][0]) / w * 100, 2), round(float(pt[0][1]) / h * 100, 2)]
                for pt in approx
            ]
            predictions.append({"class": label, "points": points})

        print(f"[OpenCV] {len(predictions)} contour regions for {project_type}")
        return predictions
    except Exception as e:
        print(f"[OpenCV] Pre-annotation failed (non-blocking): {e}")
        return []


async def _sam_preannotations(image_url: str, project_type: str,
                               img_w: int = 1024, img_h: int = 1024) -> list:
    """Call SAM ML backend with 5-point auto-prompt. Returns predictions or [] on any failure."""
    try:
        import httpx
        sam_url = os.getenv("SAM_ML_BACKEND_URL", "http://sam-ml-backend:9090")
        label = "house_facade" if project_type == "housing" else "business_signage"

        # Health check — fast timeout so we don't stall
        async with httpx.AsyncClient(timeout=5.0) as hc:
            health = await hc.get(f"{sam_url}/health")
            if health.status_code != 200:
                print(f"[SAM] Backend unhealthy ({health.status_code}), skipping.")
                return []

        # 5-point cross pattern: center + four quadrant midpoints
        auto_points = [(50, 50), (25, 25), (75, 25), (25, 75), (75, 75)]
        context_results = [
            {
                "type": "keypointlabels",
                "value": {"x": x, "y": y, "keypointlabels": [label]},
                "original_width": img_w,
                "original_height": img_h,
                "is_positive": 1,
            }
            for x, y in auto_points
        ]
        payload = {
            "tasks": [{"id": 1, "data": {"image": image_url}}],
            "context": {"result": context_results},
        }

        async with httpx.AsyncClient(timeout=30.0) as hc:
            resp = await hc.post(f"{sam_url}/predict", json=payload)
            resp.raise_for_status()
            result = resp.json()

        predictions = []
        for pred in result:
            for region in pred.get("result", []):
                if region.get("type") == "polygonlabels":
                    predictions.append({
                        "class": label,
                        "points": region["value"]["points"],
                    })

        print(f"[SAM] {len(predictions)} polygon regions for {project_type}")
        return predictions
    except Exception as e:
        print(f"[SAM] Pre-annotation failed (non-blocking): {e}")
        return []


async def _get_image_preannotations(image_url: str, project_type: str) -> list:
    """SAM → OpenCV fallback. Never raises, always returns a list."""
    try:
        predictions = await _sam_preannotations(image_url, project_type)
        if predictions:
            return predictions
        print(f"[PreAnnotation] SAM empty for {project_type}, falling back to OpenCV.")
        image_np = await asyncio.to_thread(_fetch_image_numpy, image_url)
        if image_np is not None:
            return await asyncio.to_thread(_opencv_preannotations, image_np, project_type)
    except Exception as e:
        print(f"[PreAnnotation] All methods failed (non-blocking): {e}")
    return []


async def run_jewelry_pipeline(
    blob_filename: str,
    client_code: str,
    original_filename: str,
    timestamp: str,
    force_project_type: str = "auto",
):
    try:
        filename_lower = original_filename.lower()
        
        if force_project_type == "housing":
            project_type = "housing"
            project_name = "House Image Annotation"
        elif force_project_type == "business":
            project_type = "business"
            project_name = "Nature of Business"
        elif force_project_type == "jewelry":
            project_type = "jewelry"
            project_name = "Jewelry"
        else:
            project_type = await resolve_image_category(client_code, original_filename)
            if project_type == "housing":
                project_name = "House Image Annotation"
            elif project_type == "business":
                project_name = "Nature of Business"
            else:
                project_name = "Jewelry"

        print(f"Starting image pipeline ({project_name}) for {client_code}/{original_filename}")
        await update_log_status(client_code, original_filename, timestamp, "Processing Image")

        # 1. Get secure SAS URL for RunPod / Label Studio
        image_url = generate_sas_url(original_filename, client_code)
        
        # Redact the security signature token when printing to container logs
        clean_log_url = image_url.split("?")[0]
        print(f"Generated secure image url: {clean_log_url}")

        predictions = []
        if project_type == "jewelry":
            # 2a. RunPod inference for polygon pre-annotations
            print("Triggering RunPod jewelry classification model...")
            inference_result = await asyncio.to_thread(run_runpod_inference, image_url, task_type="jewelry")
            if inference_result.get("status") == "success":
                predictions = inference_result.get("predictions", [])
                print(f"Jewelry inference complete: detected {len(predictions)} items.")
            else:
                print(f"WARNING: RunPod inference failed: {inference_result.get('message')}. Proceeding with empty predictions.")

            # 2b. Collateral duplicate detection (non-blocking — never stalls pipeline)
            if predictions:
                try:
                    from collateral_detector import find_duplicates, generate_signature
                    image_np = await asyncio.to_thread(_fetch_image_numpy, image_url)
                    if image_np is not None:
                        h_px, w_px = image_np.shape[:2]
                        sigs = []
                        for idx, pred in enumerate(predictions):
                            sig = generate_signature(
                                image_np, pred.get("points", []),
                                pred.get("class", "Jewelry"), client_code,
                                task_id=0, item_index=idx,
                                image_file=original_filename,
                                img_width=w_px, img_height=h_px,
                            )
                            if sig:
                                sigs.append(sig)
                        if sigs:
                            matches = await asyncio.to_thread(find_duplicates, sigs)
                            if matches:
                                top = matches[0]
                                warn = (f"Matches {top['matched_item']['image_file']} "
                                        f"({top['similarity']:.0%} similar)")
                                print(f"[Collateral] DUPLICATE DETECTED: {original_filename} — {warn}")
                                await update_log_status(
                                    client_code, original_filename, timestamp,
                                    "Duplicate Detected", error=warn,
                                )
                            else:
                                print(f"[Collateral] No duplicates found for {original_filename}.")
                except Exception as cd_err:
                    print(f"[Collateral] Detection skipped (non-blocking): {cd_err}")
        else:
            # 2c. House / Business: SAM → OpenCV fallback pre-annotations
            print(f"Generating pre-annotations for {project_type} via SAM → OpenCV fallback...")
            predictions = await _get_image_preannotations(image_url, project_type)
            print(f"Pre-annotation complete: {len(predictions)} regions for {project_type}.")

        await update_log_status(client_code, original_filename, timestamp, "Reviewing")

        # 3. Push to Label Studio inside a non-blocking threadpool
        ls_result = await asyncio.to_thread(
            push_jewelry_to_labelstudio, 
            original_filename, 
            client_code, 
            predictions,
            project_type
        )
        
        if ls_result.get("status") == "success":
            await update_log_status(
                client_code, 
                original_filename, 
                timestamp, 
                "In Review", 
                predictions_count=len(predictions)
            )
        else:
            await update_log_status(
                client_code, 
                original_filename, 
                timestamp, 
                "Failed (Label Studio)", 
                error=ls_result.get("error")
            )

    except Exception as e:
        print(f"Image pipeline error: {str(e)}")
        try:
            await update_log_status(client_code, original_filename, timestamp, "Error", error=str(e))
        except Exception:
            pass


async def run_form_pipeline(
    blob_filename: str,
    client_code: str,
    original_filename: str,
    timestamp: str,
):
    try:
        print(f"Starting highly secure form OCR pipeline for {client_code}/{original_filename}")
        await update_log_status(client_code, original_filename, timestamp, "Parsing Form")

        # 1. Get secure SAS URL for local OCR scanner
        doc_url = generate_sas_url(original_filename, client_code)
        
        clean_log_url = doc_url.split("?")[0]
        print(f"Generated secure document url: {clean_log_url}")

        raw_text = ""
        # 2. Extract raw OCR from RunPod
        try:
            inference_result = await asyncio.to_thread(run_runpod_inference, doc_url, task_type="form")
            if inference_result.get("status") == "success":
                raw_text = inference_result.get("raw_ocr_text", "")
                print("RunPod OCR completed successfully.")
            else:
                print(f"WARNING: RunPod OCR returned non-success status. Message: {inference_result.get('message')}")
                raise Exception("Non-success RunPod response")
        except Exception as e:
            print(f"WARNING: RunPod Form OCR failed ({str(e)}). Routing to local OCR Fallback Sandbox...")
            # Route to our robust local OCR fallbacks (EasyOCR or Sandbox Simulation)
            raw_text = await asyncio.to_thread(local_ocr_scan, doc_url)
        
        # 3. Apply Local PII Redaction/Masking before reviewer exposure (Name, Phone, Bank, Aadhaar, PAN)
        anonymized_text = mask_text_data(raw_text)
        
        await update_log_status(client_code, original_filename, timestamp, "Reviewing")
        
        # 4. Push masked data to secure Label Studio project
        ls_result = await asyncio.to_thread(push_form_to_labelstudio, original_filename, client_code, anonymized_text)
        if ls_result.get("status") == "success":
            await update_log_status(client_code, original_filename, timestamp, "In Review")
        else:
            await update_log_status(client_code, original_filename, timestamp, "Failed (Label Studio)", error=ls_result.get("error"))

    except Exception as e:
        print(f"Form pipeline error: {str(e)}")
        try:
            await update_log_status(client_code, original_filename, timestamp, "Error", error=str(e))
        except Exception:
            pass


async def run_clickstream_pipeline(
    blob_filename: str,
    client_code: str,
    original_filename: str,
    timestamp: str,
):
    try:
        print(f"Starting clickstream sequence pipeline for {client_code}/{original_filename}")
        await update_log_status(client_code, original_filename, timestamp, "Parsing Logs")
        
        # 1. Fetch raw log bytes from Azure Blob Intake container
        raw_content = b""
        try:
            async with BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            ) as blob_service_client:
                blob_client = blob_service_client.get_blob_client(
                    container="client-intake", blob=blob_filename
                )
                if await blob_client.exists():
                    blob_data = await blob_client.download_blob()
                    raw_content = await blob_data.readall()
                    print(f"Successfully downloaded raw clickstream content: {len(raw_content)} bytes")
        except Exception as e:
            print(f"WARNING: Failed to fetch raw clickstream blob from Azure ({str(e)}). Proceeding with simulation fallback...")

        # 2. Parse clickstream timeline events dynamically using our analysis engine
        clickstream_data = await asyncio.to_thread(parse_clickstream_logs, raw_content, original_filename)
        
        await update_log_status(client_code, original_filename, timestamp, "Reviewing")
        
        # 3. Push timeline actions to Label Studio inside threadpool
        ls_result = await asyncio.to_thread(push_clickstream_to_labelstudio, original_filename, client_code, clickstream_data)
        if ls_result.get("status") == "success":
            await update_log_status(client_code, original_filename, timestamp, "In Review")
        else:
            await update_log_status(client_code, original_filename, timestamp, "Failed (Label Studio)", error=ls_result.get("error"))

    except Exception as e:
        print(f"Clickstream pipeline error: {str(e)}")
        try:
            await update_log_status(client_code, original_filename, timestamp, "Error", error=str(e))
        except Exception:
            pass


async def run_transcript_pipeline(
    blob_filename: str,
    client_code: str,
    original_filename: str,
    timestamp: str,
):
    try:
        print(f"Starting text transcript pipeline for {client_code}/{original_filename}")
        await update_log_status(client_code, original_filename, timestamp, "Parsing Logs")
        
        # 1. Fetch raw content from Azure blob
        raw_content = b""
        try:
            from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
            async with AsyncBlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            ) as blob_service_client:
                blob_client = blob_service_client.get_blob_client(
                    container="client-intake", blob=blob_filename
                )
                if await blob_client.exists():
                    blob_data = await blob_client.download_blob()
                    raw_content = await blob_data.readall()
                    print(f"Successfully downloaded raw transcript content: {len(raw_content)} bytes")
        except Exception as e:
            print(f"WARNING: Failed to fetch raw transcript blob from Azure ({str(e)}). Proceeding with default template...")

        # 2. Parse text transcript dynamically
        from transcript_parser import parse_transcript_content
        segments = await asyncio.to_thread(parse_transcript_content, raw_content, original_filename)
        
        await update_log_status(client_code, original_filename, timestamp, "Reviewing")
        
        # 3. Push segments with pre-annotations to Label Studio inside threadpool
        ls_result = await asyncio.to_thread(push_text_transcript_to_labelstudio, original_filename, client_code, segments)
        if ls_result.get("status") == "success":
            await update_log_status(client_code, original_filename, timestamp, "In Review")
        else:
            await update_log_status(client_code, original_filename, timestamp, "Failed (Label Studio)", error=ls_result.get("error"))

    except Exception as e:
        print(f"Transcript pipeline error: {str(e)}")
        try:
            await update_log_status(client_code, original_filename, timestamp, "Error", error=str(e))
        except Exception:
            pass



async def _run_webhook_export(client_code: str, original_filename: str, project_id: str, label_id: str):
    try:
        # Gatekeeper Workflow: DO NOT auto-deliver. 
        # Just update the upload log to trigger the Admin Dashboard's manual "Deliver to Client" button!
        try:
            async with aiofiles.open("upload_log.json", "r") as f:
                content = await f.read()
            logs = json.loads(content)
            for log in logs:
                if log.get("client_code") == client_code and log.get("filename") == original_filename:
                    log["status"] = "Review Finished"
                    break
            async with aiofiles.open("upload_log.json", "w") as f:
                await f.write(json.dumps(logs, indent=2))
            print(f"Webhook processed: Status updated to 'Review Finished' for {client_code}/{original_filename}")
        except Exception as e:
            print(f"Log update failed in webhook: {e}")
    except Exception as e:
        print(f"Webhook pipeline failed: {e}")


async def run_zip_batch_pipeline(
    blob_filename: str,
    client_code: str,
    original_filename: str,
    timestamp: str,
    batch_id: str,
    category: str = "auto",
):
    try:
        print(f"Starting ZIP batch pipeline for {client_code}/{original_filename} (Batch ID: {batch_id})")
        # 1. Download the ZIP file from Azure client-intake container
        async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as blob_service_client:
            blob_client = blob_service_client.get_blob_client(container="client-intake", blob=blob_filename)
            blob_data = await blob_client.download_blob()
            zip_content = await blob_data.readall()

        import zipfile
        import io
        import tempfile
        import shutil

        temp_dir = Path("scratch/batches") / batch_id
        temp_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            z.extractall(temp_dir)

        # Walk through files recursively
        extracted_files = []
        for root, dirs, files in os.walk(temp_dir):
            for file in files:
                if file.startswith(".") or file.startswith("__") or "macosx" in root.lower():
                    continue
                full_path = Path(root) / file
                relative_path = full_path.relative_to(temp_dir)
                extracted_files.append((full_path, relative_path.as_posix()))

        print(f"ZIP batch extracted {len(extracted_files)} valid files.")

        if not extracted_files:
            await update_log_status(client_code, original_filename, timestamp, "Failed (Empty ZIP)", batch_id=batch_id)
            return

        # Initialize tracking total files
        await update_log_status(
            client_code,
            original_filename,
            timestamp,
            "Processing Batch",
            batch_id=batch_id,
            total_files=len(extracted_files),
            processed_files=0
        )

        # Upload files back and run pipelines
        async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as blob_service_client:
            for idx, (file_path, rel_path_str) in enumerate(extracted_files):
                file_size = file_path.stat().st_size
                with open(file_path, "rb") as f:
                    file_content = f.read()

                sub_safe_filename = os.path.basename(rel_path_str)
                sub_blob_name = f"{client_code}/batches/{batch_id}/{sub_safe_filename}"
                sub_blob_client = blob_service_client.get_blob_client(container="client-intake", blob=sub_blob_name)
                await sub_blob_client.upload_blob(file_content, overwrite=True)

                sub_timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

                sub_log_entry = {
                    "client_code": client_code,
                    "filename": sub_safe_filename,
                    "file_size": file_size,
                    "timestamp": sub_timestamp,
                    "status": "Uploaded",
                    "batch_id": batch_id,
                    "parent_zip": original_filename,
                    "sub_blob_name": sub_blob_name
                }

                # Read and write to logs safely
                async with aiofiles.open("upload_log.json", "r") as lf:
                    log_content = await lf.read()
                    logs = json.loads(log_content)
                logs.append(sub_log_entry)
                async with aiofiles.open("upload_log.json", "w") as lf:
                    await lf.write(json.dumps(logs, indent=2))

                sub_filename_lower = sub_safe_filename.lower()
                audio_extensions = ['.wav', '.mp3', '.m4a', '.ogg', '.flac', '.mp4']
                doc_extensions = ['.pdf', '.doc', '.docx']
                image_extensions = ['.jpg', '.jpeg', '.png', '.webp', '.gif', '.tiff']
                data_extensions = ['.json', '.csv']

                if any(sub_filename_lower.endswith(ext) for ext in audio_extensions):
                    asyncio.create_task(run_full_pipeline(
                        blob_filename=sub_blob_name,
                        client_code=client_code,
                        language="en",
                        original_filename=sub_safe_filename,
                        timestamp=sub_timestamp
                    ))
                elif any(ext in sub_filename_lower for ext in doc_extensions) or any(keyword in sub_filename_lower for keyword in ["form", "invoice", "aadhaar", "pan", "kyc", "personal"]):
                    asyncio.create_task(run_form_pipeline(
                        blob_filename=sub_blob_name,
                        client_code=client_code,
                        original_filename=sub_safe_filename,
                        timestamp=sub_timestamp
                    ))
                elif any(keyword in sub_filename_lower for keyword in ["transcript", "conversation", "dialogue", "chat", "talk"]) or sub_filename_lower.endswith(".txt"):
                    asyncio.create_task(run_transcript_pipeline(
                        blob_filename=sub_blob_name,
                        client_code=client_code,
                        original_filename=sub_safe_filename,
                        timestamp=sub_timestamp
                    ))
                elif any(sub_filename_lower.endswith(ext) for ext in data_extensions) or "clickstream" in sub_filename_lower:
                    asyncio.create_task(run_clickstream_pipeline(
                        blob_filename=sub_blob_name,
                        client_code=client_code,
                        original_filename=sub_safe_filename,
                        timestamp=sub_timestamp
                    ))
                elif any(sub_filename_lower.endswith(ext) for ext in image_extensions):
                    asyncio.create_task(run_jewelry_pipeline(
                        blob_filename=sub_blob_name,
                        client_code=client_code,
                        original_filename=sub_safe_filename,
                        timestamp=sub_timestamp,
                        force_project_type=category,
                    ))
                else:
                    print(f"Unknown extension for {sub_safe_filename}, uploaded without processing.")

        shutil.rmtree(temp_dir, ignore_errors=True)
        await update_log_status(client_code, original_filename, timestamp, "In Review", batch_id=batch_id)

    except Exception as e:
        print(f"ZIP Batch pipeline error: {str(e)}")
        try:
            await update_log_status(client_code, original_filename, timestamp, "Error", error=str(e), batch_id=batch_id)
        except Exception:
            pass


@app.post("/api/admin/batches/{batch_id}/deliver")
async def admin_deliver_batch(batch_id: str, vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    try:
        # 1. Fetch child items from logs
        async with aiofiles.open("upload_log.json", "r") as f:
            content = await f.read()
        logs = json.loads(content)

        batch_logs = [log for log in logs if log.get("batch_id") == batch_id and not log.get("is_batch")]
        parent_log = next((log for log in logs if log.get("batch_id") == batch_id and log.get("is_batch")), None)

        if not batch_logs:
            raise HTTPException(status_code=404, detail="No files found in batch")

        client_code = batch_logs[0]["client_code"]
        project_id = os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")

        # 2. Trigger individual exports
        exported_files = []
        for log in batch_logs:
            filename = log["filename"]
            res = await asyncio.to_thread(export_and_deliver, client_code, filename, project_id)
            if res.get("status") == "success":
                exported_files.append(res)

        # 3. Compile a single ZIP file containing all exports
        import zipfile
        import io
        
        zip_buffer = io.BytesIO()
        async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as blob_service_client:
            container_delivery = blob_service_client.get_container_client("client-delivery")
            
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as z:
                for res in exported_files:
                    filename_in_container = res.get("xlsx_filename")
                    if not filename_in_container:
                        continue
                    blob_name = f"{client_code}/{filename_in_container}"
                    try:
                        b_client = container_delivery.get_blob_client(blob_name)
                        if await b_client.exists():
                            blob_data = await b_client.download_blob()
                            file_content = await blob_data.readall()
                            z.writestr(filename_in_container, file_content)
                    except Exception as ex:
                        print(f"Failed to include {blob_name} in batch zip: {ex}")

        # 4. Upload ZIP delivery back to Azure
        batch_zip_name = f"{batch_id}_delivered.zip"
        batch_blob_name = f"{client_code}/{batch_zip_name}"
        
        async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as blob_service_client:
            container_delivery = blob_service_client.get_container_client("client-delivery")
            b_client = container_delivery.get_blob_client(batch_blob_name)
            await b_client.upload_blob(zip_buffer.getvalue(), overwrite=True)

        # 5. Update logs to Delivered
        for log in logs:
            if log.get("batch_id") == batch_id:
                log["status"] = "Delivered"

        async with aiofiles.open("upload_log.json", "w") as f:
            await f.write(json.dumps(logs, indent=2))

        # 6. Generate secure 72-hour SAS link
        exported_filename = batch_zip_name
        batch_blob_name = f"{client_code}/{exported_filename}"
        sas_token = generate_blob_sas(
            account_name=AZURE_STORAGE_ACCOUNT_NAME,
            container_name="client-delivery",
            blob_name=batch_blob_name,
            account_key=AZURE_STORAGE_ACCOUNT_KEY,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=72),
        )
        download_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/client-delivery/{batch_blob_name}?{sas_token}"

        return {
            "success": True,
            "message": f"Successfully packaged and delivered batch {batch_id}!",
            "download_url": download_url,
            "expires_in": "72 hours"
        }
    except Exception as e:
        print(f"Error packaging batch delivery {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/admin/batches/{batch_id}")
async def admin_delete_batch(batch_id: str, vaicore_admin: str = Cookie(None)):
    _check_admin(vaicore_admin)
    try:
        async with aiofiles.open("upload_log.json", "r") as f:
            content = await f.read()
        logs = json.loads(content)

        deleted_filenames = []
        deleted_sub_blobs = []
        client_code = None

        new_logs = []
        for log in logs:
            if log.get("batch_id") == batch_id:
                deleted_filenames.append(log.get("filename"))
                client_code = log.get("client_code")
                if log.get("sub_blob_name"):
                    deleted_sub_blobs.append(log.get("sub_blob_name"))
                if log.get("is_batch"):
                    deleted_sub_blobs.append(f"{client_code}/{log.get('timestamp')}_{log.get('filename')}")
            else:
                new_logs.append(log)

        if not deleted_filenames:
            raise HTTPException(status_code=404, detail="Batch not found")

        async with aiofiles.open("upload_log.json", "w") as f:
            await f.write(json.dumps(new_logs, indent=2))

        async with BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING) as blob_service_client:
            container_intake = blob_service_client.get_container_client("client-intake")
            container_processing = blob_service_client.get_container_client("processing")
            container_delivery = blob_service_client.get_container_client("client-delivery")

            for blob_name in deleted_sub_blobs:
                try:
                    b_client = container_intake.get_blob_client(blob_name)
                    if await b_client.exists():
                        await b_client.delete_blob()
                        print(f"Deleted intake blob: {blob_name}")
                except Exception as ex:
                    print(f"Failed to delete intake blob {blob_name}: {ex}")

            if client_code:
                async for blob in container_intake.list_blobs(name_starts_with=f"{client_code}/batches/{batch_id}/"):
                    try:
                        await container_intake.delete_blob(blob.name)
                        print(f"Deleted remaining intake blob: {blob.name}")
                    except Exception:
                        pass
                
                async for blob in container_processing.list_blobs(name_starts_with=f"{client_code}/"):
                    if batch_id in blob.name:
                        try:
                            await container_processing.delete_blob(blob.name)
                            print(f"Deleted processing blob: {blob.name}")
                        except Exception:
                            pass

                async for blob in container_delivery.list_blobs(name_starts_with=f"{client_code}/"):
                    if batch_id in blob.name:
                        try:
                            await container_delivery.delete_blob(blob.name)
                            print(f"Deleted delivery blob: {blob.name}")
                        except Exception:
                            pass

        return {"success": True, "message": f"Successfully deleted batch {batch_id} and all nested resources."}
    except Exception as e:
        print(f"Error deleting batch {batch_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/webhook/labelstudio")
async def labelstudio_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()

    # Label Studio signs with HMAC-SHA256 in the X-LS-Signature header
    webhook_secret = os.getenv("LABEL_STUDIO_WEBHOOK_SECRET", "")
    if webhook_secret:
        signature = request.headers.get("X-LS-Signature", "")
        expected = hmac.new(webhook_secret.encode(), body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    action = payload.get("action", "")

    annotation = payload.get("annotation", {})
    
    # Accept any completed annotation so the admin can deliver it
    if action not in ("ANNOTATION_UPDATED", "ANNOTATION_CREATED", "REVIEW_CREATED", "REVIEW_UPDATED"):
        return {"received": True, "action": "ignored", "reason": f"Action not supported: {action}"}

    # Task data contains client_code and filename set during push_to_labelstudio
    task = payload.get("task", {})
    task_data = task.get("data", {})
    client_code = task_data.get("client_code", "")
    original_filename = task_data.get("filename", "")
    project_id = str(payload.get("project", {}).get("id", os.getenv("LABEL_STUDIO_PROJECT_ID", "")))
    annotation_id = str(annotation.get("id", ""))

    if not client_code or not original_filename:
        print(f"Missing client_code or filename in task data: {task_data!r}")
        return {"received": True, "action": "ignored", "reason": "missing client_code or filename"}

    background_tasks.add_task(
        _run_webhook_export,
        client_code=client_code,
        original_filename=original_filename,
        project_id=project_id,
        label_id=annotation_id,
    )

    print(f"Webhook: {action} for {client_code}/{original_filename}, export queued")
    return {"received": True, "action": "export_queued", "client_code": client_code, "filename": original_filename}


app.mount("/static", StaticFiles(directory="static"), name="static")
