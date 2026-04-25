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
from processor import process_audio
from labelbox_client import push_to_labelbox
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


def _next_client_code(clients: dict) -> str:
    existing = {v["client_code"] for v in clients.values()}
    n = 1
    while True:
        code = f"CLIENT{n:03d}"
        if code not in existing:
            return code
        n += 1


@app.get("/access/{token}")
async def access(token: str):
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


@app.post("/api/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    client_code: str = Form(...),
    language: str = Form("en"),
):
    if client_code not in get_valid_client_codes():
        return JSONResponse(
            status_code=401,
            content={"success": False, "message": "Invalid client code"},
        )

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
    ]

    print(f"DEBUG: File content_type: {file.content_type}")
    print(f"DEBUG: File filename: {file.filename}")

    audio_extensions = ['.wav', '.mp3', '.m4a', '.ogg', '.flac', '.mp4']
    is_audio_by_extension = any(file.filename.lower().endswith(ext) for ext in audio_extensions)

    if file.content_type not in allowed_types and not is_audio_by_extension:
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
                blob_name = f"{client_code}/{timestamp}_{file.filename}"
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


    log_entry = {
        "client_code": client_code,
        "filename": file.filename,
        "file_size": file_size,
        "timestamp": timestamp,
        "status": "Uploaded",
    }

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

    if file.content_type in audio_types or is_audio_by_extension:
        background_tasks.add_task(
            run_full_pipeline,
            blob_filename=blob_name,
            client_code=client_code,
            language=language,
            original_filename=file.filename,
            timestamp=timestamp,
        )

    return {"success": True, "file_name": file.filename, "upload_id": blob_name}


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
async def get_files(client_code: str):
    if client_code not in get_valid_client_codes():
        raise HTTPException(status_code=401, detail="Invalid client code")

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
                (l for l in logs if l.get("client_code") == client_code and l.get("filename") == original_name),
                {},
            )
            status = log_entry.get("status", "Received")
            lang_code = log_entry.get("language")
            language = LANGUAGE_MAP.get(lang_code, "Detecting...") if lang_code else "Detecting..."

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
                }
            )

    return {"intake": intake_files, "delivery": delivery_files}


@app.get("/api/transcript/{client_code}/{full_name}")
async def get_transcript(client_code: str, full_name: str):
    if client_code not in get_valid_client_codes():
        raise HTTPException(status_code=401, detail="Invalid client code")

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
async def get_labelbox_status(client_code: str, filename: str):
    if client_code not in get_valid_client_codes():
        return {"status": "error", "message": "Invalid client code"}

    project_id = os.getenv("LABELBOX_PROJECT_ID")
    if not project_id:
        return {"status": "error", "message": "LABELBOX_PROJECT_ID not configured"}

    status = await asyncio.to_thread(check_annotation_status, client_code, project_id, filename)
    return status


@app.post("/api/export/{client_code}/{filename}")
async def export_results(client_code: str, filename: str):
    if client_code not in get_valid_client_codes():
        return {"status": "error", "message": "Invalid client code"}

    project_id = os.getenv("LABELBOX_PROJECT_ID")
    if not project_id:
        return {"status": "error", "message": "LABELBOX_PROJECT_ID not configured"}

    try:
        result = await asyncio.to_thread(export_and_deliver, client_code, filename, project_id)

        if result['status'] == 'success':
            async with aiofiles.open("upload_log.json", "r") as f:
                content = await f.read()
                logs = json.loads(content)

            for log in logs:
                if log.get("client_code") == client_code and log.get("filename") == filename:
                    log["status"] = "Completed"

            async with aiofiles.open("upload_log.json", "w") as f:
                await f.write(json.dumps(logs, indent=2))

            return {"success": True, "message": "Results exported and delivered to your download section.", "result": result}
        else:
            return {"success": False, "message": result.get("error", "Export failed")}

    except Exception as e:
        print(f"ERROR in export API: {str(e)}")
        return {"success": False, "message": str(e)}


@app.get("/api/download/{client_code}/{filename}")
async def download_file(client_code: str, filename: str):
    if client_code not in get_valid_client_codes():
        raise HTTPException(status_code=401, detail="Invalid client code")

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
        expiry=datetime.utcnow() + timedelta(hours=1),
    )
    download_url = f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/client-delivery/{blob_name}?{sas_token}"
    return {"download_url": download_url, "expires_in": "1 hour"}


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
    }
    save_clients(clients)
    return {
        "token": token,
        "client_code": client_code,
        "client_name": client_name,
        "active": True,
        "created_at": str(date.today()),
        "contact_email": contact_email,
    }


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
                lb_result = push_to_labelbox(
                    result['processed_file'],
                    client_code,
                    original_filename,
                )
                print(f"Labelbox push result: {lb_result}")
                if lb_result.get('status') == 'success':
                    sync_update("In Review")
                else:
                    error_detail = lb_result.get('error', 'Unknown error')
                    print(f"Labelbox push failed: {error_detail}")
                    sync_update("Failed (Labelbox)", labelbox_error=error_detail)
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


def _parse_labelbox_external_id(external_id: str):
    """Parse '[CLIENT001] filename.m4a (timestamp)' → (client_code, filename)."""
    match = re.match(r'\[([^\]]+)\]\s+(.+?)\s+\(', external_id)
    if match:
        return match.group(1), match.group(2)
    return None, None


async def _run_webhook_export(client_code: str, original_filename: str, project_id: str, label_id: str):
    try:
        result = await asyncio.to_thread(export_and_deliver, client_code, original_filename, project_id, label_id)
        if result.get("status") == "success":
            try:
                async with aiofiles.open("upload_log.json", "r") as f:
                    content = await f.read()
                logs = json.loads(content)
                for log in logs:
                    if log.get("client_code") == client_code and log.get("filename") == original_filename:
                        log["status"] = "Completed"
                        break
                async with aiofiles.open("upload_log.json", "w") as f:
                    await f.write(json.dumps(logs, indent=2))
            except Exception as e:
                print(f"Log update failed after webhook export: {e}")
        print(f"Webhook export result: {result}")
    except Exception as e:
        print(f"Webhook export failed: {e}")


@app.post("/webhook/labelbox")
async def labelbox_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()

    webhook_secret = os.getenv("LABELBOX_WEBHOOK_SECRET", "")
    if webhook_secret:
        signature = request.headers.get("X-Hub-Signature", "")
        expected = "sha1=" + hmac.new(webhook_secret.encode(), body, hashlib.sha1).hexdigest()
        if not hmac.compare_digest(signature, expected):
            raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_type = payload.get("type", "")
    if event_type not in ("REVIEW_CREATED", "REVIEW_UPDATED"):
        return {"received": True, "action": "ignored", "event": event_type}

    label = payload.get("label", {})
    review = label.get("review", {})
    if review.get("score", 0) != 1:
        return {"received": True, "action": "ignored", "reason": "not approved"}

    data_row = payload.get("data_row", {})
    external_id = data_row.get("external_id", "")
    label_id = label.get("id", "")
    project_id = os.getenv("LABELBOX_PROJECT_ID", "")

    client_code, original_filename = _parse_labelbox_external_id(external_id)
    if not client_code or not original_filename:
        print(f"Could not parse external_id: {external_id!r}")
        return {"received": True, "action": "ignored", "reason": "unparseable external_id"}

    background_tasks.add_task(
        _run_webhook_export,
        client_code=client_code,
        original_filename=original_filename,
        project_id=project_id,
        label_id=label_id,
    )

    print(f"Webhook: review approved for {client_code}/{original_filename}, export queued")
    return {"received": True, "action": "export_queued", "client_code": client_code, "filename": original_filename}


app.mount("/static", StaticFiles(directory="static"), name="static")
