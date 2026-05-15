import json
import os
import uuid
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Dict, Any
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, ContentSettings

load_dotenv()

import requests as _req

def _resolve_token(token: str, url: str) -> str:
    """If token is a refresh JWT, exchange it for a short-lived access token."""
    if token and token.startswith("yJ"):
        token = "e" + token
    if not token or not token.startswith("eyJ"):
        return token  # legacy short token, use as-is
    import base64, json as _json
    try:
        payload = token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = _json.loads(base64.b64decode(payload))
        if claims.get("token_type") == "refresh":
            r = _req.post(f"{url}/api/token/refresh", json={"refresh": token}, timeout=15)
            r.raise_for_status()
            return r.json()["access"]
    except Exception as e:
        print(f"Token refresh failed, using token as-is: {e}")
    return token

def _ls_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def get_client_project_id(client_code: str, project_type: str, fallback_env_var: str, default_val: str) -> str:
    """Resolve project ID from clients.json if configured, else fallback to env var / default."""
    try:
        import json
        clients_file = os.path.join(os.path.dirname(__file__), "clients.json")
        if os.path.exists(clients_file):
            with open(clients_file, 'r', encoding='utf-8') as f:
                clients = json.load(f)
            for entry in clients.values():
                if entry.get("client_code") == client_code:
                    project_ids = entry.get("project_ids", {})
                    if project_type in project_ids:
                        val = str(project_ids[project_type])
                        if val.strip():
                            return val
    except Exception as e:
        print(f"Error reading project_ids from clients.json for client {client_code}: {e}")
    
    env_val = os.getenv(fallback_env_var)
    if env_val and env_val.strip():
        return env_val
        
    if fallback_env_var == "LABEL_STUDIO_PROJECT_ID" or fallback_env_var == "LABEL_STUDIO_AUDIO_PROJECT_ID":
        return "8"
    elif fallback_env_var == "LABEL_STUDIO_HOUSING_PROJECT_ID":
        return "5"
    elif fallback_env_var == "LABEL_STUDIO_BUSINESS_PROJECT_ID":
        return "6"
    elif fallback_env_var == "LABEL_STUDIO_JEWELRY_PROJECT_ID":
        return "2"
    elif fallback_env_var == "LABEL_STUDIO_FORM_PROJECT_ID":
        return "3"
    elif fallback_env_var == "LABEL_STUDIO_CLICKSTREAM_PROJECT_ID":
        return "4"
    elif fallback_env_var == "LABEL_STUDIO_TRANSCRIPT_PROJECT_ID":
        return "7"
        
    return default_val


try:
    from label_studio_sdk import LabelStudio
except ImportError:
    print("Label Studio SDK not installed. Install with: pip install label-studio-sdk")
    raise


def _transcode_to_mp3(input_data: bytes, original_ext: str = ".m4a") -> bytes:
    """Transcode audio bytes to MP3 using ffmpeg."""
    with tempfile.NamedTemporaryFile(suffix=original_ext, delete=False) as tmp_in:
        tmp_in.write(input_data)
        tmp_in_path = tmp_in.name

    tmp_out_path = tmp_in_path.replace(original_ext, ".mp3")
    try:
        subprocess.run(
            ['ffmpeg', '-y', '-i', tmp_in_path, '-codec:a', 'libmp3lame', '-qscale:a', '2', tmp_out_path],
            check=True, capture_output=True
        )
        with open(tmp_out_path, "rb") as f:
            return f.read()
    finally:
        if os.path.exists(tmp_in_path):
            os.remove(tmp_in_path)
        if os.path.exists(tmp_out_path):
            os.remove(tmp_out_path)


def _make_sas_url(account_name: str, account_key: str, container: str, blob_name: str) -> str:
    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=30),
    )
    return f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}?{sas_token}"


def generate_mp3_sas_url(filename: str, client_code: str) -> str:
    """
    Return a 30-day SAS URL for an audio file suitable for Label Studio playback.
    Attempts to transcode to MP3 via ffmpeg when available; falls back to the
    original file (M4A/WAV) with a direct SAS URL when ffmpeg is absent.
    """
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")

    parts = {}
    for item in connection_string.split(';'):
        if '=' in item:
            key, value = item.split('=', 1)
            parts[key] = value

    account_name = parts.get('AccountName')
    account_key = parts.get('AccountKey')

    if not account_name or not account_key:
        raise ValueError("Invalid AZURE_STORAGE_CONNECTION_STRING: missing AccountName or AccountKey")

    blob_svc = BlobServiceClient.from_connection_string(connection_string)

    # Find original blob (may have timestamp prefix)
    intake_cc = blob_svc.get_container_client("client-intake")
    blobs = list(intake_cc.list_blobs(name_starts_with=f"{client_code}/"))
    matching = [b.name for b in blobs if b.name.endswith(filename)]
    if not matching:
        raise ValueError(f"No blob found in client-intake matching {client_code}/*{filename}")
    src_blob_name = sorted(matching)[-1]

    ext = os.path.splitext(src_blob_name)[1].lower()
    mp3_blob_name = src_blob_name[: -len(ext)] + ".mp3"

    mp3_blob_client = blob_svc.get_blob_client(container="processing", blob=mp3_blob_name)

    if mp3_blob_client.exists():
        print(f"Reusing existing MP3 at processing/{mp3_blob_name}")
        return _make_sas_url(account_name, account_key, "processing", mp3_blob_name)

    # Try transcoding with ffmpeg; fall back to original when ffmpeg is unavailable
    print(f"Transcoding {src_blob_name} → MP3 for Label Studio...")
    audio_data = blob_svc.get_blob_client(container="client-intake", blob=src_blob_name).download_blob().readall()
    try:
        mp3_data = _transcode_to_mp3(audio_data, ext)
        mp3_blob_client.upload_blob(
            mp3_data, overwrite=True,
            content_settings=ContentSettings(content_type="audio/mpeg"),
        )
        print(f"Uploaded MP3 to processing/{mp3_blob_name}")
        return _make_sas_url(account_name, account_key, "processing", mp3_blob_name)
    except (FileNotFoundError, subprocess.SubprocessError) as e:
        print(f"ffmpeg unavailable ({e}), serving original {ext} directly")
        return _make_sas_url(account_name, account_key, "client-intake", src_blob_name)


# Member ID mappings in your Label Studio instance:
# Update these integer IDs based on the User IDs generated when annotators register.
LANGUAGE_ANNOTATOR_MAP = {
    "hi": [12],  # Hindi Expert User ID
    "mr": [15],  # Marathi Expert User ID
    "ta": [18],  # Tamil Expert User ID
    "bn": [19],  # Bengali Expert User ID
    "en": [20],  # English Expert User ID
}

def push_to_labelstudio(
    processed_file_path: str,
    client_code: str,
    original_filename: str,
    processed_blob: str = None,
    language: str = None,
) -> Dict[str, Any]:
    """
    Push processed audio and pre-annotations to Label Studio.

    processed_blob: Azure blob path inside the 'processing' container
                    (e.g. "CLIENT001/audio_processed.json"). When provided,
                    segments are loaded from Azure instead of the local path.
    processed_file_path: local fallback path (used when processed_blob is None).
    """
    try:
        print(f"Starting Label Studio upload for {client_code}/{original_filename}")

        ls_url = os.getenv("LABEL_STUDIO_URL")
        api_key = os.getenv("LABEL_STUDIO_API_KEY")
        project_id = get_client_project_id(client_code, "audio", "LABEL_STUDIO_PROJECT_ID", "1")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not ls_url or not api_key or not project_id:
            raise ValueError("Label Studio credentials not found in environment (LABEL_STUDIO_URL, LABEL_STUDIO_API_KEY, LABEL_STUDIO_PROJECT_ID)")

        # STEP 1: Load processed JSON — prefer Azure blob, fall back to local file
        if processed_blob and connection_string:
            print(f"Loading segments from Azure processing/{processed_blob}...")
            blob_svc = BlobServiceClient.from_connection_string(connection_string)
            bc = blob_svc.get_blob_client(container="processing", blob=processed_blob)
            data = json.loads(bc.download_blob().readall())
        else:
            print(f"Loading segments from local {processed_file_path}...")
            with open(processed_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

        raw = data if isinstance(data, list) else data.get('segments', [])
        if not raw:
            raise ValueError(f"No segments found in processed JSON")

        # Normalise both processed.json (start_time/end_time/transcript) and
        # transcript.json (start/end/text) to a common shape.
        segments = [
            {
                "start_time": s.get("start_time") if s.get("start_time") is not None else s.get("start", 0),
                "end_time":   s.get("end_time")   if s.get("end_time")   is not None else s.get("end",   0),
                "transcript": s.get("transcript") or s.get("text", ""),
                "speaker":    s.get("speaker", "Unknown"),
            }
            for s in raw
        ]

        print(f"Loaded {len(segments)} segments")

        # STEP 2: Build auth headers — auto-exchange refresh tokens for access tokens
        ls_url = ls_url.rstrip("/")

        access_token = _resolve_token(api_key, ls_url)
        headers = _ls_headers(access_token)

        # Verify connection
        r = _req.get(f"{ls_url}/api/current-user/whoami", headers=headers, timeout=15)
        r.raise_for_status()
        print(f"Connected as: {r.json().get('email', 'unknown')}")

        # STEP 3: Transcode to MP3 and get SAS URL (browsers can't reliably play m4a/wav)
        print(f"Transcoding audio to MP3 for {client_code}/{original_filename}...")
        sas_url = generate_mp3_sas_url(original_filename, client_code)
        print(f"MP3 SAS URL ready")

        # STEP 4: Build Label Studio task with pre-annotations

        # Load client-specific role labels from clients.json
        clients_file = os.path.join(os.path.dirname(__file__), "clients.json")
        role_labels = ["Speaker 1", "Speaker 2"]  # fallback default
        try:
            with open(clients_file, 'r', encoding='utf-8') as f:
                clients = json.load(f)
            for entry in clients.values():
                if entry.get("client_code") == client_code:
                    role_labels = entry.get("role_labels", role_labels)
                    break
        except Exception as ce:
            print(f"Could not load role_labels from clients.json: {ce}")

        print(f"Using role labels for {client_code}: {role_labels}")

        # Map first-appearing speaker → role_labels[0], second → role_labels[1]
        result = []
        seen_speakers: list = []
        for seg in segments:
            sp = seg.get('speaker', 'Unknown')
            if sp not in seen_speakers:
                seen_speakers.append(sp)
            if len(seen_speakers) == 2:
                break

        speaker_to_label = {}
        if len(seen_speakers) >= 1:
            speaker_to_label[seen_speakers[0]] = role_labels[0]
        if len(seen_speakers) >= 2:
            speaker_to_label[seen_speakers[1]] = role_labels[1] if len(role_labels) > 1 else role_labels[0]

        for segment in segments:
            speaker_raw = segment.get('speaker', 'Unknown')
            label = speaker_to_label.get(speaker_raw, role_labels[0])

            region_id = str(uuid.uuid4())[:8]

            # 1. Labels (speaker bar on timeline)
            result.append({
                "id": region_id,
                "from_name": "speaker",
                "to_name": "audio",
                "type": "labels",
                "value": {
                    "start": segment["start_time"],
                    "end": segment["end_time"],
                    "labels": [label]
                }
            })

            # 2. perRegion textarea — start/end and id must match parent label region precisely
            result.append({
                "id": region_id,
                "from_name": "transcript",
                "to_name": "audio",
                "type": "textarea",
                "value": {
                    "start": segment["start_time"],
                    "end": segment["end_time"],
                    "text": [segment.get("transcript", "")]
                }
            })

        # STEP 5: Import task with annotations embedded.
        # Embedding annotations in the import payload is the only reliable way
        # to pre-fill perRegion textarea in Label Studio.
        task_payload = {
            "data": {
                "audio": sas_url,
                "filename": original_filename,
                "client_code": client_code,
                "language": language or "en"
            },
            "annotations": [
                {
                    "result": result,
                    "was_cancelled": False,
                    "ground_truth": False
                }
            ]
        }
        
        # Optionally route/assign to specific annotators based on language
        if language and language in LANGUAGE_ANNOTATOR_MAP:
            task_payload["assignees"] = LANGUAGE_ANNOTATOR_MAP[language]

        print(f"Importing task to Label Studio with {len(segments)} segments ({len(result)} annotation regions)...")
        r = _req.post(
            f"{ls_url}/api/projects/{project_id}/import",
            json=[task_payload],
            headers=headers,
            timeout=60
        )
        r.raise_for_status()
        resp = r.json()
        print(f"Import complete. Response: {resp}")

        task_id = None
        try:
            ids = resp.get('task_ids') or resp.get('ids', [])
            task_id = ids[0] if ids else resp.get('id')
        except Exception:
            pass

        print(f"Label Studio upload successful. Task ID: {task_id}, Segments: {len(segments)}")

        return {
            "status": "success",
            "task_id": task_id,
            "segments": len(segments)
        }

    except Exception as e:
        error_msg = f"Error in Label Studio upload: {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}


def generate_sas_url(filename: str, client_code: str) -> str:
    """Return a 30-day secure SAS URL for any file in client-intake container."""
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")

    parts = {}
    for item in connection_string.split(';'):
        if '=' in item:
            key, value = item.split('=', 1)
            parts[key] = value

    account_name = parts.get('AccountName')
    account_key = parts.get('AccountKey')

    if not account_name or not account_key:
        raise ValueError("Invalid AZURE_STORAGE_CONNECTION_STRING: missing AccountName or AccountKey")

    blob_svc = BlobServiceClient.from_connection_string(connection_string)
    intake_cc = blob_svc.get_container_client("client-intake")
    blobs = list(intake_cc.list_blobs(name_starts_with=f"{client_code}/"))
    matching = [b.name for b in blobs if b.name.endswith(filename)]
    
    if not matching:
        raise ValueError(f"No blob found in client-intake matching {client_code}/*{filename}")
    src_blob_name = sorted(matching)[-1]

    return _make_sas_url(account_name, account_key, "client-intake", src_blob_name)


def push_jewelry_to_labelstudio(
    original_filename: str,
    client_code: str,
    predictions: list,
    project_type: str = "jewelry"
) -> dict:
    """Push processed image and pre-annotated bounding boxes to Label Studio."""
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        api_key = os.getenv("LABEL_STUDIO_API_KEY")

        if project_type == "housing":
            project_id = get_client_project_id(client_code, "housing", "LABEL_STUDIO_HOUSING_PROJECT_ID", "5")
            data_field = "image"
        elif project_type == "business":
            project_id = get_client_project_id(client_code, "business", "LABEL_STUDIO_BUSINESS_PROJECT_ID", "6")
            data_field = "document_url"
        else:
            project_id = get_client_project_id(client_code, "jewelry", "LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
            data_field = "image"

        if not ls_url or not api_key or not project_id:
            raise ValueError("Label Studio credentials missing")

        # 1. Resolve Auth Token
        import requests as _req
        access_token = _resolve_token(api_key, ls_url)
        headers = _ls_headers(access_token)

        # 2. Generate secure image URL
        image_sas = generate_sas_url(original_filename, client_code)

        # 3. Compile Polygon annotations
        results = []
        for pred in predictions:
            region_id = str(uuid.uuid4())[:8]
            
            # If explicit polygon points are supplied, use them directly
            if "points" in pred:
                points = pred["points"]
            else:
                # Convert bounding box [x, y, w, h] into 4-point polygon corners
                bbox = pred.get("bbox", [10, 10, 20, 20])
                x, y, w, h = bbox
                points = [
                    [x, y],
                    [x + w, y],
                    [x + w, y + h],
                    [x, y + h]
                ]

            results.append({
                "id": region_id,
                "from_name": "label",
                "to_name": "image" if project_type != "business" else "document",
                "type": "polygonlabels",
                "value": {
                    "points": points,
                    "polygonlabels": [pred.get("class", "Jewelry" if project_type == "jewelry" else ("business_signage" if project_type == "business" else "Main Structure"))]
                }
            })

        task_payload = {
            "data": {
                data_field: image_sas,
                "filename": original_filename,
                "client_code": client_code,
            }
        }

        if results:
            task_payload["annotations"] = [
                {
                    "result": results,
                    "was_cancelled": False,
                    "ground_truth": False
                }
            ]

        r = _req.post(f"{ls_url}/api/projects/{project_id}/import", json=[task_payload], headers=headers, timeout=30)
        r.raise_for_status()
        resp = r.json()
        ids = resp.get('task_ids') or resp.get('ids', [])
        task_id = ids[0] if ids else resp.get('id')

        return {"status": "success", "task_id": task_id, "predictions_count": len(predictions)}

    except Exception as e:
        error_msg = f"Error pushing image to Label Studio ({project_type}): {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}


def push_form_to_labelstudio(
    original_filename: str,
    client_code: str,
    anonymized_ocr_text: str,
) -> dict:
    """Push processed secure form scans with pre-labeled redacted text."""
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        api_key = os.getenv("LABEL_STUDIO_API_KEY")
        project_id = get_client_project_id(client_code, "form", "LABEL_STUDIO_FORM_PROJECT_ID", "3")

        if not ls_url or not api_key or not project_id:
            raise ValueError("Label Studio credentials missing")

        import requests as _req
        access_token = _resolve_token(api_key, ls_url)
        headers = _ls_headers(access_token)

        # Generate secure document URL
        document_sas = generate_sas_url(original_filename, client_code)

        # Pre-populate redacted text annotation
        region_id = str(uuid.uuid4())[:8]
        results = [
            {
                "id": region_id,
                "from_name": "extracted_text",
                "to_name": "document",
                "type": "textarea",
                "value": {
                    "text": [anonymized_ocr_text]
                }
            }
        ]

        task_payload = {
            "data": {
                "document_url": document_sas,
                "filename": original_filename,
                "client_code": client_code,
                "anonymized_ocr_text": anonymized_ocr_text
            },
            "annotations": [
                {
                    "result": results,
                    "was_cancelled": False,
                    "ground_truth": False
                }
            ]
        }

        r = _req.post(f"{ls_url}/api/projects/{project_id}/import", json=[task_payload], headers=headers, timeout=30)
        r.raise_for_status()
        resp = r.json()
        ids = resp.get('task_ids') or resp.get('ids', [])
        task_id = ids[0] if ids else resp.get('id')

        return {"status": "success", "task_id": task_id}

    except Exception as e:
        error_msg = f"Error pushing form to Label Studio: {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}


def push_clickstream_to_labelstudio(
    original_filename: str,
    client_code: str,
    clickstream_data: list,
) -> dict:
    """Push clickstream sessions to Label Studio — one task per CT_SESSION_ID."""
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        api_key = os.getenv("LABEL_STUDIO_API_KEY")
        project_id = get_client_project_id(client_code, "clickstream", "LABEL_STUDIO_CLICKSTREAM_PROJECT_ID", "4")

        if not ls_url or not api_key or not project_id:
            raise ValueError("Label Studio credentials missing")

        import requests as _req
        access_token = _resolve_token(api_key, ls_url)
        headers = _ls_headers(access_token)

        # Backward compat: flat event list → wrap into single session
        if clickstream_data and isinstance(clickstream_data[0], dict) and "session_id" not in clickstream_data[0]:
            clickstream_data = [{
                "session_id": "LEGACY",
                "event_count": len(clickstream_data),
                "device": "Unknown", "user_type": "Unknown",
                "app_version": "", "platform": "Mobile",
                "friction_signals": [], "session_status": "Smooth Journey",
                "events": [{"action": f"[{i+1}] {ev.get('action','Event')}", "element": ev.get('element',''), "friction": ev.get('friction','')} for i, ev in enumerate(clickstream_data)]
            }]

        task_payloads = []
        for session in clickstream_data:
            session_id     = session.get("session_id", "N/A")
            event_count    = session.get("event_count", 0)
            device         = session.get("device", "Unknown")
            user_type      = session.get("user_type", "Unknown")
            app_version    = session.get("app_version", "")
            platform       = session.get("platform", "Mobile")
            friction_signals = session.get("friction_signals", [])
            session_status = session.get("session_status", "Smooth Journey")
            events         = session.get("events", [])

            # Session summary as first paragraph entry
            friction_label = f">> {', '.join(friction_signals)}" if friction_signals else ">> No friction detected"
            summary_entry = {
                "action": "SESSION SUMMARY",
                "element": f"ID: {session_id}  Device: {device}  App: {app_version}  User: {user_type}  Platform: {platform}  Events: {event_count}  {friction_label}"
            }
            timeline = [summary_entry] + [{"action": ev["action"], "element": ev["element"]} for ev in events]

            # Pre-annotations
            results = [{
                "id": str(uuid.uuid4())[:8],
                "from_name": "session_status",
                "to_name": "filename",
                "type": "choices",
                "value": {"choices": [session_status]}
            }]
            if friction_signals:
                results.append({
                    "id": str(uuid.uuid4())[:8],
                    "from_name": "friction_types",
                    "to_name": "filename",
                    "type": "choices",
                    "value": {"choices": friction_signals}
                })
            ai_note = (
                f"AI Pre-analysis: {', '.join(friction_signals)} ({event_count} events)."
                if friction_signals
                else f"Smooth session — {event_count} events, no friction detected."
            )
            results.append({
                "id": str(uuid.uuid4())[:8],
                "from_name": "summary",
                "to_name": "filename",
                "type": "textarea",
                "value": {"text": [ai_note]}
            })

            # ML training schema: Journey Intent, Outcome, Root Cause, Drop-off Breakpoint
            results.append({
                "id": str(uuid.uuid4())[:8],
                "from_name": "journey_intent",
                "to_name": "filename",
                "type": "choices",
                "value": {"choices": [session.get("journey_intent", "Unknown / Multi-intent")]}
            })
            results.append({
                "id": str(uuid.uuid4())[:8],
                "from_name": "journey_outcome",
                "to_name": "filename",
                "type": "choices",
                "value": {"choices": [session.get("journey_outcome", "Browsing / Inconclusive")]}
            })
            results.append({
                "id": str(uuid.uuid4())[:8],
                "from_name": "root_cause",
                "to_name": "filename",
                "type": "choices",
                "value": {"choices": [session.get("root_cause", "No Issue — Smooth")]}
            })
            bp_idx = session.get("breakpoint_index")
            if bp_idx is not None and bp_idx < len(timeline):
                results.append({
                    "id": str(uuid.uuid4())[:8],
                    "from_name": "breakpoint",
                    "to_name": "timeline",
                    "type": "paragraphlabels",
                    "value": {
                        "start": str(bp_idx),
                        "end": str(bp_idx),
                        "startOffset": 0,
                        "endOffset": 0,
                        "paragraphlabels": ["Drop-off Point"]
                    }
                })

            task_payloads.append({
                "data": {
                    "clickstream_timeline": timeline,
                    "filename": original_filename,
                    "client_code": client_code
                },
                "annotations": [{"result": results, "was_cancelled": False, "ground_truth": False}]
            })

        r = _req.post(f"{ls_url}/api/projects/{project_id}/import", json=task_payloads, headers=headers, timeout=30)
        r.raise_for_status()
        resp = r.json()
        ids = resp.get('task_ids') or resp.get('ids', [])
        task_id = ids[0] if ids else resp.get('id')

        return {"status": "success", "task_id": task_id, "sessions_imported": len(task_payloads)}

    except Exception as e:
        error_msg = f"Error pushing clickstream to Label Studio: {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}


def push_text_transcript_to_labelstudio(
    original_filename: str,
    client_code: str,
    segments: list,
    audio_url: str = None
) -> dict:
    """
    Push a text-only transcript (pre-parsed segments) to Label Studio.
    Autodetects and applies local PII/GSTIN redactor rules, then formats the task
    with pre-annotations linked to a placeholder silent audio track.
    """
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        api_key = os.getenv("LABEL_STUDIO_API_KEY")
        project_id = get_client_project_id(client_code, "transcript", "LABEL_STUDIO_TRANSCRIPT_PROJECT_ID", "7")

        if not ls_url or not api_key or not project_id:
            raise ValueError("Label Studio credentials missing")

        from redactor import mask_text_data
        import uuid
        import requests as _req

        access_token = _resolve_token(api_key, ls_url)
        headers = _ls_headers(access_token)

        # 1. Apply local PII scrubbing and role mapping
        clients_file = os.path.join(os.path.dirname(__file__), "clients.json")
        role_labels = ["Speaker 1", "Speaker 2"]
        try:
            with open(clients_file, 'r', encoding='utf-8') as f:
                clients = json.load(f)
            for entry in clients.values():
                if entry.get("client_code") == client_code:
                    role_labels = entry.get("role_labels", role_labels)
                    break
        except Exception:
            pass

        seen_speakers = []
        for seg in segments:
            sp = seg.get('speaker', 'Unknown')
            if sp not in seen_speakers:
                seen_speakers.append(sp)
            if len(seen_speakers) == 2:
                break

        if not segments:
            return {"status": "success", "task_id": None, "segments": 0}

        # Check if we are in Bulk Mode (list of task dicts instead of segments)
        if isinstance(segments[0], dict) and segments[0].get("type") == "bulk_call":
            from processor import get_call_intelligence
            
            all_payloads = []
            for task_data in segments:
                metadata = task_data.get("metadata", {})
                call_segments = task_data.get("segments", [])
                
                # Combine segments into full text for intelligence analysis
                full_transcript = "\n".join([f"{s.get('speaker', 'Unknown')}: {s.get('transcript', '')}" for s in call_segments])
                
                # AI Rate-Limit Safety
                try:
                    import time
                    time.sleep(0.5) # Prevent TPM burst
                    intel = get_call_intelligence(full_transcript)
                except Exception as ai_e:
                    print(f"AI Intelligence skipped for bulk row: {ai_e}")
                    intel = {}
                
                dialogue_data = []
                result = []
                
                # 1. Dialogue Turns
                for i, seg in enumerate(call_segments):
                    sp_raw = seg.get("speaker", "Unknown")
                    tx = seg.get("transcript", "")
                    scrubbed = mask_text_data(tx)
                    
                    label = "Agent"
                    sp_low = sp_raw.lower()
                    if any(x in sp_low for x in ["customer", "user", "client", "borrower"]):
                        label = "Customer"
                    elif any(x in sp_low for x in ["system", "bot", "auto"]):
                        label = "System"
                    
                    dialogue_data.append({"author": sp_raw, "text": scrubbed})
                    result.append({
                        "id": f"p_{i}",
                        "from_name": "labels", "to_name": "dialogue", "type": "paragraphlabels",
                        "value": {"start": str(i), "end": str(i), "paragraphlabels": [label]}
                    })
                
                # Intent
                if intel.get("intent"):
                    result.append({
                        "from_name": "intent", "to_name": "dialogue", "type": "choices",
                        "value": {"choices": [intel["intent"]]}
                    })
                # Mood
                if intel.get("mood"):
                    result.append({
                        "from_name": "customer_mood", "to_name": "dialogue", "type": "choices",
                        "value": {"choices": [intel["mood"]]}
                    })
                # Churn Risk
                if intel.get("churn_risk"):
                    result.append({
                        "from_name": "churn_risk", "to_name": "dialogue", "type": "choices",
                        "value": {"choices": [intel["churn_risk"]]}
                    })
                # Multi-choice tags
                for key, from_name in [
                    ("onboarding_friction", "onboarding_friction"),
                    ("operational_pain", "operational_pain"),
                    ("financial_disputes", "financial_disputes"),
                    ("service_leakage", "service_leakage")
                ]:
                    vals = intel.get(key, [])
                    if vals:
                        result.append({
                            "from_name": from_name, "to_name": "dialogue", "type": "choices",
                            "value": {"choices": vals}
                        })
                # Summary
                if intel.get("summary"):
                    result.append({
                        "from_name": "summary", "to_name": "dialogue", "type": "textarea",
                        "value": {"text": [intel["summary"]]}
                    })

                payload = {
                    "data": {
                        "dialogue": dialogue_data,
                        "call_id": str(metadata.get("call_id", "N/A")),
                        "agent_name": str(metadata.get("agent_name", "N/A")),
                        "call_date": str(metadata.get("call_date", "N/A")),
                        "summary": str(intel.get("summary", "N/A")),
                        "sentiment": str(intel.get("mood", "N/A")),
                        "filename": original_filename,
                        "client_code": client_code
                    },
                    "annotations": [{"result": result}]
                }
                all_payloads.append(payload)
            
            # Import all tasks at once
            r = _req.post(f"{ls_url}/api/projects/{project_id}/import", json=all_payloads, headers=headers, timeout=60)
            r.raise_for_status()
            return {"status": "success", "tasks_created": len(all_payloads)}

        # Legacy Single Task Mode
        from processor import get_call_intelligence
        
        speaker_to_label = {}
        if len(seen_speakers) >= 1:
            speaker_to_label[seen_speakers[0]] = "Agent"
        if len(seen_speakers) >= 2:
            speaker_to_label[seen_speakers[1]] = "Customer"

        result = []
        dialogue_data = []
        
        # Collect full transcript for AI analysis
        full_transcript = "\n".join([f"{s.get('speaker', 'Unknown')}: {s.get('transcript', '')}" for s in segments])
        try:
            intel = get_call_intelligence(full_transcript)
        except Exception as ai_e:
            print(f"AI Intelligence skipped for single task: {ai_e}")
            intel = {}
        
        metadata = {} # Default to empty for single-task legacy mode
        
        for i, segment in enumerate(segments):
            speaker_raw = segment.get('speaker', 'Unknown')
            label = speaker_to_label.get(speaker_raw, "Unknown")
            scrubbed_text = mask_text_data(segment.get("transcript", ""))
            
            dialogue_data.append({"author": label, "text": scrubbed_text})
            result.append({
                "id": f"p_{i}",
                "from_name": "labels", "to_name": "dialogue", "type": "paragraphlabels",
                "value": {"start": str(i), "end": str(i), "paragraphlabels": [label]}
            })

        # Add Intelligence Pre-annotations
        if intel.get("intent"):
            result.append({
                "from_name": "intent", "to_name": "dialogue", "type": "choices",
                "value": {"choices": [intel["intent"]]}
            })
        if intel.get("mood"):
            result.append({
                "from_name": "customer_mood", "to_name": "dialogue", "type": "choices",
                "value": {"choices": [intel["mood"]]}
            })
        for key, from_name in [
            ("onboarding_friction", "onboarding_friction"),
            ("operational_pain", "operational_pain"),
            ("financial_disputes", "financial_disputes"),
            ("service_leakage", "service_leakage")
        ]:
            vals = intel.get(key, [])
            if vals:
                result.append({
                    "from_name": from_name, "to_name": "dialogue", "type": "choices",
                    "value": {"choices": vals}
                })
        if intel.get("summary"):
            result.append({
                "from_name": "summary", "to_name": "dialogue", "type": "textarea",
                "value": {"text": [intel["summary"]]}
            })

        # Final Payload (supports Project 1 and legacy fallbacks)
        task_data = {
            "dialogue": dialogue_data,
            "filename": original_filename,
            "client_code": client_code,
            "call_id": str(metadata.get("call_id", "N/A")),
            "agent_name": str(metadata.get("agent_name", "N/A"))
        }
        
        # Add audio URL if it's an audio project
        if audio_url:
            task_data["audio"] = audio_url

        task_payload = {
            "data": task_data,
            "annotations": [{"result": result}]
        }
        
        r = _req.post(f"{ls_url}/api/projects/{project_id}/import", json=[task_payload], headers=headers, timeout=60)
        r.raise_for_status()
        resp = r.json()
        ids = resp.get('task_ids') or resp.get('ids', [])
        task_id = ids[0] if ids else resp.get('id')

        return {"status": "success", "task_id": task_id, "segments": len(segments)}

    except Exception as e:
        error_msg = f"Error pushing text transcript to Label Studio: {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}


