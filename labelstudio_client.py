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


def generate_mp3_sas_url(filename: str, client_code: str) -> str:
    """
    Download the original audio from Azure, transcode to MP3, upload to
    the 'processing' container, and return a 30-day SAS URL for the MP3.
    Browsers need MP3; m4a/wav playback is unreliable in Label Studio.
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

    ext = os.path.splitext(src_blob_name)[1]
    mp3_blob_name = src_blob_name.replace(ext, ".mp3")

    mp3_blob_client = blob_svc.get_blob_client(container="processing", blob=mp3_blob_name)

    if not mp3_blob_client.exists():
        print(f"Transcoding {src_blob_name} → MP3 for Label Studio...")
        audio_data = blob_svc.get_blob_client(container="client-intake", blob=src_blob_name).download_blob().readall()
        mp3_data = _transcode_to_mp3(audio_data, ext)
        mp3_blob_client.upload_blob(
            mp3_data, overwrite=True,
            content_settings=ContentSettings(content_type="audio/mpeg")
        )
        print(f"Uploaded MP3 to processing/{mp3_blob_name}")
    else:
        print(f"Reusing existing MP3 at processing/{mp3_blob_name}")

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name="processing",
        blob_name=mp3_blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=30),
    )

    return f"{mp3_blob_client.url}?{sas_token}"


def push_to_labelstudio(
    processed_file_path: str,
    client_code: str,
    original_filename: str,
    processed_blob: str = None,
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
        project_id = os.getenv("LABEL_STUDIO_PROJECT_ID")
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

        segments = data if isinstance(data, list) else data.get('segments', [])

        if not segments:
            raise ValueError(f"No segments found in processed JSON")

        print(f"Loaded {len(segments)} segments")

        # STEP 2: Connect to Label Studio (SDK v2.x)
        print(f"Connecting to Label Studio at {ls_url}...")
        ls_client = LabelStudio(base_url=ls_url, api_key=api_key)
        me = ls_client.users.whoami()
        print(f"Connected as: {me.email}")

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

            # 2. perRegion textarea — parentID links to region, start/end must match parent
            result.append({
                "id": region_id + "_t",
                "from_name": "transcript",
                "to_name": "audio",
                "type": "textarea",
                "parentID": region_id,
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
            },
            "annotations": [
                {
                    "result": result,
                    "was_cancelled": False,
                    "ground_truth": False
                }
            ]
        }

        print(f"Importing task to Label Studio with {len(segments)} segments ({len(result)} annotation regions)...")
        imported = ls_client.projects.import_tasks(id=int(project_id), request=[task_payload])
        print(f"Import complete. Response: {imported}")

        task_id = None
        try:
            ids = getattr(imported, 'task_ids', None) or getattr(imported, 'ids', None)
            if ids:
                task_id = ids[0]
            elif isinstance(imported, dict):
                ids = imported.get('task_ids') or imported.get('ids', [])
                task_id = ids[0] if ids else imported.get('id')
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
