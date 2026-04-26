import json
import csv
import io
import os
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

try:
    from label_studio_sdk import LabelStudio
except ImportError:
    print("Label Studio SDK not installed. Install with: pip install label-studio-sdk")
    raise


def export_and_deliver(
    client_code: str,
    original_filename: str,
    project_id: str,
    label_id: str = None,
) -> Dict[str, Any]:
    """
    Export completed annotations from Label Studio and deliver to client via Azure Blob.

    Args:
        client_code: Client identifier
        original_filename: Original audio filename
        project_id: Label Studio project ID
        label_id: Unused — kept for compatibility with main.py call signature

    Returns:
        Dict with export results including status, rows_exported, json_filename,
        csv_filename, and delivery_path
    """
    try:
        print(f"Starting export and delivery for {client_code}/{original_filename}")

        # Get credentials
        ls_url = os.getenv("LABEL_STUDIO_URL")
        ls_api_key = os.getenv("LABEL_STUDIO_API_KEY")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not ls_url:
            raise ValueError("LABEL_STUDIO_URL not found in environment")
        if not ls_api_key:
            raise ValueError("LABEL_STUDIO_API_KEY not found in environment")
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")

        # Create temp directory
        base_temp_dir = Path(f"/tmp/vaidikai/{client_code}")
        base_temp_dir.mkdir(parents=True, exist_ok=True)

        # STEP 1: Connect to Label Studio and get labeled tasks (SDK v2.x)
        print(f"Connecting to Label Studio at {ls_url}, project {project_id}...")
        ls = LabelStudio(base_url=ls_url, api_key=ls_api_key)

        print("Fetching tasks...")
        all_tasks = list(ls.tasks.list(project=int(project_id)))
        # Filter to only tasks that have at least one annotation
        tasks = [t for t in all_tasks if len(getattr(t, 'annotations', None) or t.get('annotations', [])) > 0]

        if not tasks:
            print("No labeled tasks found in project")
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No completed annotations found",
            }

        print(f"Retrieved {len(tasks)} labeled task(s) from project")

        # STEP 2: Filter tasks by client_code and filename
        # SDK v2.x returns Task objects; fall back to dict access for safety
        def _task_field(t, *keys):
            for k in keys:
                try:
                    v = t.data.get(k) if hasattr(t, 'data') else t.get("data", {}).get(k)
                    if v is not None:
                        return v
                except Exception:
                    pass
            return None

        matched_tasks = [
            t for t in tasks
            if _task_field(t, "client_code") == client_code
            and _task_field(t, "filename") == original_filename
        ]

        print(f"Tasks matching client_code='{client_code}', filename='{original_filename}': {len(matched_tasks)}")

        if not matched_tasks:
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No tasks matched the given client_code and filename",
            }

        # STEP 3: Extract segments from matched tasks
        print(f"Extracting audio annotation segments for {original_filename}...")
        segments: List[Dict[str, Any]] = []

        for task in matched_tasks:
            raw_annotations = getattr(task, 'annotations', None) or task.get("annotations", [])
            for annotation in raw_annotations:
                result = getattr(annotation, 'result', None) or annotation.get("result", [])

                # Map start_end_key → speaker label and transcript text
                labels_map: Dict[str, str] = {}
                text_map: Dict[str, str] = {}

                for r in result:
                    val = r.get("value", {}) if isinstance(r, dict) else getattr(r, 'value', {})
                    rtype = r.get("type") if isinstance(r, dict) else getattr(r, 'type', '')
                    key = f"{val.get('start', 0):.3f}_{val.get('end', 0):.3f}"
                    if rtype == "labels":
                        labels = val.get("labels", [])
                        labels_map[key] = labels[0] if labels else "Unknown"
                    elif rtype == "textarea":
                        texts = val.get("text", [])
                        text_map[key] = texts[0] if texts else ""

                # Merge speaker + transcript into segments
                for key, speaker in labels_map.items():
                    start, end = map(float, key.split("_"))
                    segments.append({
                        "start_time": start,
                        "end_time": end,
                        "speaker": speaker,
                        "transcript": text_map.get(key, ""),
                    })

        print(f"Extracted {len(segments)} segments total")

        if not segments:
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No annotation segments could be extracted",
            }

        # Sort segments by start_time
        segments.sort(key=lambda s: s["start_time"])

        # STEP 4: Build output filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = Path(original_filename).stem
        json_filename = f"{stem}_annotated_{timestamp}.json"
        csv_filename = f"{stem}_annotated_{timestamp}.csv"

        # STEP 5: Build JSON payload
        exported_at = datetime.now().isoformat()
        json_payload = {
            "client_code": client_code,
            "original_filename": original_filename,
            "exported_at": exported_at,
            "total_segments": len(segments),
            "segments": segments,
        }

        json_path = base_temp_dir / json_filename
        json_path.write_text(json.dumps(json_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON saved to {json_path}")

        # STEP 6: Build CSV
        csv_path = base_temp_dir / csv_filename
        df = pd.DataFrame(segments, columns=["start_time", "end_time", "speaker", "transcript"])
        df.to_csv(csv_path, index=False, encoding="utf-8")
        print(f"CSV saved to {csv_path}")

        # STEP 7: Upload both files to Azure client-delivery/{client_code}/
        print("Uploading to Azure client-delivery container...")
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        delivery_base = f"{client_code}/"

        for local_path, remote_name in [(json_path, json_filename), (csv_path, csv_filename)]:
            blob_name = delivery_base + remote_name
            blob_client = blob_service_client.get_blob_client(
                container="client-delivery", blob=blob_name
            )
            with open(local_path, "rb") as fh:
                blob_client.upload_blob(fh, overwrite=True)
            print(f"Uploaded client-delivery/{blob_name}")
            os.remove(local_path)

        delivery_path = f"client-delivery/{delivery_base}"

        result = {
            "status": "success",
            "rows_exported": len(segments),
            "json_filename": json_filename,
            "csv_filename": csv_filename,
            "delivery_path": delivery_path,
        }

        print(f"Export and delivery complete: {result}")
        return result

    except Exception as e:
        error_msg = f"Error in export and delivery for {client_code}/{original_filename}: {str(e)}"
        print(error_msg)
        return {
            "status": "error",
            "error": error_msg,
            "client_code": client_code,
            "original_filename": original_filename,
        }


def check_annotation_status(
    client_code: str,
    project_id: str,
    original_filename: str = None,
) -> Dict[str, Any]:
    """
    Check the annotation status for a client (or specific file) in Label Studio.

    Args:
        client_code: Client identifier
        project_id: Label Studio project ID
        original_filename: Optional filename to narrow the filter

    Returns:
        Dict with annotation status including total_segments, completed_annotations,
        pending_annotations, and completion_percentage
    """
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL")
        ls_api_key = os.getenv("LABEL_STUDIO_API_KEY")

        if not ls_url:
            raise ValueError("LABEL_STUDIO_URL not found in environment")
        if not ls_api_key:
            raise ValueError("LABEL_STUDIO_API_KEY not found in environment")

        ls = LabelStudio(base_url=ls_url, api_key=ls_api_key)

        print(f"Fetching tasks for project {project_id}...")
        all_tasks = list(ls.tasks.list(project=int(project_id)))

        def _get_data(t, key):
            try:
                return t.data.get(key) if hasattr(t, 'data') else t.get("data", {}).get(key)
            except Exception:
                return None

        # Filter by client_code, and optionally by filename
        filtered_tasks = [
            t for t in all_tasks
            if _get_data(t, "client_code") == client_code
            and (original_filename is None or _get_data(t, "filename") == original_filename)
        ]

        total = len(filtered_tasks)

        # A task is considered labeled if it has at least one completed annotation
        completed = sum(
            1 for t in filtered_tasks
            if len(getattr(t, 'annotations', None) or t.get("annotations", [])) > 0
        )

        status = {
            "client_code": client_code,
            "filename": original_filename,
            "total_segments": total,
            "completed_annotations": completed,
            "pending_annotations": total - completed,
            "completion_percentage": round((completed / total * 100), 1) if total > 0 else 0,
        }

        return status

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "client_code": client_code,
        }
