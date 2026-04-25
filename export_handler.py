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
    import labelbox as lb
except ImportError:
    print("Labelbox SDK not installed. Install with: pip install labelbox")
    raise


def _extract_segments_from_label(label_obj) -> list:
    """
    Extract temporal audio annotation segments from a Labelbox label object.

    Returns a list of dicts with keys: start_time, end_time, speaker, transcript.
    Times are in seconds (converted from Labelbox ms values).
    """
    segments = []
    try:
        for annotation in label_obj.annotations:
            # Get annotation name (should be "speaker")
            ann_name = getattr(annotation, 'name', '')
            if ann_name.lower() != 'speaker':
                continue

            # Get time range (Labelbox stores in ms for audio)
            start_ms = getattr(annotation, 'start', None)
            end_ms = getattr(annotation, 'end', None)
            if start_ms is None:
                # Try frames attribute
                frames = getattr(annotation, 'frames', [])
                if frames:
                    start_ms = frames[0].get('start', 0)
                    end_ms = frames[0].get('end', 0)

            if start_ms is None:
                continue

            # Get speaker answer
            answer = getattr(annotation, 'answer', None)
            speaker = 'Unknown'
            transcript = ''

            if answer:
                speaker = getattr(answer, 'name', '') or (answer.get('name', '') if isinstance(answer, dict) else '')
                # Extract nested transcript classification
                sub_cls = getattr(answer, 'classifications', []) or (answer.get('classifications', []) if isinstance(answer, dict) else [])
                for cls in sub_cls:
                    cls_name = getattr(cls, 'name', '') or (cls.get('name', '') if isinstance(cls, dict) else '')
                    if cls_name.lower() == 'transcript':
                        cls_ans = getattr(cls, 'answer', '') or (cls.get('answer', '') if isinstance(cls, dict) else '')
                        transcript = str(cls_ans) if cls_ans else ''

            segments.append({
                'start_time': start_ms / 1000.0,  # ms to seconds
                'end_time': end_ms / 1000.0,
                'speaker': speaker,
                'transcript': transcript,
            })
    except Exception as e:
        print(f"Error extracting segments: {e}")
    return segments


def export_and_deliver(
    client_code: str,
    original_filename: str,
    labelbox_project_id: str,
    label_id: str = None,
) -> Dict[str, Any]:
    """
    Export completed annotations from Labelbox and deliver to client via Azure Blob.

    Args:
        client_code: Client identifier
        original_filename: Original audio filename
        labelbox_project_id: Labelbox project ID
        label_id: Optional specific label ID to filter results

    Returns:
        Dict with export results including status, rows_exported, json_filename,
        csv_filename, and delivery_path
    """
    try:
        print(f"Starting export and delivery for {client_code}/{original_filename}")

        # Get credentials
        api_key = os.getenv("LABELBOX_API_KEY")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not api_key:
            raise ValueError("LABELBOX_API_KEY not found in environment")
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")

        # Create temp directory
        base_temp_dir = Path(f"/tmp/vaidikai/{client_code}")
        base_temp_dir.mkdir(parents=True, exist_ok=True)

        # STEP 1: Export from Labelbox
        print(f"Exporting annotations from Labelbox project {labelbox_project_id}...")
        lb_client = lb.Client(api_key=api_key)
        project = lb_client.get_project(labelbox_project_id)

        # Build export params — always request label_details so we can inspect annotations
        export_params: Dict[str, Any] = {
            "data_row_details": True,
            "label_details": True,
            "performance_details": True,
            "attachments": True,
        }

        # Do NOT pass a global_key filter here — Labelbox requires exact key matches,
        # not prefix matches. We filter in Python after fetching all results.
        export_task = project.export_v2(**export_params)

        print("Export task created. Waiting for completion...")
        export_task.wait_for_completion(sleep_time=5)

        if export_task.status == "FAILED":
            raise ValueError("Export task failed")

        print("Export completed successfully")

        # Get export results
        export_results = export_task.get_labels()

        if not export_results:
            print("No labels found in export results")
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No completed annotations found",
            }

        print(f"Retrieved {len(export_results)} labels from export")

        # STEP 2: Filter labels
        # If a specific label_id was requested, narrow to that one label.
        if label_id:
            export_results = [l for l in export_results if str(getattr(l, 'uid', '') or getattr(l, 'id', '')) == str(label_id)]
            print(f"Filtered to label_id={label_id}: {len(export_results)} result(s)")

        # Filter by file prefix using data_row.global_key
        file_prefix = f"{client_code}_{original_filename}_"
        matched_labels = []
        for label in export_results:
            try:
                data_row = label.data_row
                global_key = data_row.global_key or ""
                if global_key.startswith(file_prefix):
                    matched_labels.append(label)
            except Exception as e:
                print(f"Error reading data_row for label: {e}")
                continue

        print(f"Labels matching prefix '{file_prefix}': {len(matched_labels)}")

        if not matched_labels:
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No labels matched the file prefix",
            }

        # STEP 3: Extract segments from matched labels
        print(f"Extracting temporal audio segments for {original_filename}...")
        all_segments: List[Dict[str, Any]] = []

        for label in matched_labels:
            segments = _extract_segments_from_label(label)
            all_segments.extend(segments)

        print(f"Extracted {len(all_segments)} segments total")

        if not all_segments:
            return {
                "status": "success",
                "rows_exported": 0,
                "json_filename": "",
                "csv_filename": "",
                "delivery_path": "",
                "client_code": client_code,
                "message": "No annotation segments could be extracted",
            }

        # STEP 4: Build output filenames
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        original_filename_without_ext = Path(original_filename).stem
        json_filename = f"{original_filename_without_ext}_annotated_{timestamp}.json"
        csv_filename = f"{original_filename_without_ext}_annotated_{timestamp}.csv"

        # STEP 5: Build JSON payload
        exported_at = datetime.now().isoformat()
        json_payload = {
            "client_code": client_code,
            "original_filename": original_filename,
            "exported_at": exported_at,
            "total_segments": len(all_segments),
            "segments": all_segments,
        }

        json_path = base_temp_dir / json_filename
        json_path.write_text(json.dumps(json_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON saved to {json_path}")

        # STEP 6: Build CSV
        csv_path = base_temp_dir / csv_filename
        df = pd.DataFrame(all_segments, columns=["start_time", "end_time", "speaker", "transcript"])
        df = df.sort_values("start_time").reset_index(drop=True)
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
            "rows_exported": len(all_segments),
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

def check_annotation_status(client_code: str, labelbox_project_id: str, original_filename: str = None) -> Dict[str, Any]:
    """
    Check the annotation status for a client or a specific file in Labelbox.
    
    Args:
        client_code: Client identifier
        labelbox_project_id: Labelbox project ID
        original_filename: Optional original filename to filter by
    
    Returns:
        Dict with annotation status
    """
    try:
        api_key = os.getenv("LABELBOX_API_KEY")
        if not api_key:
            raise ValueError("LABELBOX_API_KEY not found in environment")
        
        client = lb.Client(api_key=api_key)
        project = client.get_project(labelbox_project_id)
        
        # Get all data rows for this client
        data_rows = list(project.data_rows())
        
        prefix = f"{client_code}_"
        if original_filename:
            prefix = f"{client_code}_{original_filename}_"
            
        client_rows = []
        for row in data_rows:
            if hasattr(row, 'global_key') and row.global_key.startswith(prefix):
                client_rows.append(row)
        
        total_rows = len(client_rows)
        labeled_rows = 0
        
        for row in client_rows:
            # In newer SDKs, data_row.labels() is the way
            labels = list(row.labels())
            if labels:
                labeled_rows += 1
        
        status = {
            "client_code": client_code,
            "filename": original_filename,
            "total_segments": total_rows,
            "completed_annotations": labeled_rows,
            "pending_annotations": total_rows - labeled_rows,
            "completion_percentage": round((labeled_rows / total_rows * 100), 1) if total_rows > 0 else 0
        }
        
        return status
        
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "client_code": client_code
        }
