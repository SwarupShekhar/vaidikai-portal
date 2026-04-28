import json
import os
import io
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

load_dotenv()

def _get_ls_headers() -> dict:
    """Helper to get authenticated headers for Label Studio API."""
    ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
    api_key = os.getenv("LABEL_STUDIO_API_KEY", "")
    
    if not ls_url or not api_key:
        raise ValueError("LABEL_STUDIO_URL or LABEL_STUDIO_API_KEY not found in environment")

    # If it's a refresh token (JWT starting with eyJ), exchange it
    if api_key.startswith("eyJ"):
        import base64
        try:
            payload = api_key.split(".")[1]
            payload += "=" * (4 - len(payload) % 4)
            claims = json.loads(base64.b64decode(payload))
            if claims.get("token_type") == "refresh":
                r = requests.post(f"{ls_url}/api/token/refresh", json={"refresh": api_key}, timeout=15)
                r.raise_for_status()
                access_token = r.json()["access"]
                return {"Authorization": f"Bearer {access_token}"}
        except Exception as e:
            print(f"Token refresh failed, using as-is: {e}")
            
    return {"Authorization": f"Bearer {api_key}"}

def export_and_deliver(
    client_code: str,
    original_filename: str,
    project_id: str,
    label_id: str = None,
    task_id: int = None,
) -> Dict[str, Any]:
    """
    Export completed annotations from Label Studio and deliver to client via Azure Blob as XLSX.
    """
    try:
        print(f"Starting XLSX export and delivery for {client_code}/{original_filename}")

        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING not found in environment")

        headers = _get_ls_headers()

        # STEP 1: Fetch Task(s)
        matched_tasks = []
        if task_id:
            print(f"Fetching specific task ID: {task_id}")
            r = requests.get(f"{ls_url}/api/tasks/{task_id}", headers=headers, timeout=15)
            if r.status_code == 200:
                matched_tasks = [r.json()]
        
        if not matched_tasks:
            print(f"Fetching tasks for project {project_id} and filtering...")
            r = requests.get(
                f"{ls_url}/api/tasks",
                params={"project": project_id, "page_size": 1000},
                headers=headers,
                timeout=30
            )
            r.raise_for_status()
            resp = r.json()
            all_tasks = resp if isinstance(resp, list) else resp.get("tasks", [])

            # The list endpoint does NOT include annotations — only IDs.
            # Find matching tasks by client_code/filename first, then fetch
            # each individually to get full annotation data.
            candidate_ids = [
                t["id"] for t in all_tasks
                if t.get("data", {}).get("client_code") == client_code
                and t.get("data", {}).get("filename") == original_filename
                and t.get("total_annotations", 0) > 0
            ]
            print(f"Found {len(candidate_ids)} candidate task IDs: {candidate_ids}")

            for tid in candidate_ids:
                tr = requests.get(f"{ls_url}/api/tasks/{tid}", headers=headers, timeout=15)
                if tr.status_code == 200:
                    full_task = tr.json()
                    if len(full_task.get("annotations", [])) > 0:
                        matched_tasks.append(full_task)

        if not matched_tasks:
            print(f"No annotated tasks found for {client_code}/{original_filename}")
            return {
                "status": "error",
                "message": f"No completed annotations found for {original_filename}",
                "client_code": client_code
            }

        print(f"Found {len(matched_tasks)} annotated task(s)")

        segments: List[Dict[str, Any]] = []
        annotators = set()
        languages = set()

        for task in matched_tasks:
            task_data = task.get("data", {})
            task_lang = task_data.get("language", "Unknown")
            languages.add(task_lang)
            
            for annotation in task.get("annotations", []):
                if annotation.get("was_cancelled"):
                    continue
                
                result = annotation.get("result", [])

                # Manual review status check for Community Edition
                manual_status = "Pending"
                for r_item in result:
                    if r_item.get("from_name") == "review_status":
                        manual_status = r_item.get("value", {}).get("choices", ["Pending"])[0]
                        break
                
                # Check both Enterprise review result and manual choice status
                review_result = annotation.get("review_result")
                task_reviews = task.get("reviews", [])
                is_accepted = (review_result == "accepted") or \
                              (manual_status == "Accepted") or \
                              (len(task_reviews) > 0 and task_reviews[0].get("accepted") is True)
                
                if not is_accepted:
                    continue
                    
                email = annotation.get("created_username") or "Annotator"
                annotators.add(email.split("@")[0] if "@" in email else email)
                
                regions = {}
                for r_item in result:
                    rid = r_item.get("id")
                    pid = r_item.get("parentID")
                    val = r_item.get("value", {})
                    rtype = r_item.get("type")
                    
                    if rtype == "labels":
                        if rid not in regions:
                            regions[rid] = {"start": val.get("start", 0), "end": val.get("end", 0), "speaker": "Unknown", "transcript": "", "language": task_lang}
                        labels = val.get("labels", [])
                        regions[rid]["speaker"] = labels[0] if labels else "Unknown"
                        regions[rid]["start"] = val.get("start", regions[rid]["start"])
                        regions[rid]["end"] = val.get("end", regions[rid]["end"])
                    elif rtype == "textarea":
                        target_id = pid if pid else rid
                        if target_id not in regions:
                            regions[target_id] = {"start": val.get("start", 0), "end": val.get("end", 0), "speaker": "Unknown", "transcript": "", "language": task_lang}
                        texts = val.get("text", [])
                        regions[target_id]["transcript"] = texts[0] if texts else ""

                for rid, data in regions.items():
                    if not data["transcript"].strip():
                        continue
                    duration = round(data["end"] - data["start"], 3)
                    segments.append({
                        "Speaker": data["speaker"],
                        "Start Time (s)": round(data["start"], 3),
                        "End Time (s)": round(data["end"], 3),
                        "Duration (s)": duration,
                        "Transcript": data["transcript"],
                        "Language": data["language"],
                        "Audio File": original_filename
                    })

        segments.sort(key=lambda x: x["Start Time (s)"])
        for i, seg in enumerate(segments):
            seg["Segment #"] = i + 1

        # Reorder columns and rename Transcript to Transcript (Language)
        lang_str = sorted(list(languages))[0] if languages else "Annotated"
        transcript_col = f"Transcript ({lang_str})"
        
        final_segments = []
        for s in segments:
            final_segments.append({
                "Segment #": s["Segment #"],
                "Speaker": s["Speaker"],
                "Start Time (s)": s["Start Time (s)"],
                "End Time (s)": s["End Time (s)"],
                "Duration (s)": s["Duration (s)"],
                transcript_col: s["Transcript"],
                "Language": s["Language"],
                "Audio File": s["Audio File"]
            })

        columns = ["Segment #", "Speaker", "Start Time (s)", "End Time (s)", "Duration (s)", transcript_col, "Language", "Audio File"]
        df_transcript = pd.DataFrame(final_segments)[columns]

        # STEP 2: Build Summary Data
        total_duration_secs = sum(s["Duration (s)"] for s in segments)
        unique_speakers = sorted(list(set(s["Speaker"] for s in segments)))
        
        summary_headers = ["Language", "Total Segments", "Total Duration (mins)", "Speakers", "Annotated By", "Export Date"]
        summary_values = [
            ", ".join(sorted(list(languages))),
            len(segments),
            round(total_duration_secs / 60, 2),
            ", ".join(unique_speakers),
            ", ".join(sorted(list(annotators))) or "Vaidik AI",
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ]

        # STEP 3: Create XLSX with premium formatting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = Path(original_filename).stem
        xlsx_filename = f"{stem}_annotated_{timestamp}.xlsx"
        xlsx_path = f"/tmp/vaidikai/{client_code}/{xlsx_filename}"
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)

        wb = Workbook()
        ws1 = wb.active
        ws1.title = "Transcript - Annotated"
        
        HEADER_FILL = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        HEADER_FONT = Font(name="Arial", size=10, bold=True, color="FFFFFF")
        BODY_FONT = Font(name="Arial", size=10)
        ALTERNATE_FILL = PatternFill(start_color="EBF3FB", end_color="EBF3FB", fill_type="solid")
        ALIGN_CENTER = Alignment(horizontal="center", vertical="center")
        ALIGN_LEFT = Alignment(horizontal="left", vertical="top", wrap_text=True)

        for col_num, header in enumerate(columns, 1):
            cell = ws1.cell(row=1, column=col_num, value=header)
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT
            cell.alignment = ALIGN_CENTER

        for row_num, row_data in enumerate(df_transcript.values, 2):
            is_alternate = row_num % 2 != 0
            for col_num, value in enumerate(row_data, 1):
                cell = ws1.cell(row=row_num, column=col_num, value=value)
                cell.font = BODY_FONT
                if is_alternate:
                    cell.fill = ALTERNATE_FILL
                if columns[col_num-1] in ["Segment #", "Start Time (s)", "End Time (s)", "Duration (s)"]:
                    cell.alignment = ALIGN_CENTER
                else:
                    cell.alignment = ALIGN_LEFT

        widths = {"Segment #": 10, "Speaker": 15, "Start Time (s)": 15, "End Time (s)": 15, "Duration (s)": 15, "Transcript": 60, "Language": 12, "Audio File": 30}
        for col_num, header in enumerate(columns, 1):
            ws1.column_dimensions[get_column_letter(col_num)].width = widths.get(header, 15)

        ws2 = wb.create_sheet(title="Summary")
        for col_num, header in enumerate(summary_headers, 1):
            cell = ws2.cell(row=1, column=col_num, value=header)
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT
            cell.alignment = ALIGN_CENTER
        
        for col_num, value in enumerate(summary_values, 1):
            cell = ws2.cell(row=2, column=col_num, value=value)
            cell.font = BODY_FONT
            if summary_headers[col_num-1] in ["Speakers", "Annotated By"]:
                cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
            else:
                cell.alignment = ALIGN_LEFT

        for col_num in range(1, len(summary_headers) + 1):
            ws2.column_dimensions[get_column_letter(col_num)].width = 25

        wb.save(xlsx_path)
        print(f"XLSX saved to {xlsx_path}")

        # STEP 4: Upload to Azure
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        delivery_blob_name = f"{client_code}/{xlsx_filename}"
        blob_client = blob_service_client.get_blob_client(container="client-delivery", blob=delivery_blob_name)
        
        with open(xlsx_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded to client-delivery/{delivery_blob_name}")
        os.remove(xlsx_path)
        try:
            parent_dir = os.path.dirname(xlsx_path)
            if not os.listdir(parent_dir):
                os.rmdir(parent_dir)
        except Exception:
            pass

        return {
            "status": "success",
            "rows_exported": len(segments),
            "xlsx_filename": xlsx_filename,
            "delivery_path": f"client-delivery/{client_code}/",
            "message": f"Successfully exported {len(segments)} segments to XLSX"
        }

    except Exception as e:
        error_msg = f"Error in export and delivery: {str(e)}"
        print(error_msg)
        return {
            "status": "error",
            "error": error_msg,
            "client_code": client_code,
            "original_filename": original_filename
        }

def check_annotation_status(
    client_code: str,
    project_id: str,
    original_filename: str = None,
) -> Dict[str, Any]:
    """
    Check the annotation status for a client (or specific file) in Label Studio.
    """
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        headers = _get_ls_headers()

        print(f"Checking status for project {project_id}, client {client_code}...")
        r = requests.get(
            f"{ls_url}/api/tasks",
            params={"project": project_id, "page_size": 1000},
            headers=headers,
            timeout=30
        )
        r.raise_for_status()
        resp = r.json()
        all_tasks = resp if isinstance(resp, list) else resp.get("tasks", [])

        def _get_data(t, key):
            return t.get("data", {}).get(key)

        filtered_tasks = [
            t for t in all_tasks
            if _get_data(t, "client_code") == client_code
            and (original_filename is None or _get_data(t, "filename") == original_filename)
        ]

        total = len(filtered_tasks)
        completed = sum(1 for t in filtered_tasks if t.get("total_annotations", 0) > 0)

        return {
            "client_code": client_code,
            "filename": original_filename,
            "total_segments": total,
            "completed_annotations": completed,
            "pending_annotations": total - completed,
            "completion_percentage": round((completed / total * 100), 1) if total > 0 else 0,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "client_code": client_code,
        }
