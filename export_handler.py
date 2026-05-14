import json
import os
import io
import time
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

import cv2
import numpy as np
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from collateral_detector import generate_signature, find_duplicates, store_signatures
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
    force_delivery: bool = False,
    internal_export: bool = False,
) -> Dict[str, Any]:
    """
    Export completed annotations from Label Studio and deliver to client via Azure Blob.
    For jewelry projects, generates a Gold Standard Bundle (.zip) with audit images,
    COCO JSON, and runs duplicate collateral detection before delivery.
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
            project_ids_to_check = [str(project_id)]
            for pid in ["1", "2", "3", "4", "5", "6"]:
                if pid not in project_ids_to_check:
                    project_ids_to_check.append(pid)

            for pid in project_ids_to_check:
                try:
                    print(f"Fetching tasks for project {pid} and filtering...")
                    r = requests.get(
                        f"{ls_url}/api/tasks",
                        params={"project": pid, "page_size": 1000},
                        headers=headers,
                        timeout=15
                    )
                    if r.status_code == 200:
                        resp = r.json()
                        all_tasks = resp if isinstance(resp, list) else resp.get("tasks", [])
                        candidate_ids = [
                            t["id"] for t in all_tasks
                            if t.get("data", {}).get("client_code") == client_code
                            and t.get("data", {}).get("filename") == original_filename
                            and t.get("total_annotations", 0) > 0
                        ]
                        if candidate_ids:
                            print(f"Found {len(candidate_ids)} candidate task IDs in project {pid}: {candidate_ids}")
                            project_id = pid
                            for tid in candidate_ids:
                                tr = requests.get(f"{ls_url}/api/tasks/{tid}", headers=headers, timeout=15)
                                if tr.status_code == 200:
                                    full_task = tr.json()
                                    if len(full_task.get("annotations", [])) > 0:
                                        matched_tasks.append(full_task)
                            break
                except Exception as ex:
                    print(f"Error checking project {pid}: {ex}")

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

                # For Audio Project, enforce manual review status. For Jewelry, accept all non-cancelled.
                is_jewelry = str(project_id) == os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
                is_housing = str(project_id) == os.getenv("LABEL_STUDIO_HOUSING_PROJECT_ID", "5")
                is_business = str(project_id) == os.getenv("LABEL_STUDIO_BUSINESS_PROJECT_ID", "6")
                is_image_project = is_jewelry or is_housing or is_business
                
                if not is_image_project:
                    manual_status = "Pending"
                    for r_item in result:
                        if r_item.get("from_name") == "review_status":
                            manual_status = r_item.get("value", {}).get("choices", ["Pending"])[0]
                            break
                    review_result = annotation.get("review_result")
                    task_reviews = task.get("reviews", [])
                    is_accepted = (review_result == "accepted") or \
                                  (manual_status == "Accepted") or \
                                  (len(task_reviews) > 0 and task_reviews[0].get("accepted") is True)
                    if not is_accepted:
                        continue
                    
                email = annotation.get("created_username") or "Annotator"
                annotators.add(email.split("@")[0] if "@" in email else email)
                
                if is_image_project:
                    for r_item in result:
                        val = r_item.get("value", {})
                        rtype = r_item.get("type", "")
                        if rtype in ("polygonlabels", "keypointlabels", "rectanglelabels"):
                            default_cat = "General Jewelry"
                            if is_housing:
                                default_cat = "General House"
                            elif is_business:
                                default_cat = "General Business"
                                
                            labels = val.get(rtype, [default_cat])
                            category = labels[0] if labels else default_cat
                            points = val.get("points", [])
                            points_count = len(points) if rtype == "polygonlabels" else 1
                            segments.append({
                                "Category": category,
                                "Geometry Type": rtype.replace("labels", "").capitalize(),
                                "Points Count": points_count,
                                "Annotator": email.split("@")[0] if "@" in email else email,
                                "Image File": original_filename,
                                "Points": points,
                                "RType": rtype
                            })
                else:
                    regions = {}
                    for r_item in result:
                        rid = r_item.get("id")
                        pid = r_item.get("parentID")
                        val = r_item.get("value", {})
                        rtype = r_item.get("type")

                        target_id = pid if pid else rid
                        if target_id.endswith("_t"):
                            target_id = target_id[:-2]

                        if target_id not in regions:
                            regions[target_id] = {
                                "start": val.get("start", 0), 
                                "end": val.get("end", 0), 
                                "speaker": "Unknown", 
                                "transcript": "", 
                                "language": task_lang
                            }
                        
                        if rtype == "labels":
                            labels = val.get("labels", [])
                            regions[target_id]["speaker"] = labels[0] if labels else "Unknown"
                            regions[target_id]["start"] = val.get("start", regions[target_id]["start"])
                            regions[target_id]["end"] = val.get("end", regions[target_id]["end"])
                        elif rtype == "textarea":
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

        is_jewelry = str(project_id) == os.getenv("LABEL_STUDIO_JEWELRY_PROJECT_ID", "2")
        is_housing = str(project_id) == os.getenv("LABEL_STUDIO_HOUSING_PROJECT_ID", "5")
        is_business = str(project_id) == os.getenv("LABEL_STUDIO_BUSINESS_PROJECT_ID", "6")
        is_image_project = is_jewelry or is_housing or is_business
        
        final_segments = []
        if is_image_project:
            for i, seg in enumerate(segments):
                seg["Item #"] = i + 1
                row_data = {
                    "Item #": seg["Item #"],
                    "Category": seg["Category"],
                    "Geometry Type": seg["Geometry Type"],
                    "Points Count": seg["Points Count"],
                    "Image File": seg["Image File"]
                }
                if internal_export:
                    row_data["Annotator"] = seg["Annotator"]
                final_segments.append(row_data)

            if internal_export:
                columns = ["Item #", "Category", "Geometry Type", "Points Count", "Annotator", "Image File"]
            else:
                columns = ["Item #", "Category", "Geometry Type", "Points Count", "Image File"]
            
            # Summary Configuration
            category_counts = {}
            for s in segments:
                category_counts[s["Category"]] = category_counts.get(s["Category"], 0) + 1
                
            overview_title = "Inventory Overview"
            if is_housing:
                overview_title = "Housing Validation Overview"
            elif is_business:
                overview_title = "MSME Business Overview"
                
            summary_headers = [overview_title, "Total Count"]
            summary_values_list = []
            for cat, count in sorted(category_counts.items()):
                summary_values_list.append([cat, count])
            summary_values_list.append(["Total Items", len(segments)])
            
            annotators_str = ", ".join(sorted(list(annotators))) or "Vaidik AI" if internal_export else "Vaidik AI"
            summary_values_list.append(["Annotated By", annotators_str])
            summary_values_list.append(["Export Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            
        else:
            segments.sort(key=lambda x: x.get("Start Time (s)", 0))
            for i, seg in enumerate(segments):
                seg["Segment #"] = i + 1

            lang_str = sorted(list(languages))[0] if languages else "Annotated"
            transcript_col = f"Transcript ({lang_str})"
            
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
            
            # Audio Summary
            total_duration_secs = sum(s["Duration (s)"] for s in segments)
            unique_speakers = sorted(list(set(s["Speaker"] for s in segments)))
            summary_headers = ["Language", "Total Segments", "Total Duration (mins)", "Speakers", "Annotated By", "Export Date"]
            summary_values_list = [[
                ", ".join(sorted(list(languages))),
                len(segments),
                round(total_duration_secs / 60, 2),
                ", ".join(unique_speakers),
                ", ".join(sorted(list(annotators))) or "Vaidik AI" if internal_export else "Vaidik AI",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ]]

        df_transcript = pd.DataFrame(final_segments)[columns] if final_segments else pd.DataFrame(columns=columns)

        # STEP 3: Create XLSX with premium formatting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        stem = Path(original_filename).stem
        xlsx_filename = f"{stem}_annotated_{timestamp}.xlsx"
        xlsx_path = f"/tmp/vaidikai/{client_code}/{xlsx_filename}"
        os.makedirs(os.path.dirname(xlsx_path), exist_ok=True)

        wb = Workbook()
        ws1 = wb.active
        sheet_title = "Transcript - Annotated"
        if is_image_project:
            sheet_title = "Inventory Data"
            if is_housing:
                sheet_title = "Housing Collateral Data"
            elif is_business:
                sheet_title = "Business Audit Data"
        ws1.title = sheet_title
        
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

        SPEAKER_COLORS = {
            "AI": Font(name="Arial", size=10, bold=True, color="000080"), # Dark Blue
            "Speaker 1": Font(name="Arial", size=10, bold=True, color="006400"), # Dark Green
            "Speaker 2": Font(name="Arial", size=10, bold=True, color="8B0000"), # Dark Red
        }

        for row_num, row_data in enumerate(df_transcript.values, 2):
            is_alternate = row_num % 2 != 0
            for col_num, value in enumerate(row_data, 1):
                cell = ws1.cell(row=row_num, column=col_num, value=value)
                
                # Apply speaker-specific font if this is the Speaker column (col 2)
                if columns[col_num-1] == "Speaker" and value in SPEAKER_COLORS:
                    cell.font = SPEAKER_COLORS[value]
                else:
                    cell.font = BODY_FONT
                
                if is_alternate:
                    cell.fill = ALTERNATE_FILL
                
                if columns[col_num-1] in ["Segment #", "Item #", "Start Time (s)", "End Time (s)", "Duration (s)", "Points Count"]:
                    cell.alignment = ALIGN_CENTER
                else:
                    cell.alignment = ALIGN_LEFT

        widths = {
            "Item #": 10, "Category": 20, "Geometry Type": 15, "Points Count": 12, "Annotator": 20, "Image File": 30,
            "Segment #": 10, "Speaker": 15, "Start Time (s)": 15, "End Time (s)": 15, "Duration (s)": 15, "Transcript": 60, "Language": 12, "Audio File": 30
        }
        for col_num, header in enumerate(columns, 1):
            ws1.column_dimensions[get_column_letter(col_num)].width = widths.get(header, 15)

        ws2 = wb.create_sheet(title="Summary")
        for col_num, header in enumerate(summary_headers, 1):
            cell = ws2.cell(row=1, column=col_num, value=header)
            cell.fill = HEADER_FILL
            cell.font = HEADER_FONT
            cell.alignment = ALIGN_CENTER
        
        for row_idx, row_vals in enumerate(summary_values_list, 2):
            for col_num, value in enumerate(row_vals, 1):
                cell = ws2.cell(row=row_idx, column=col_num, value=value)
                cell.font = BODY_FONT
                if summary_headers[col_num-1] in ["Speakers", "Annotated By"]:
                    cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)
                else:
                    cell.alignment = ALIGN_LEFT

        for col_num in range(1, len(summary_headers) + 1):
            ws2.column_dimensions[get_column_letter(col_num)].width = 25

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        if is_image_project:
            # 1) Setup base dir
            base_dir = f"/tmp/vaidikai/{client_code}/{stem}_export_{timestamp}"
            audit_dir = os.path.join(base_dir, "Audit")
            ml_dir = os.path.join(base_dir, "ML_Training")
            os.makedirs(audit_dir, exist_ok=True)
            os.makedirs(ml_dir, exist_ok=True)
            
            # Save excel to Audit
            xlsx_name_base = "inventory"
            if is_housing:
                xlsx_name_base = "housing_verification"
            elif is_business:
                xlsx_name_base = "business_audit"
            wb.save(os.path.join(audit_dir, f"{stem}_{xlsx_name_base}_{timestamp}.xlsx"))
            
            # 2) Download original image from Azure (dynamically matching timestamp-prefixed blobs)
            intake_cc = blob_service_client.get_container_client("client-intake")
            blobs = list(intake_cc.list_blobs(name_starts_with=f"{client_code}/"))
            matching = [b.name for b in blobs if b.name.endswith(original_filename)]
            
            if not matching:
                raise ValueError(f"No blob found in client-intake matching {client_code}/*{original_filename}")
            src_blob_name = sorted(matching)[-1]
            
            intake_blob_client = blob_service_client.get_blob_client(container="client-intake", blob=src_blob_name)
            img_path = os.path.join(base_dir, f"raw_{original_filename}")
            with open(img_path, "wb") as f:
                f.write(intake_blob_client.download_blob().readall())
            
            img = cv2.imread(img_path)
            if img is None:
                raise ValueError(f"Failed to read image from {img_path}")
            h, w = img.shape[:2]
            
            # Apply dynamic face redaction blurring for borrower or representative faces
            from redactor import redact_faces_in_image
            img = redact_faces_in_image(img, segments)
            
            # ── DUPLICATE COLLATERAL DETECTION ──────────────────────
            # Generate signatures for each image label item
            new_signatures = []
            for item in segments:
                pts = item.get("Points", [])
                if item.get("RType") in ("polygonlabels", "rectanglelabels") and pts:
                    sig = generate_signature(
                        image=img,
                        polygon_points=pts,
                        category=item["Category"],
                        client_code=client_code,
                        task_id=candidate_ids[0] if candidate_ids else 0,
                        item_index=item["Item #"],
                        image_file=original_filename,
                        img_width=w,
                        img_height=h,
                    )
                    if sig:
                        new_signatures.append(sig)
            
            # Check for duplicates against historical database
            if new_signatures and not force_delivery:
                matches = find_duplicates(new_signatures)
                if matches:
                    # Clean up temp files
                    os.remove(img_path)
                    shutil.rmtree(base_dir, ignore_errors=True)
                    
                    return {
                        "status": "duplicate_warning",
                        "matches": matches,
                        "total_matches": len(matches),
                        "message": f"⚠️ COLLATERAL MATCH DETECTED: {len(matches)} item(s) match historical records. Review required before delivery.",
                    }
            
            print(f"[Collateral Detector] No duplicates found. Proceeding with delivery.")
            
            # 3) Generate COCO JSON & draw polygons
            coco = {
                "images": [{"id": 1, "width": w, "height": h, "file_name": original_filename}],
                "annotations": [],
                "categories": []
            }
            cat_map = {}
            
            supercat = "jewelry"
            if is_housing:
                supercat = "housing"
            elif is_business:
                supercat = "msme_business"
                
            for item in segments:
                cat = item["Category"]
                if cat not in cat_map:
                    cat_map[cat] = len(cat_map) + 1
                    coco["categories"].append({"id": cat_map[cat], "name": cat, "supercategory": supercat})
                
                pts = item.get("Points", [])
                if item.get("RType") in ("polygonlabels", "rectanglelabels") and pts:
                    # Draw on image
                    cv_pts = np.array([[int(p[0] * w / 100.0), int(p[1] * h / 100.0)] for p in pts], np.int32)
                    cv_pts = cv_pts.reshape((-1, 1, 2))
                    cv2.polylines(img, [cv_pts], True, (0, 255, 0), 3)
                    
                    # Add label
                    cx, cy = cv_pts[0][0]
                    cv2.putText(img, f"#{item['Item #']} {cat}", (cx, cy-10), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 0), 2)
                    
                    # Add to COCO
                    coco_pts = [p for pt in pts for p in [pt[0] * w / 100.0, pt[1] * h / 100.0]]
                    
                    # Calculate real bounding box dynamically
                    xs = [pt[0] * w / 100.0 for pt in pts]
                    ys = [pt[1] * h / 100.0 for pt in pts]
                    xmin = min(xs)
                    ymin = min(ys)
                    xmax = max(xs)
                    ymax = max(ys)
                    bbox_w = xmax - xmin
                    bbox_h = ymax - ymin
                    bbox = [round(xmin, 2), round(ymin, 2), round(bbox_w, 2), round(bbox_h, 2)]
                    
                    # Shoelace formula for polygon area calculation
                    pixel_pts = [(pt[0] * w / 100.0, pt[1] * h / 100.0) for pt in pts]
                    n = len(pixel_pts)
                    polygon_area = 0.0
                    for idx in range(n):
                        next_idx = (idx + 1) % n
                        polygon_area += pixel_pts[idx][0] * pixel_pts[next_idx][1]
                        polygon_area -= pixel_pts[next_idx][0] * pixel_pts[idx][1]
                    polygon_area = abs(polygon_area) / 2.0
                    
                    coco["annotations"].append({
                        "id": item["Item #"],
                        "image_id": 1,
                        "category_id": cat_map[cat],
                        "segmentation": [coco_pts],
                        "area": round(polygon_area, 2),
                        "bbox": bbox,
                        "iscrowd": 0
                    })
            
            # Save image
            cv2.imwrite(os.path.join(audit_dir, f"{stem}_annotated.jpg"), img)
            os.remove(img_path)
            
            # Save JSON
            with open(os.path.join(ml_dir, f"{stem}_coco.json"), "w") as f:
                json.dump(coco, f, indent=2)
            
            # Zip it all up
            zip_filename = f"{stem}_export_{timestamp}.zip"
            zip_path = f"/tmp/vaidikai/{client_code}/{stem}_export_{timestamp}"
            shutil.make_archive(zip_path, 'zip', base_dir)
            
            # Upload ZIP
            delivery_blob_name = f"{client_code}/{zip_filename}"
            blob_client = blob_service_client.get_blob_client(container="client-delivery", blob=delivery_blob_name)
            with open(zip_path + ".zip", "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
                
            shutil.rmtree(base_dir, ignore_errors=True)
            try:
                os.remove(zip_path + ".zip")
            except:
                pass
                
            # Store signatures after successful delivery
            if new_signatures:
                store_signatures(new_signatures)
            
            return {
                "status": "success",
                "rows_exported": len(segments),
                "xlsx_filename": zip_filename,
                "delivery_path": f"client-delivery/{client_code}/",
                "message": f"Successfully exported Gold Standard Bundle to {zip_filename}"
            }
            
        else:
            wb.save(xlsx_path)
            print(f"XLSX saved to {xlsx_path}")

            # STEP 4: Upload to Azure
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

_TASK_CACHE = {}

def check_annotation_status(
    client_code: str,
    project_id: str,
    original_filename: str = None,
) -> Dict[str, Any]:
    """
    Check the annotation status for a client (or specific file) in Label Studio across all project IDs with 15s TTL caching.
    """
    import time
    global _TASK_CACHE
    try:
        ls_url = os.getenv("LABEL_STUDIO_URL", "").rstrip("/")
        headers = _get_ls_headers()

        project_ids_to_check = [str(project_id)]
        for pid in ["1", "2", "3", "4", "5", "6"]:
            if pid not in project_ids_to_check:
                project_ids_to_check.append(pid)

        filtered_tasks = []
        actual_pid = project_id

        for pid in project_ids_to_check:
            try:
                now = time.time()
                cache_key = f"{ls_url}_{pid}"
                if cache_key in _TASK_CACHE and now - _TASK_CACHE[cache_key]["time"] < 15:
                    all_tasks = _TASK_CACHE[cache_key]["tasks"]
                else:
                    r = requests.get(
                        f"{ls_url}/api/tasks",
                        params={"project": pid, "page_size": 1000},
                        headers=headers,
                        timeout=10
                    )
                    if r.status_code == 200:
                        resp = r.json()
                        all_tasks = resp if isinstance(resp, list) else resp.get("tasks", [])
                        _TASK_CACHE[cache_key] = {"tasks": all_tasks, "time": now}
                    else:
                        all_tasks = []

                tasks_for_file = [
                    t for t in all_tasks
                    if t.get("data", {}).get("client_code") == client_code
                    and (original_filename is None or t.get("data", {}).get("filename") == original_filename)
                ]
                if tasks_for_file:
                    filtered_tasks = tasks_for_file
                    actual_pid = pid
                    break
            except Exception:
                pass

        total = len(filtered_tasks)
        completed = sum(1 for t in filtered_tasks if t.get("total_annotations", 0) > 0)

        return {
            "client_code": client_code,
            "filename": original_filename,
            "total_segments": total,
            "completed_annotations": completed,
            "pending_annotations": total - completed,
            "completion_percentage": round((completed / total * 100), 1) if total > 0 else 0,
            "project_id": actual_pid,
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "client_code": client_code,
        }
