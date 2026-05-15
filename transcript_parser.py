import json
import csv
import io
import re
from typing import List, Dict, Any

def parse_transcript_content(content: bytes, filename: str) -> List[Dict[str, Any]]:
    """
    Parses raw transcript bytes (JSON, CSV, TSV, TXT) into a standard segment list format:
    [
        {"start_time": float, "end_time": float, "speaker": str, "transcript": str}
    ]
    """
    segments = []
    if not content:
        # Generate elegant default simulation data if file is empty or mock
        return [
            {"start_time": 0.0, "end_time": 4.5, "speaker": "Agent", "transcript": "Hello, thank you for calling Vaidika Loan Services. How can I assist you today?"},
            {"start_time": 5.0, "end_time": 9.2, "speaker": "Borrower", "transcript": "Hi, I am looking to get a micro business loan for Sri Mahammayi Hardware store."},
            {"start_time": 10.0, "end_time": 15.5, "speaker": "Agent", "transcript": "Excellent, we can certainly help with that. Could you please share your GSTIN or business registration license code?"},
            {"start_time": 16.0, "end_time": 20.8, "speaker": "Borrower", "transcript": "Sure! My GSTIN is 21BUAPB0758D1ZH, registered in Odisha."},
            {"start_time": 21.5, "end_time": 26.0, "speaker": "Agent", "transcript": "Thank you, Rajesh. Let me pull that up. We will initiate our physical operations verification checklist next."}
        ]

    filename_lower = filename.lower()
    
    # 1. Parse JSON
    if filename_lower.endswith(".json"):
        try:
            data = json.loads(content.decode("utf-8", errors="ignore"))
            raw_list = []
            if isinstance(data, list):
                raw_list = data
            elif isinstance(data, dict):
                # Search for lists inside dictionary keys
                for key in ["segments", "dialogue", "transcript", "conversation", "messages", "turns"]:
                    if key in data and isinstance(data[key], list):
                        raw_list = data[key]
                        break
                if not raw_list:
                    # Fallback to dictionary values
                    for val in data.values():
                        if isinstance(val, list):
                            raw_list = val
                            break
            
            # Map segments
            current_time = 0.0
            for item in raw_list:
                if not isinstance(item, dict):
                    continue
                speaker = item.get("speaker") or item.get("role") or item.get("name") or item.get("speaker_name") or "Unknown"
                text = item.get("transcript") or item.get("text") or item.get("message") or item.get("content") or ""
                start = item.get("start_time") or item.get("start")
                end = item.get("end_time") or item.get("end")
                
                # Assign automatic timestamps if missing
                if start is None:
                    start = current_time
                if end is None:
                    duration = max(2.0, len(text.split()) * 0.4)
                    end = start + duration
                current_time = end + 0.5
                
                segments.append({
                    "start_time": float(start),
                    "end_time": float(end),
                    "speaker": str(speaker).strip(),
                    "transcript": str(text).strip()
                })
        except Exception as e:
            print(f"Error parsing JSON transcript: {e}")

    # 2. Parse Excel (.xlsx / .xls)
    elif filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
            sheet = wb.active
            rows = list(sheet.iter_rows(values_only=True))
            if rows:
                headers = [str(h).strip().lower() if h is not None else "" for h in rows[0]]
                speaker_idx = -1
                text_idx = -1
                start_idx = -1
                end_idx = -1
                for i, h in enumerate(headers):
                    if h in ["speaker", "role", "name", "speaker_name", "from"]:
                        speaker_idx = i
                    elif h in ["text", "transcript", "message", "content", "dialogue"]:
                        text_idx = i
                    elif h in ["start", "start_time", "starttime"]:
                        start_idx = i
                    elif h in ["end", "end_time", "endtime"]:
                        end_idx = i
                
                if speaker_idx == -1 and len(rows[0]) > 0:
                    speaker_idx = 0
                if text_idx == -1 and len(rows[0]) > 1:
                    text_idx = 1
                elif text_idx == -1 and len(rows[0]) == 1:
                    text_idx = 0
                
                data_rows = rows[1:] if len(headers) > 0 and (speaker_idx != -1 or text_idx != -1) else rows
                current_time = 0.0
                for r in data_rows:
                    if not r or all(cell is None or str(cell).strip() == "" for cell in r):
                        continue
                    speaker = str(r[speaker_idx]).strip() if speaker_idx < len(r) and r[speaker_idx] is not None else "Unknown"
                    text = str(r[text_idx]).strip() if text_idx < len(r) and r[text_idx] is not None else ""
                    if not text:
                        continue
                    
                    start = float(r[start_idx]) if start_idx != -1 and start_idx < len(r) and r[start_idx] is not None else current_time
                    duration = max(2.0, len(text.split()) * 0.4)
                    end = float(r[end_idx]) if end_idx != -1 and end_idx < len(r) and r[end_idx] is not None else (start + duration)
                    current_time = end + 0.5
                    
                    segments.append({
                        "start_time": float(start),
                        "end_time": float(end),
                        "speaker": speaker,
                        "transcript": text
                    })

                # Specialized Detection: Bulk Call Export (Each row is a separate call)
                if "transcript" in headers and "call_id" in headers and len(rows) > 1:
                    bulk_tasks = []
                    import re
                    for r_idx, r in enumerate(rows[1:]):
                        if not r or all(cell is None for cell in r): continue
                        row_dict = {headers[i]: r[i] for i in range(min(len(headers), len(r)))}
                        raw_text = str(row_dict.get("transcript", "")).strip()
                        if not raw_text: continue
                        
                        # Split dialogue turns
                        call_segments = []
                        lines = raw_text.split('\n')
                        for line in lines:
                            line = line.strip()
                            if not line: continue
                            match = re.match(r'^([^:]+):\s*(.*)$', line)
                            if match:
                                sp, tx = match.groups()
                                call_segments.append({"speaker": sp.strip(), "transcript": tx.strip()})
                            else:
                                call_segments.append({"speaker": "Unknown", "transcript": line})
                        
                        bulk_tasks.append({
                            "type": "bulk_call",
                            "metadata": row_dict,
                            "segments": call_segments
                        })
                    if bulk_tasks:
                        return bulk_tasks

        except Exception as e:
            print(f"Error parsing Excel transcript: {e}")

    # 3. Parse CSV / TSV
    elif filename_lower.endswith(".csv") or filename_lower.endswith(".tsv"):
        try:
            delimiter = "\t" if filename_lower.endswith(".tsv") else ","
            text_stream = io.StringIO(content.decode("utf-8", errors="ignore"))
            reader = csv.reader(text_stream, delimiter=delimiter)
            rows = list(reader)
            if rows:
                headers = [h.strip().lower() for h in rows[0]]
                # Map headers
                speaker_idx = -1
                text_idx = -1
                for i, h in enumerate(headers):
                    if h in ["speaker", "role", "name", "speaker_name", "from"]:
                        speaker_idx = i
                    elif h in ["text", "transcript", "message", "content", "dialogue"]:
                        text_idx = i
                
                # Fallback index if headers are not descriptive
                if speaker_idx == -1 and len(rows[0]) > 0:
                    speaker_idx = 0
                if text_idx == -1 and len(rows[0]) > 1:
                    text_idx = 1
                elif text_idx == -1 and len(rows[0]) == 1:
                    text_idx = 0
                
                data_rows = rows[1:] if len(headers) > 0 and (speaker_idx != -1 or text_idx != -1) else rows
                current_time = 0.0
                for r in data_rows:
                    if not r:
                        continue
                    speaker = r[speaker_idx] if speaker_idx < len(r) else "Unknown"
                    text = r[text_idx] if text_idx < len(r) else ""
                    if not text:
                        continue
                    start = current_time
                    duration = max(2.0, len(text.split()) * 0.4)
                    end = start + duration
                    current_time = end + 0.5
                    
                    segments.append({
                        "start_time": float(start),
                        "end_time": float(end),
                        "speaker": str(speaker).strip(),
                        "transcript": str(text).strip()
                    })
        except Exception as e:
            print(f"Error parsing CSV/TSV transcript: {e}")

    # 4. Parse TXT / plain text
    else:
        try:
            text_lines = content.decode("utf-8", errors="ignore").splitlines()
            current_time = 0.0
            # Common patterns:
            # Speaker 1: Hello
            # [Speaker 1] Hello
            # Agent (00:05): Hello
            regex_patterns = [
                re.compile(r"^\[?([^:\]]+)\]?\s*:\s*(.*)$"), # Speaker: Text
                re.compile(r"^\[([^\]]+)\]\s*(.*)$")        # [Speaker] Text
            ]
            for line in text_lines:
                line = line.strip()
                if not line:
                    continue
                matched = False
                for pat in regex_patterns:
                    m = pat.match(line)
                    if m:
                        speaker = m.group(1).strip()
                        text = m.group(2).strip()
                        # Clean timestamps like "Speaker 1 (00:05)"
                        speaker = re.sub(r"\s*\([^)]*\)", "", speaker).strip()
                        
                        start = current_time
                        duration = max(2.0, len(text.split()) * 0.4)
                        end = start + duration
                        current_time = end + 0.5
                        
                        segments.append({
                            "start_time": float(start),
                            "end_time": float(end),
                            "speaker": speaker,
                            "transcript": text
                        })
                        matched = True
                        break
                
                # Fallback if line does not match speaker format
                if not matched:
                    start = current_time
                    duration = max(2.0, len(line.split()) * 0.4)
                    end = start + duration
                    current_time = end + 0.5
                    segments.append({
                        "start_time": float(start),
                        "end_time": float(end),
                        "speaker": "Unknown",
                        "transcript": line
                    })
        except Exception as e:
            print(f"Error parsing plain text transcript: {e}")

    return segments
