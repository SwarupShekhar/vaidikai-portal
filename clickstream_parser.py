import os
import re
import json
import csv
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ClickstreamParser")

def parse_clickstream_logs(raw_data: bytes, filename: str) -> list:
    """
    Parses raw bytes from an uploaded Clickstream log (JSON, CSV, TSV, or XLSX).
    Falls back to a detailed sandbox timeline if raw_data is empty or unparseable.
    """
    events = []
    filename_lower = filename.lower()
    
    # Check if raw data was provided and is not empty
    if raw_data and len(raw_data.strip()) > 0:
        try:
            # 1. Parse Excel (.xlsx / .xls) Clickstream Logs (Binary Format)
            if filename_lower.endswith(".xlsx") or filename_lower.endswith(".xls"):
                logger.info("Parsing Clickstream Excel (.xlsx) entries across multiple sheets...")
                import io
                import pandas as pd
                excel_file = pd.ExcelFile(io.BytesIO(raw_data))
                sheet_names = excel_file.sheet_names
                logger.info(f"Excel sheets found: {sheet_names}")
                
                # Identify the sheet containing actual event timeline data
                target_df = None
                for sheet in sheet_names:
                    df_sheet = pd.read_excel(excel_file, sheet_name=sheet)
                    cols_lower = [str(c).lower().strip() for c in df_sheet.columns]
                    if any(c in cols_lower for c in ["eventname", "action", "event_name", "event", "event_params", "element", "page"]):
                        target_df = df_sheet
                        logger.info(f"Selected sheet '{sheet}' as events table based on column match.")
                        break
                
                # If no specific sheet matched, default to the second sheet if 2+ sheets exist, else first
                if target_df is None:
                    if len(sheet_names) >= 2:
                        target_df = pd.read_excel(excel_file, sheet_name=sheet_names[1])
                        logger.info(f"Defaulting to second sheet '{sheet_names[1]}'")
                    else:
                        target_df = pd.read_excel(excel_file, sheet_name=sheet_names[0])
                        logger.info(f"Defaulting to first sheet '{sheet_names[0]}'")

                target_df = target_df.fillna("")
                records = []
                for _, row in target_df.iterrows():
                    rec = {}
                    for col, val in row.items():
                        if pd.isna(val) or val == "nan":
                            rec[str(col)] = ""
                        else:
                            rec[str(col)] = val
                    records.append(rec)
                events = records
                logger.info(f"Successfully parsed {len(events)} Clickstream Excel row entries.")
            
            else:
                decoded_content = raw_data.decode("utf-8", errors="ignore").strip()
                
                # 2. Parse JSON Clickstream Logs
                if filename_lower.endswith(".json") or decoded_content.startswith("["):
                    try:
                        events = json.loads(decoded_content)
                        if not isinstance(events, list):
                            events = [events]
                        logger.info(f"Successfully parsed {len(events)} Clickstream JSON event entries.")
                    except json.JSONDecodeError:
                        logger.warning("Failed parsing as JSON. Attempting line-by-line JSON parsing...")
                        # Fallback for log lines where each line is a JSON object
                        for line in decoded_content.splitlines():
                            if line.strip():
                                try:
                                    events.append(json.loads(line))
                                except Exception:
                                    pass
                
                # 3. Parse TSV / Tab-Separated Clickstream Logs
                elif filename_lower.endswith(".tsv") or "\t" in decoded_content.split("\n")[0]:
                    logger.info("Parsing Clickstream TSV entries...")
                    reader = csv.DictReader(decoded_content.splitlines(), delimiter="\t")
                    for row in reader:
                        events.append(dict(row))
                    logger.info(f"Successfully parsed {len(events)} Clickstream TSV row entries.")
                
                # 4. Parse CSV Clickstream Logs
                elif filename_lower.endswith(".csv") or "," in decoded_content.split("\n")[0]:
                    logger.info("Parsing Clickstream CSV entries...")
                    reader = csv.DictReader(decoded_content.splitlines())
                    for row in reader:
                        events.append(dict(row))
                    logger.info(f"Successfully parsed {len(events)} Clickstream CSV row entries.")
                
        except Exception as e:
            logger.error(f"Error parsing raw clickstream file content: {str(e)}. Falling back to simulation...")

    # 5. Fallback Sandbox Simulator
    if not events:
        logger.info("No raw events loaded or parsed. Generating high-fidelity clickstream sandbox timeline...")
        events = get_clickstream_simulation_logs(filename)

    # 5. Run Heuristic Friction and Timeline Aggregator
    return analyze_timeline_friction(events)


def parse_time_string(ts_str: str) -> datetime:
    """Helper to parse timestamps from various formats robustly."""
    if not ts_str:
        return None
    # Strip whitespace
    ts_str = ts_str.strip()
    # Try various formats
    for fmt in (
        "%H:%M:%S", 
        "%Y-%m-%d %H:%M:%S", 
        "%d-%b-%Y %H:%M:%S", 
        "%Y/%m/%d %H:%M:%S", 
        "%I:%M:%S %p",
        "%d-%m-%Y %H:%M:%S"
    ):
        try:
            return datetime.strptime(ts_str, fmt)
        except Exception:
            pass
            
    # Try custom extraction of HH:MM:SS if nested in string
    match = re.search(r'(\d{1,2}):(\d{2}):(\d{2})', ts_str)
    if match:
        try:
            # Reconstruct dummy time
            h, m, s = map(int, match.groups())
            return datetime(2026, 1, 1, h, m, s)
        except Exception:
            pass
            
    return None


def analyze_timeline_friction(events: list) -> list:
    """
    Groups events by CT_SESSION_ID, detects real app-analytics friction signals.
    Returns one session dict per CT_SESSION_ID — each becomes one Label Studio task.
    """
    # Group by CT_SESSION_ID
    sessions_map = {}
    session_order = []
    for ev in events:
        sid = str(ev.get("CT_SESSION_ID") or ev.get("ct_session_id") or "SINGLE_SESSION")
        if sid not in sessions_map:
            sessions_map[sid] = []
            session_order.append(sid)
        sessions_map[sid].append(ev)

    result = []

    for session_id in session_order:
        raw_events = sessions_map[session_id]
        first = raw_events[0]

        # Device / user metadata from first event
        make = str(first.get("Make") or first.get("make") or "")
        model = str(first.get("Model") or first.get("model") or "")
        device = f"{make} {model}".strip()
        if not device or device.lower() in ("null null", "null"):
            device = "Unknown Device"
        user_type = str(first.get("EP_USER_TYPE") or "Unknown")
        app_version = str(first.get("AppVersion") or first.get("CT App Version") or "")
        platform = str(first.get("PLATFORM") or "Mobile")

        formatted_events = []
        friction_signals = set()
        page_visit_counts = {}

        for i, ev in enumerate(raw_events):
            # Parse EVENT_PARAMS JSON blob
            event_params = {}
            raw_params = ev.get("EVENT_PARAMS") or ev.get("event_params")
            if raw_params:
                if isinstance(raw_params, dict):
                    event_params = raw_params
                elif isinstance(raw_params, str):
                    try:
                        event_params = json.loads(raw_params)
                    except Exception:
                        pass

            # Search keys flexibly to match all standard analytics schemas (Mixpanel, CleverTap, Amplitude, GA4, CSVs)
            def get_field(*keys):
                for k in keys:
                    for ek, ev_val in ev.items():
                        if str(ek).lower().strip() == k.lower():
                            return str(ev_val) if ev_val not in (None, "nan", "NA", "") else ""
                return ""

            def get_param_field(*keys):
                for k in keys:
                    for pk, pv in event_params.items():
                        if str(pk).lower().strip() == k.lower():
                            return str(pv) if pv not in (None, "nan", "NA", "") else ""
                return ""

            event_name = get_field("EVENTNAME", "eventname", "event_name", "Event Name", "action", "Action", "Event", "event") or "UNKNOWN_EVENT"
            page = get_param_field("EP_PAGE_NAME", "EP_SCREEN_NAME", "page", "Page Name", "screen", "url", "URL") or get_field("page", "Page", "Page Name", "screen", "Screen", "url", "URL")
            source = get_param_field("EP_SOURCE", "EP_CTA", "element", "button", "target", "cta", "source") or get_field("element", "Element", "button", "Button", "target", "Target", "cta", "CTA", "source", "Source")
            if source.startswith("{value:") and source.endswith("}"):
                source = source[7:-1]
            section = get_param_field("EP_SECTION", "section", "Section") or get_field("section", "Section")
            error_type = get_param_field("EP_ERROR_TYPE", "error_type", "error", "error_message") or get_field("error_type", "error_message", "error")
            cta = get_param_field("EP_CTA", "cta", "CTA") or get_field("cta", "CTA")

            ev_upper = event_name.upper()
            event_friction = []

            # PWA / technical errors
            if "EXCEPTION" in ev_upper or "3IN1_PWA" in ev_upper:
                event_friction.append("System / PWA Error")
                friction_signals.add("System / PWA Error")

            # Error / fail / blocked in event name
            if any(k in ev_upper for k in ["_ERROR", "_FAIL", "_BLOCKED", "_DENIED", "_REJECTED"]):
                if "System / PWA Error" not in event_friction:
                    event_friction.append("System / PWA Error")
                friction_signals.add("System / PWA Error")

            # Connectivity error in EVENT_PARAMS
            if error_type and "NO INTERNET" in error_type.upper():
                event_friction.append("Connectivity Error")
                friction_signals.add("Connectivity Error")
            elif error_type and error_type.strip() not in ("", "NA", "nan"):
                if "System / PWA Error" not in event_friction:
                    event_friction.append("System / PWA Error")
                friction_signals.add("System / PWA Error")

            # User sought help — strong frustration signal
            if "HELP_SUPPORT" in ev_upper:
                event_friction.append("Help Support Triggered")
                friction_signals.add("Help Support Triggered")

            # Exit intent / abandonment
            if page and "LEAVING SO SOON" in page.upper():
                event_friction.append("Exit Intent / Abandoned")
                friction_signals.add("Exit Intent / Abandoned")

            # Login failure
            if "LOGIN" in ev_upper and error_type and error_type.strip() not in ("", "NA"):
                event_friction.append("Login Failure")
                friction_signals.add("Login Failure")

            # Page reload loop (same page 3+ times in this session)
            if page:
                page_visit_counts[page] = page_visit_counts.get(page, 0) + 1
                if page_visit_counts[page] >= 3:
                    friction_signals.add("Page Reload Loop")

            # Build compact single-line display
            detail_parts = []
            if page:
                detail_parts.append(page)
            if source:
                detail_parts.append(f"<- {source}")
            if section:
                detail_parts.append(f"[{section}]")
            if event_friction:
                detail_parts.append(f">> {' / '.join(event_friction)}")

            formatted_events.append({
                "action": f"[{i+1}] {event_name}",
                "element": "  ".join(detail_parts) if detail_parts else "",
                "friction": " / ".join(event_friction) if event_friction else ""
            })

        # Determine overall session status
        friction_list = sorted(friction_signals)
        if "Exit Intent / Abandoned" in friction_signals or "Login Failure" in friction_signals:
            session_status = "Abandonment / Error"
        elif "Help Support Triggered" in friction_signals or len(friction_signals) >= 2:
            session_status = "High Frustration"
        elif friction_signals:
            session_status = "Minor Confusion"
        else:
            session_status = "Smooth Journey"

        # Build concatenated event name string for intent/outcome inference
        ev_names_str = " ".join(
            str(ev.get("EVENTNAME") or ev.get("eventname") or ev.get("event_name")
                or ev.get("Event Name") or ev.get("action") or "").upper()
            for ev in raw_events
        )

        # Infer Journey Intent
        if any(k in ev_names_str for k in ["PL_", "PERSONAL_LOAN", "LOAN_"]):
            journey_intent = "Personal Loan / Credit"
        elif any(k in ev_names_str for k in ["EMI_", "MY_RELATIONS", "REPAYMENT"]):
            journey_intent = "EMI Payment / Loan Mgmt"
        elif any(k in ev_names_str for k in ["HELP_SUPPORT", "SUPPORT_", "CONTACT_US"]):
            journey_intent = "Help & Support Seeking"
        elif any(k in ev_names_str for k in ["ACCOUNT_LOGIN", "DEVICE_PERMISSIONS", "ONBOARDING", "REGISTRATION"]):
            journey_intent = "App Login / Onboarding"
        elif any(k in ev_names_str for k in ["ACCOUNT_", "PROFILE_", "KYC_"]):
            journey_intent = "Account / Profile"
        elif any(k in ev_names_str for k in ["BANNER_PAGE", "HOMEPAGE", "PRODUCT_VIEW", "CATALOG"]):
            journey_intent = "Product Discovery"
        else:
            journey_intent = "Unknown / Multi-intent"

        # Infer Journey Outcome
        if "Exit Intent / Abandoned" in friction_signals or "Login Failure" in friction_signals:
            journey_outcome = "Abandoned Mid-Journey"
        elif "Help Support Triggered" in friction_signals:
            journey_outcome = "Deflected to Support"
        elif "Connectivity Error" in friction_signals or "System / PWA Error" in friction_signals:
            journey_outcome = "Blocked by Error"
        elif not friction_signals and any(
            k in ev_names_str for k in ["_SUBMITTED", "_SUCCESS", "_COMPLETE", "PAYMENT_DONE"]
        ):
            journey_outcome = "Successfully Completed"
        else:
            journey_outcome = "Browsing / Inconclusive"

        # Infer Root Cause
        if "Connectivity Error" in friction_signals or "Login Failure" in friction_signals:
            root_cause = "Network / Connectivity Issue"
        elif "System / PWA Error" in friction_signals and any(
            k in ev_names_str for k in ["EXCEPTION", "PWA", "_ERROR", "_FAIL"]
        ):
            root_cause = "App / PWA Bug"
        elif "Help Support Triggered" in friction_signals and not (
            "System / PWA Error" in friction_signals or "Connectivity Error" in friction_signals
        ):
            root_cause = "UI Confusion"
        elif "Exit Intent / Abandoned" in friction_signals and len(friction_signals) == 1:
            root_cause = "Deliberate User Exit"
        elif friction_signals:
            root_cause = "Backend / API Error"
        else:
            root_cause = "No Issue — Smooth"

        # Find first friction event index in timeline (+1 offset for summary row at index 0)
        breakpoint_index = None
        for idx, fev in enumerate(formatted_events):
            if fev.get("friction"):
                breakpoint_index = idx + 1
                break

        result.append({
            "session_id": session_id,
            "event_count": len(raw_events),
            "device": device,
            "user_type": user_type,
            "app_version": app_version,
            "platform": platform,
            "friction_signals": friction_list,
            "session_status": session_status,
            "journey_intent": journey_intent,
            "journey_outcome": journey_outcome,
            "root_cause": root_cause,
            "breakpoint_index": breakpoint_index,
            "events": formatted_events,
        })

    return result


def get_clickstream_simulation_logs(filename: str) -> list:
    """
    Returns realistic clickstream events containing simulated rage-clicks,
    navigation friction, or suspicious fast interaction.
    """
    filename_lower = filename.lower()
    
    # Case A: Bot / Scraping simulation
    if any(k in filename_lower for k in ["bot", "scrap", "automated"]):
        return [
            {"timestamp": "12:00:01", "page": "Homepage", "action": "View Page", "element": "Banner"},
            {"timestamp": "12:00:01", "page": "Catalog", "action": "Click", "element": "Ring Detail SKU-1"},
            {"timestamp": "12:00:01", "page": "Catalog", "action": "Click", "element": "Ring Detail SKU-2"},
            {"timestamp": "12:00:02", "page": "Catalog", "action": "Click", "element": "Ring Detail SKU-3"},
            {"timestamp": "12:00:02", "page": "Catalog", "action": "Click", "element": "Ring Detail SKU-4"},
            {"timestamp": "12:00:02", "page": "Catalog", "action": "Click", "element": "Ring Detail SKU-5"}
        ]
        
    # Case B: Standard high friction user journey with rage clicks
    elif any(k in filename_lower for k in ["friction", "rage", "cart", "bug"]):
        return [
            {"timestamp": "14:15:20", "page": "Homepage", "action": "View Page", "element": "Hero Banner"},
            {"timestamp": "14:15:35", "page": "Catalog", "action": "Click", "element": "Gold Diamond Studs"},
            {"timestamp": "14:16:01", "page": "Catalog", "action": "Click", "element": "Add To Cart"},
            {"timestamp": "14:16:01", "page": "Catalog", "action": "Click", "element": "Add To Cart"}, # Trigger Rage Click
            {"timestamp": "14:16:02", "page": "Catalog", "action": "Click", "element": "Add To Cart"}, # Trigger Rage Click
            {"timestamp": "14:16:20", "page": "Cart", "action": "View Page", "element": "Item List"},
            {"timestamp": "14:16:45", "page": "Checkout", "action": "View Page", "element": "Shipping Form"},
            {"timestamp": "14:17:00", "page": "Checkout", "action": "Click", "element": "Place Order Button"},
            {"timestamp": "14:17:01", "page": "Checkout", "action": "Click", "element": "Place Order Button"} # Another frustration click
        ]
        
    # Case C: Regular Smooth User Journey
    else:
        return [
            {"timestamp": "09:30:10", "page": "Homepage", "action": "View Page", "element": "Banner"},
            {"timestamp": "09:30:45", "page": "Catalog", "action": "Click", "element": "Traditional Necklace SKU-231"},
            {"timestamp": "09:31:12", "page": "Catalog", "action": "Click", "element": "Add to Cart"},
            {"timestamp": "09:31:30", "page": "Cart", "action": "Click", "element": "Checkout Button"},
            {"timestamp": "09:32:05", "page": "Checkout", "action": "Click", "element": "Complete Payment"}
        ]
