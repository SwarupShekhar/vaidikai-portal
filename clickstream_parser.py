import os
import re
import json
import csv
import html
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


def _build_timeline_html(formatted_events: list, breakpoint_index) -> str:
    """
    Render the session event timeline as a rich HTML log instead of LS's
    default chat-bubble Paragraphs view. Step numbers, page transitions,
    friction in red, drop-off step in amber.

    LS's HyperText element renders this verbatim in the labelling UI.
    """
    if not formatted_events:
        return "<div style='color:#888;font-style:italic;'>No events in this session.</div>"

    rows = [
        "<div style=\"font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:13px;line-height:1.5;\">"
    ]
    for i, ev in enumerate(formatted_events, start=1):
        is_friction = bool(ev.get("friction"))
        is_breakpoint = (breakpoint_index is not None and i == breakpoint_index)

        # Strip the "[N]" prefix from action; we render the step number ourselves
        action_clean = str(ev.get("action") or "")
        if action_clean.startswith("["):
            close_idx = action_clean.find("]")
            if close_idx > 0:
                action_clean = action_clean[close_idx + 1:].strip()
        element = str(ev.get("element") or "")

        # Colour palette by row state
        if is_breakpoint:
            bg, border, prefix = "#fff3cd", "#ff9800", "🎯"
            badge_label = "DROP-OFF POINT"
        elif is_friction:
            bg, border, prefix = "#ffebee", "#e53935", "⚠"
            badge_label = ev.get("friction", "Friction")
        else:
            bg, border, prefix = "#f5f5f5", "#90a4ae", "•"
            badge_label = ""

        # Escape user-derived content to avoid breaking the HTML / XSS
        action_safe  = html.escape(action_clean)
        element_safe = html.escape(element)
        badge_safe   = html.escape(badge_label)

        row = (
            f"<div style=\"display:flex;align-items:flex-start;background:{bg};"
            f"border-left:4px solid {border};padding:8px 12px;margin:4px 0;"
            f"border-radius:4px;\">"
            f"<span style=\"min-width:32px;font-weight:bold;color:#555;"
            f"text-align:right;margin-right:12px;\">#{i}</span>"
            f"<span style=\"font-family:'SF Mono',Menlo,Consolas,monospace;"
            f"font-weight:bold;color:#1565c0;margin-right:10px;\">{action_safe}</span>"
            f"<span style=\"color:#444;flex:1;\">{element_safe}</span>"
        )
        if badge_label:
            row += (
                f"<span style=\"background:white;border:1px solid {border};"
                f"padding:2px 8px;border-radius:12px;font-size:11px;"
                f"color:{border};font-weight:bold;margin-left:8px;"
                f"white-space:nowrap;\">{prefix} {badge_safe}</span>"
            )
        row += "</div>"
        rows.append(row)

    rows.append("</div>")
    return "".join(rows)


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
        # Scan ALL events for a non-empty user type — first-event-only loses
        # ~38% of sessions because EP_USER_TYPE is intermittently null.
        user_type = "Unknown"
        for _ev in raw_events:
            cand = str(_ev.get("EP_USER_TYPE") or "").strip()
            if cand and cand.lower() not in ("none", "null", "nan", "na", ""):
                user_type = cand
                break
            # Try nested EVENT_PARAMS too
            ep = _ev.get("EVENT_PARAMS") or _ev.get("event_params")
            if isinstance(ep, str):
                try:
                    ep = json.loads(ep)
                except Exception:
                    ep = {}
            if isinstance(ep, dict):
                cand2 = str(ep.get("EP_USER_TYPE") or "").strip()
                if cand2 and cand2.lower() not in ("none", "null", "nan", "na", ""):
                    user_type = cand2
                    break
        app_version = str(first.get("AppVersion") or first.get("CT App Version") or "")
        platform = str(first.get("PLATFORM") or "Mobile")

        formatted_events = []
        friction_signals = set()
        page_visit_counts = {}
        pages_so_far = []

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

            pages_so_far.append(page)

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

            # Rage Click Heuristic (multiple clicks/taps on the same target within 2 seconds)
            is_rage_click = False
            if i > 0:
                prev_ev = raw_events[i - 1]
                def get_prev_field(*keys):
                    for k in keys:
                        for ek, ev_val in prev_ev.items():
                            if str(ek).lower().strip() == k.lower():
                                return str(ev_val) if ev_val not in (None, "nan", "NA", "") else ""
                    return ""
                
                prev_event_params = {}
                prev_raw_params = prev_ev.get("EVENT_PARAMS") or prev_ev.get("event_params")
                if prev_raw_params:
                    if isinstance(prev_raw_params, dict):
                        prev_event_params = prev_raw_params
                    elif isinstance(prev_raw_params, str):
                        try:
                            prev_event_params = json.loads(prev_raw_params)
                        except Exception:
                            pass
                def get_prev_param_field(*keys):
                    for k in keys:
                        for pk, pv in prev_event_params.items():
                            if str(pk).lower().strip() == k.lower():
                                return str(pv) if pv not in (None, "nan", "NA", "") else ""
                    return ""

                prev_event_name = get_prev_field("EVENTNAME", "eventname", "event_name", "Event Name", "action", "Action", "Event", "event") or "UNKNOWN_EVENT"
                prev_page = get_prev_param_field("EP_PAGE_NAME", "EP_SCREEN_NAME", "page", "Page Name", "screen", "url", "URL") or get_prev_field("page", "Page", "Page Name", "screen", "Screen", "url", "URL")
                prev_source = get_prev_param_field("EP_SOURCE", "EP_CTA", "element", "button", "target", "cta", "source") or get_prev_field("element", "Element", "button", "Button", "target", "Target", "cta", "CTA", "source", "Source")
                if prev_source.startswith("{value:") and prev_source.endswith("}"):
                    prev_source = prev_source[7:-1]

                is_click = any(k in ev_upper for k in ("CLICK", "TAP", "VIEW")) or event_name.lower() in ("click", "tap")
                prev_is_click = any(k in prev_event_name.upper() for k in ("CLICK", "TAP", "VIEW")) or prev_event_name.lower() in ("click", "tap")

                if is_click and prev_is_click and page == prev_page and source == prev_source:
                    t1_str = get_field("timestamp", "date", "DATE")
                    t0_str = get_prev_field("timestamp", "date", "DATE")
                    if t1_str and t0_str:
                        if t1_str == t0_str:
                            is_rage_click = True
                        else:
                            t1_dt = parse_time_string(t1_str)
                            t0_dt = parse_time_string(t0_str)
                            if t1_dt and t0_dt and (t1_dt - t0_dt).total_seconds() <= 2.0:
                                is_rage_click = True

            if is_rage_click:
                event_friction.append("Rage Click")
                friction_signals.add("Rage Click")
                event_name = f"{event_name} (Double Click (Immediate))"

            # Navigation Loop Friction (Page A -> Page B -> Page A -> Page B)
            if len(pages_so_far) >= 4:
                p0 = pages_so_far[-4]
                p1 = pages_so_far[-3]
                p2 = pages_so_far[-2]
                p3 = pages_so_far[-1]
                if p0 == p2 and p1 == p3 and p0 != p1 and p0 != "" and p1 != "":
                    event_friction.append("Navigation Loop Friction")
                    friction_signals.add("Navigation Loop Friction")

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
                "friction": " / ".join(event_friction) if event_friction else "",
                "page": page,
                "action_raw": event_name,
                "element_raw": source
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

        # === Ami's expanded label schema (May 2026) ===
        # Derive 4 new dimensions + behavioural archetype from the real
        # Bajaj Finserv CleverTap event taxonomy.
        new_dims = _derive_new_dimensions(
            raw_events=raw_events,
            ev_upper=ev_names_str,
            friction_signals=friction_signals,
            journey_outcome=journey_outcome,
            event_count=len(raw_events),
        )

        # Device cohort = Make + AppVersion bucket — for release-impact slicing
        cohort_version = app_version.split(".")[:3] if app_version else []
        device_cohort = (
            f"{make or 'Unknown'} · {'.'.join(cohort_version) if cohort_version else 'No-Ver'}"
        )

        # === User-graph layer (Ami's "understand segments" ask) ===
        # PROFILE_PHONE is already hashed at source — we never decode it,
        # we just group by the hash to correlate sessions of the same user.
        user_id = ""
        for _ev in raw_events:
            cand = str(_ev.get("PROFILE_PHONE") or "").strip()
            if cand and cand.lower() not in ("none", "null", "nan", "na", ""):
                user_id = cand
                break
        # Fallback for anonymous sessions (pre-login): bucket each one
        # under its own pseudo-user so they don't collapse into a single
        # "Anonymous" mega-user.
        if not user_id:
            user_id = f"ANON-{session_id}"

        # Earliest date for chronological ordering across the user's sessions
        session_date = ""
        for _ev in raw_events:
            cand = str(_ev.get("DATE") or "").strip()
            if cand and cand.lower() not in ("none", "null", "nan", "na", ""):
                session_date = cand
                break

        # Build a rich HTML event log for the LS HyperText view (Option 2)
        # — replaces the chat-bubble Paragraphs rendering which read like a
        # dialogue. Step numbers, page transitions, friction in red, drop-off
        # in amber.
        timeline_html = _build_timeline_html(formatted_events, breakpoint_index)

        result.append({
            "session_id": session_id,
            "event_count": len(raw_events),
            "device": device,
            "user_type": user_type,
            "app_version": app_version,
            "platform": platform,
            "timeline_html": timeline_html,
            "user_id": user_id,
            "session_date": session_date,
            "friction_signals": friction_list,
            "session_status": session_status,
            "journey_intent": journey_intent,
            "journey_outcome": journey_outcome,
            "root_cause": root_cause,
            "breakpoint_index": breakpoint_index,
            "events": formatted_events,
            # New label dimensions (Ami 2026-05-19)
            "event_class": new_dims["event_class"],
            "granular_intent": new_dims["granular_intent"],
            "journey_stage": new_dims["journey_stage"],
            "product": new_dims["product"],
            # Segmentation axes
            "archetype": new_dims["archetype"],
            "device_cohort": device_cohort,
        })

    # Attach user-graph layer: groups sessions by hashed phone, derives
    # cross-session user archetype + recovery + persistent friction signal.
    _attach_user_graph(result)

    return result


def _attach_user_graph(sessions: list) -> None:
    """
    Mutates `sessions` in-place to attach user-level fields:
        user_archetype, user_session_count, user_session_index,
        recovery_flag, persistent_friction, other_sessions_brief.

    The hashed phone (already privacy-safe at source) is the grouping key.
    Sessions for the same user are ordered by session_date; user-level
    signals are derived from the sequence of session outcomes.
    """
    from collections import defaultdict

    # 1) Group sessions by user_id
    by_user = defaultdict(list)
    for s in sessions:
        by_user[s["user_id"]].append(s)

    # 2) For each user, sort chronologically + derive user-level signals
    for uid, user_sessions in by_user.items():
        # Sort by session_date (string-sortable for the Bajaj sample's
        # YYYY-MM-DD format; falls back to original order if absent).
        user_sessions.sort(key=lambda x: (x.get("session_date") or "", x["session_id"]))

        n = len(user_sessions)
        outcomes = [s["journey_outcome"] for s in user_sessions]
        friction_lists = [set(s.get("friction_signals", [])) for s in user_sessions]

        # Recovery = any "Successfully Completed" session followed any
        # non-success session for the same user.
        non_success_before = False
        recovery_flag = False
        for o in outcomes:
            if o == "Successfully Completed" and non_success_before:
                recovery_flag = True
                break
            if o != "Successfully Completed":
                non_success_before = True

        # Persistent friction = same friction signal appears in 2+ sessions
        from collections import Counter as _C
        friction_freq = _C()
        for fs in friction_lists:
            for f in fs:
                friction_freq[f] += 1
        persistent_friction = sorted(
            [f for f, c in friction_freq.items() if c >= 2]
        )

        # User archetype derivation
        success_count   = sum(1 for o in outcomes if o == "Successfully Completed")
        abandon_count   = sum(1 for o in outcomes if o == "Abandoned Mid-Journey")
        blocked_count   = sum(1 for o in outcomes if o == "Blocked by Error")
        deflected_count = sum(1 for o in outcomes if o == "Deflected to Support")
        bad_outcome     = abandon_count + blocked_count + deflected_count

        if n == 1:
            user_archetype = "Single-Visit Drop"
        elif recovery_flag:
            user_archetype = "Recoverer"
        elif success_count >= 2:
            user_archetype = "Power User"
        elif n >= 3 and bad_outcome == n and success_count == 0:
            user_archetype = "Persistent Struggler"
        elif n >= 2 and bad_outcome == n and success_count == 0:
            user_archetype = "Lost Cause"
        else:
            user_archetype = "Explorer"

        # Build compact "other sessions" brief so annotators can see the
        # user's full arc without leaving the current session task.
        outcome_short = {
            "Successfully Completed":   "✓ Done",
            "Abandoned Mid-Journey":    "✗ Abandoned",
            "Blocked by Error":         "✗ Error",
            "Deflected to Support":     "→ Support",
            "Browsing / Inconclusive":  "… Browsing",
        }
        sessions_brief = []
        for s in user_sessions:
            sessions_brief.append(
                f"S{user_sessions.index(s)+1} ({s.get('session_date') or '—'}): "
                f"{outcome_short.get(s['journey_outcome'], s['journey_outcome'])} "
                f"[{s['product']}/{s['journey_stage']}]"
            )

        # Mutate each session with its user-level fields
        for idx, s in enumerate(user_sessions, start=1):
            s["user_archetype"] = user_archetype
            s["user_session_count"] = n
            s["user_session_index"] = idx
            s["user_recovery_flag"] = recovery_flag
            s["user_persistent_friction"] = persistent_friction
            # Exclude the current session from its own "other sessions" view
            s["other_sessions_brief"] = [
                b for j, b in enumerate(sessions_brief) if j + 1 != idx
            ]
            s["narrative_summary"] = _generate_narrative_summary(s)
            s["story_gist"] = _generate_story_gist(s)


def _generate_narrative_summary(s: dict) -> str:
    """
    Generates a structured, easy-to-read human-style step-by-step story/gist
    of what happened during the session.
    """
    uid = s.get("user_id", "")
    idx = s.get("user_session_index", 1)
    tot = s.get("user_session_count", 1)
    arch = s.get("user_archetype", "Explorer")
    prod = s.get("product", "General")
    intent = s.get("journey_intent", "Unknown")
    stage = s.get("journey_stage", "Engagement")
    outcome = s.get("journey_outcome", "Browsing")
    cause = s.get("root_cause", "No Issue")
    events = s.get("events") or []
    bp = s.get("breakpoint_index")
    frictions = s.get("friction_signals") or []

    # Part 1: Context/History
    if "ANON-" in uid:
        part1 = f"Anonymous user initiated {prod} exploration."
    else:
        part1 = f"User (hashed {uid[:8]}...)—a '{arch}'—started session {idx} of {tot} for {prod}."

    # Part 2: Actions & Friction
    if frictions:
        fric_desc = ", ".join(frictions)
        if bp and 1 <= bp <= len(events):
            action_name = events[bp-1].get("action", "")
            if action_name.startswith("["):
                close_idx = action_name.find("]")
                if close_idx > 0:
                    action_name = action_name[close_idx+1:].strip()
            part2 = f"Flow stalled at Step {bp} on '{action_name}' due to {fric_desc}."
        else:
            part2 = f"Encountered friction ({fric_desc}) during navigation."
            
        if "Page Reload Loop" in frictions:
            part2 += " This triggered repeated page reload loops as the user attempted to resolve the issue."
    else:
        part2 = "The session timeline was completely smooth with no technical friction or bugs."

    # Part 3: Outcome
    if outcome == "Successfully Completed":
        part3 = "The user successfully completed their transaction/goal."
    elif outcome == "Blocked by Error":
        part3 = f"The user was completely blocked by the technical bug and had to abandon the session."
    elif outcome == "Abandoned Mid-Journey":
        part3 = "The user abandoned the journey mid-way."
    elif outcome == "Deflected to Support":
        part3 = "The user was deflected to Help & Support seeking assistance."
    else:
        part3 = "The session was inconclusive (pure browsing)."

    # Structure as a short step-by-step story
    lines = [
        f"1. Context: {part1}",
        f"2. Goal: Navigating {intent} in {stage} stage.",
        f"3. Timeline Gist: {len(events)} events. {part2}",
        f"4. Result: {part3} (Outcome: {outcome} | Root Cause: {cause})"
    ]
    return "\n".join(lines)


def _generate_story_gist(s: dict) -> str:
    """
    Generates a concise, highly readable, step-by-step chronological story of the user session.
    Consolidates repeated screen visits and highlights exact friction points.
    """
    events = s.get("events") or []
    if not events:
        return "No events in this session."

    story_lines = []
    
    # 1. Parse actions into logical steps
    parsed_steps = []
    for idx, ev in enumerate(events):
        action = ev.get("action") or ""
        element = ev.get("element") or ""
        friction = ev.get("friction") or ""
        
        # Clean prefix like "[1] " from action
        if action.startswith("["):
            close_idx = action.find("]")
            if close_idx > 0:
                action = action[close_idx+1:].strip()
                
        # Clean double spaces/delimiters from element details
        clean_elem = element.strip()
        # If there are '>>' indicating friction in element, split it out
        if ">>" in clean_elem:
            clean_elem = clean_elem.split(">>")[0].strip()
            
        parsed_steps.append({
            "num": idx + 1,
            "action": action,
            "element": clean_elem,
            "friction": friction
        })

    # 2. Sequential de-duplication to avoid repetitive steps (e.g. repeated reloads, multiple consecutive same views)
    consolidated = []
    prev_key = None
    repeat_count = 1
    
    for ps in parsed_steps:
        # We group by action + element (ignoring differences in step number)
        # However, if there is friction, we never group it — friction must be highlighted explicitly!
        key = (ps["action"], ps["element"])
        if ps["friction"]:
            if prev_key:
                consolidated.append((prev_key[0], prev_key[1], repeat_count, ""))
                prev_key = None
                repeat_count = 1
            consolidated.append((ps["action"], ps["element"], 1, ps["friction"]))
        else:
            if key == prev_key:
                repeat_count += 1
            else:
                if prev_key:
                    consolidated.append((prev_key[0], prev_key[1], repeat_count, ""))
                prev_key = key
                repeat_count = 1
                
    if prev_key:
        consolidated.append((prev_key[0], prev_key[1], repeat_count, ""))

    # 3. Format as a story
    step_num = 1
    for act, elem, count, fric in consolidated:
        # Convert action name to readable title case
        act_clean = act.replace("_", " ").title()
        
        # Build description
        if elem:
            # E.g. "Viewed Homepage <- HOMESCREEN_LANDING" -> "Viewed Homepage"
            if "<-" in elem:
                screen_detail = elem.split("<-")[0].strip()
                # Clean up screen name if it contains bracketed sections
                if "[" in screen_detail:
                    screen_detail = screen_detail.split("[")[0].strip()
                desc = f"{act_clean} on '{screen_detail}'"
            else:
                desc = f"{act_clean} on '{elem}'"
        else:
            desc = act_clean
            
        if count > 1:
            desc += f" ({count} times)"
            
        if fric:
            desc += f" ── ⚠️ Encountered {fric}!"
            
        story_lines.append(f"{step_num}. {desc}")
        step_num += 1

    # 4. Final outcome step
    outcome = s.get("journey_outcome", "Browsing")
    if outcome == "Successfully Completed":
        outcome_story = "Journey ended with successful completion of the goal."
    elif outcome == "Blocked by Error":
        outcome_story = f"Journey cut short and blocked by technical issue ({s.get('root_cause', 'Error')})."
    elif outcome == "Abandoned Mid-Journey":
        outcome_story = "Journey was abandoned by the user mid-way."
    elif outcome == "Deflected to Support":
        outcome_story = "User was deflected to Help & Support."
    else:
        outcome_story = "Journey ended after browsing."
        
    story_lines.append(f"{step_num}. Outcome: {outcome_story}")
    
    return "\n".join(story_lines)



def _derive_new_dimensions(
    raw_events: list,
    ev_upper: str,
    friction_signals: set,
    journey_outcome: str,
    event_count: int,
) -> dict:
    """
    Derive 4 new label dimensions (Event class, Granular Intent, Journey-stage,
    Product) + Behavioural Archetype from the real Bajaj Finserv CleverTap
    event vocabulary observed in the POC sample.

    All values are pre-annotations — annotators can override in Label Studio.
    """
    # ---------- 1. EVENT CLASS — dominant event type for the session ----------
    bucket_counts = {"App Launch": 0, "Login": 0, "Page View": 0, "CTA Click": 0}
    for ev in raw_events:
        nm = str(
            ev.get("EVENTNAME") or ev.get("eventname")
            or ev.get("event_name") or ev.get("Event Name") or ""
        ).upper()
        if not nm:
            continue
        if "APP LAUNCH" in nm or "APP_LAUNCH" in nm or "HOMEPAGE_LOAD" in nm or "APP LAUNCHED" in nm:
            bucket_counts["App Launch"] += 1
        elif any(k in nm for k in ("LOGIN", "DEVICE_PERMISSIONS", "CONSENT", "AUTH_INITIATED", "OTP", "REGISTRATION")):
            bucket_counts["Login"] += 1
        elif nm.endswith("_CLICKED") or nm.endswith("_TAPPED") or nm.endswith("_SUBMITTED") \
                or nm.endswith("_INITIATED") or nm.endswith("_CONFIRMATION") or nm.endswith("_DIGITIZED"):
            bucket_counts["CTA Click"] += 1
        elif nm.endswith("_VIEWED") or nm.endswith("_LOAD") or "VIEWED" in nm:
            bucket_counts["Page View"] += 1
        else:
            bucket_counts["Page View"] += 1
    # Pick the dominant bucket; on ties prefer the most "downstream" (CTA > View > Login > Launch)
    priority = ["CTA Click", "Page View", "Login", "App Launch"]
    event_class = max(priority, key=lambda c: (bucket_counts[c], -priority.index(c)))
    if bucket_counts[event_class] == 0:
        event_class = "App Launch"

    # ---------- 2. GRANULAR INTENT ----------
    is_repay = any(k in ev_upper for k in (
        "MAKE_PAYMENT", "PG_PAYMENT", "E-MANDATE", "E_MANDATE", "AUTOPAY",
        "UPI_REGISTRATION", "REPAYMENT", "REPAY", "MONEY_TRANSFER",
        "WALLET_PAYMENT", "WALLET_ADD_MONEY", "BBPS", "LOAN_DRAW_DOWN",
        "LOAN_PAYMENT_CONFIRMATION"
    ))
    is_service = any(k in ev_upper for k in (
        "HELP_SUPPORT", "SUPPORT_", "CONTACT_US", "RAR_", "NPS_",
        "MY_RELATIONS", "MY_ACCOUNT", "MY_ORDERS", "MY_CART",
        "EDIT_PROFILE", "STORE_LOCATOR", "DOCUMENT_CENTRE",
        "PASSBOOK", "LOAN_VIEW_STATEMENTS", "KYC_STATUS"
    ))
    n_clicks = sum(1 for ev in raw_events
                   if "_CLICKED" in str(ev.get("EVENTNAME") or "").upper()
                   or "_TAPPED" in str(ev.get("EVENTNAME") or "").upper())
    n_views = sum(1 for ev in raw_events
                  if "_VIEWED" in str(ev.get("EVENTNAME") or "").upper()
                  or "_LOAD" in str(ev.get("EVENTNAME") or "").upper())

    if is_repay:
        granular_intent = "Repayment Intent"
    elif is_service:
        granular_intent = "Service Intent"
    elif n_clicks >= 2:
        granular_intent = "Product Exploration — Active"
    else:
        granular_intent = "Product Exploration — Passive"

    # ---------- 3. JOURNEY STAGE ----------
    is_txn = any(k in ev_upper for k in (
        "_SUBMITTED", "PG_PAYMENT", "MAKE_PAYMENT", "PAYMENT_CONFIRMATION",
        "MONEY_TRANSFER_INITIATED", "LOAN_DRAW_DOWN", "AUTOPAY_INITIATED",
        "WALLET_PAYMENT_INITIATED", "E-MANDATE_POD", "E_MANDATE_POD_INITIATED",
        "WALLET_AUTH_INITIATED", "PL_DETAILS_SUBMITTED"
    ))
    is_onb = any(k in ev_upper for k in (
        "APP_LOGIN_INITIATED", "ACCOUNT_LOGIN_INITIATED", "DEVICE_PERMISSIONS",
        "CONSENT_", "REGISTRATION", "KYC_", "APP LAUNCHED", "APP_LAUNCHED",
        "WALLET_SETUP_INITIATED"
    ))

    if is_txn:
        journey_stage = "Transaction"
    elif is_onb and event_count <= 6:
        journey_stage = "Onboarding"
    else:
        journey_stage = "Engagement"

    # ---------- 4. PRODUCT ----------
    # Order matters: more specific prefixes first.
    if "EMI_CARD" in ev_upper or "EASY_EMI" in ev_upper:
        product = "EMI-Card"
    elif "PL_" in ev_upper or "PERSONAL_LOAN" in ev_upper:
        product = "Personal Loan"
    elif "LAP_" in ev_upper or "HOME_LOAN" in ev_upper:
        product = "Home Loan"
    else:
        product = "Multi-Product / General"

    # ---------- 5. BEHAVIOURAL ARCHETYPE (segment axis) ----------
    friction_count = len(friction_signals)
    has_help = "Help Support Triggered" in friction_signals
    has_abandon = journey_outcome == "Abandoned Mid-Journey"
    has_success = journey_outcome == "Successfully Completed"

    if has_abandon and friction_count >= 3:
        archetype = "Rage Quitter"
    elif has_help:
        archetype = "Help-Seeker"
    elif has_success and friction_count == 0 and is_txn:
        archetype = "Smooth Completer"
    elif friction_count == 0 and event_count >= 5 and is_txn:
        archetype = "Power User"
    elif 1 <= friction_count <= 2 and not is_txn:
        archetype = "Confused Novice"
    elif n_views >= 2 and n_clicks <= 1:
        archetype = "Curious Browser"
    elif friction_count == 0:
        archetype = "Smooth Completer"
    else:
        archetype = "Confused Novice"

    return {
        "event_class": event_class,
        "granular_intent": granular_intent,
        "journey_stage": journey_stage,
        "product": product,
        "archetype": archetype,
    }


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
