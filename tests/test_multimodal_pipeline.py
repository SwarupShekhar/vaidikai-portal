import os
import sys
from pathlib import Path
import pytest

# Ensure our app directory is in path
sys.path.insert(0, str(Path(__file__).parent.parent))

from redactor import mask_text_data
from runpod_client import run_runpod_inference
from ocr_fallback import local_ocr_scan
from clickstream_parser import parse_clickstream_logs
from labelstudio_client import push_jewelry_to_labelstudio, push_form_to_labelstudio, push_clickstream_to_labelstudio


# ===========================================================================
# 1. Test Suite: Local PII Redactor
# ===========================================================================
class TestPIIRedaction:
    def test_aadhaar_masking(self):
        text = "My Aadhaar card is 1234 5678 9012, please save it."
        masked = mask_text_data(text)
        assert "[REDACTED_AADHAAR]" in masked
        assert "1234" not in masked

    def test_pan_masking(self):
        text = "The customer tax registration number is ABCDE1234F."
        masked = mask_text_data(text)
        assert "[REDACTED_PAN]" in masked
        assert "ABCDE1234F" not in masked

    def test_phone_masking(self):
        text = "Reach me at +91 9876543210 or 98765-43210."
        masked = mask_text_data(text)
        assert "[REDACTED_PHONE]" in masked
        assert "9876543210" not in masked

    def test_name_redaction_context(self):
        text = "Name: Rajesh Kumar. Document processed."
        masked = mask_text_data(text)
        assert "[REDACTED_NAME]" in masked
        assert "Rajesh" not in masked

    def test_bank_account_redaction(self):
        text = "My bank account number is 9876543210123."
        masked = mask_text_data(text)
        assert "[REDACTED_FINANCIAL_ID]" in masked
        assert "9876543210123" not in masked


# ===========================================================================
# 2. Test Suite: RunPod Client SDK
# ===========================================================================
class TestRunPodClient:
    def test_runpod_jewelry_mock_fallback(self):
        # By passing a mock file url and triggering mock/blank credentials:
        res = run_runpod_inference("https://example.com/ring.jpg", task_type="jewelry")
        assert res["status"] == "success"
        assert len(res["predictions"]) == 1
        assert res["counts"]["total"] == 1
        assert res["predictions"][0]["class"] == "General Jewelry"

    def test_runpod_form_mock_fallback(self):
        res = run_runpod_inference("https://example.com/invoice.jpg", task_type="form")
        assert res["status"] == "success"
        assert "raw_ocr_text" in res
        assert "John Doe" in res["raw_ocr_text"]


# ===========================================================================
# 3. Test Suite: Label Studio Multimodal Pushing Adapters (Mock Setup)
# ===========================================================================
class TestLabelStudioAdapters:
    def test_push_adapters_import(self):
        # Ensures that functions can be loaded and parameters are standard
        assert callable(push_jewelry_to_labelstudio)
        assert callable(push_form_to_labelstudio)
        assert callable(push_clickstream_to_labelstudio)


# ===========================================================================
# 4. Test Suite: Local OCR Fallback & Sandbox Simulation
# ===========================================================================
class TestOCRFallback:
    def test_kyc_form_simulation_ocr(self):
        raw_text = local_ocr_scan("https://example.com/kyc_document.jpg")
        assert "Rajesh Khanna" in raw_text
        assert "4422 8866 1133" in raw_text
        assert "BPRPK9012Z" in raw_text

        # Run PII Scrubbing on output to verify integration
        redacted = mask_text_data(raw_text)
        assert "[REDACTED_NAME]" in redacted
        assert "[REDACTED_AADHAAR]" in redacted
        assert "[REDACTED_PAN]" in redacted
        assert "Rajesh" not in redacted
        assert "4422" not in redacted

    def test_invoice_simulation_ocr(self):
        raw_text = local_ocr_scan("https://example.com/invoice_882.png")
        assert "Sangeetha Nair" in raw_text
        assert "11223344556" in raw_text

        # Run PII Scrubbing
        redacted = mask_text_data(raw_text)
        assert "[REDACTED_NAME]" in redacted
        assert "[REDACTED_FINANCIAL_ID]" in redacted
        assert "Sangeetha" not in redacted
        assert "11223344556" not in redacted

    def test_default_simulation_ocr(self):
        raw_text = local_ocr_scan("https://example.com/unknown_asset.png")
        assert "Ramesh Chandra" in raw_text
        assert "9988-7766-5544" in raw_text


# ===========================================================================
# 5. Test Suite: Clickstream Log Parser & Friction Heuristics
# ===========================================================================
class TestClickstreamParser:
    def test_clickstream_json_parsing(self):
        raw_json = b'[{"timestamp": "11:00:01", "page": "Home", "action": "View", "element": "Hero"}]'
        parsed = parse_clickstream_logs(raw_json, "clickstream_user82.json")
        assert len(parsed) == 1
        assert parsed[0]["events"][0]["page"] == "Home"
        assert parsed[0]["session_status"] == "Smooth Journey"

    def test_clickstream_csv_parsing(self):
        raw_csv = b"timestamp,page,action,element\n12:10:00,Catalog,Click,Gold Ring SKU-88"
        parsed = parse_clickstream_logs(raw_csv, "clicks.csv")
        assert len(parsed) == 1
        assert parsed[0]["events"][0]["page"] == "Catalog"
        assert parsed[0]["events"][0]["element_raw"] == "Gold Ring SKU-88"

    def test_rage_click_heuristic(self):
        raw_json = b"""[
            {"timestamp": "12:00:01", "page": "Home", "action": "Click", "element": "CartBtn"},
            {"timestamp": "12:00:01", "page": "Home", "action": "Click", "element": "CartBtn"}
        ]"""
        parsed = parse_clickstream_logs(raw_json, "clicks.json")
        assert len(parsed) == 1
        assert "Rage Click" in parsed[0]["events"][1]["friction"]
        assert "Double Click (Immediate)" in parsed[0]["events"][1]["action"]

    def test_navigation_loop_friction(self):
        raw_json = b"""[
            {"timestamp": "12:00:01", "page": "Catalog", "action": "View", "element": "Grid"},
            {"timestamp": "12:00:05", "page": "Cart", "action": "View", "element": "CartList"},
            {"timestamp": "12:00:10", "page": "Catalog", "action": "View", "element": "Grid"},
            {"timestamp": "12:00:15", "page": "Cart", "action": "View", "element": "CartList"}
        ]"""
        parsed = parse_clickstream_logs(raw_json, "user_journey.json")
        assert len(parsed) == 1
        assert "Navigation Loop Friction" in parsed[0]["events"][3]["friction"]

    def test_clickstream_simulation_fallbacks(self):
        # 1. Friction / Rage fallback sim
        parsed_friction = parse_clickstream_logs(b"", "user_friction_journey.json")
        assert len(parsed_friction) >= 1
        # Ensure rage clicks were auto-detected on simulated duplicates
        assert any("Rage Click" in ev["friction"] for session in parsed_friction for ev in session["events"])

        # 2. Bot/Scraper fallback sim
        parsed_bot = parse_clickstream_logs(b"", "automated_scanner.json")
        assert len(parsed_bot) >= 1

    def test_clickstream_tsv_custom_schema(self):
        tsv_data = (
            b"CT_SESSION_ID\tEVENTNAME\tPROFILE_PHONE\tEVENT_PARAMS\tDATE\thashByte\tDL_MODIFIED_DATE\tPARTITIONCOL\tPLATFORM\tAppVersion\tMake\tModel\tEP_PLATFORM\tEP_USER_TYPE\tEP_CT_ID\n"
            b"1762845937\tBANNER_PAGE_VIEWED\tFD5A04\t{\"EP_NETWORK_CARRIER\":\"airtel\",\"EP_PAGE_NAME\":\"HOMEPAGE\",\"EP_SOURCE\":\"{value:HOMESCREEN_LANDING}\"}\t12-May-2026 11:34:21\t0x01\t2026-05-12\t01\tAndroid\t1.0\tSamsung\tS21\tAndroid\tStandard\t999\n"
            b"1762845937\tBANNER_PAGE_VIEWED\tFD5A04\t{\"EP_NETWORK_CARRIER\":\"airtel\",\"EP_PAGE_NAME\":\"HOMEPAGE\",\"EP_SOURCE\":\"{value:HOMESCREEN_LANDING}\"}\t12-May-2026 11:34:21\t0x01\t2026-05-12\t01\tAndroid\t1.0\tSamsung\tS21\tAndroid\tStandard\t999\n"
        )
        parsed = parse_clickstream_logs(tsv_data, "mobile_clickstream.tsv")
        assert len(parsed) == 1
        assert parsed[0]["session_id"] == "1762845937"
        assert parsed[0]["events"][0]["page"] == "HOMEPAGE"
        assert parsed[0]["events"][0]["action_raw"] == "BANNER_PAGE_VIEWED"
        assert parsed[0]["events"][0]["element_raw"] == "HOMESCREEN_LANDING"
        assert parsed[0]["device"] == "Samsung S21"
        assert "Rage Click" in parsed[0]["events"][1]["friction"]


# ===========================================================================
# 6. Test Suite: Housing & MSME Business Collateral Verification Pipeline
# ===========================================================================
class TestHousingAndBusinessCollateral:
    def test_msme_business_simulated_ocr_and_scrubbing(self):
        raw_text = local_ocr_scan("https://example.com/business_signboard.png")
        assert "SRI MAHAMMAYI HARDWARE" in raw_text
        assert "21BUAPB0758D1ZH" in raw_text
        assert "DL-21-9988X" in raw_text
        
        # Test scrubbing of tax/business credentials if passed through local redactor
        masked = mask_text_data(raw_text)
        assert "[REDACTED_FINANCIAL_ID]" in masked
        assert "21BUAPB0758D1ZH" not in masked

    def test_face_redaction_blurring(self):
        import numpy as np
        from redactor import redact_faces_in_image
        
        # Create a 200x200 image with a white background and a single black line
        img = np.ones((200, 200, 3), dtype=np.uint8) * 255
        img[100, :, :] = 0
        
        # Define segment for representative_person
        segments = [{
            "Category": "representative_person",
            "Points": [[25, 25], [75, 25], [75, 75], [25, 75]], # 50px to 150px bounds
            "RType": "polygonlabels"
        }]
        
        redacted = redact_faces_in_image(img, segments)
        
        # Blurring a sharp black edge (0 to 255 transition) introduces intermediate gray values
        assert not np.array_equal(img, redacted)
        assert redacted[100, 100, 0] != 0
        assert redacted[100, 100, 0] != 255

    def test_signature_category_isolation(self):
        import numpy as np
        from collateral_detector import generate_signature
        
        img = np.ones((100, 100, 3), dtype=np.uint8) * 128
        pts = [[10, 10], [90, 10], [90, 90], [10, 90]]
        
        sig_house = generate_signature(
            image=img,
            polygon_points=pts,
            category="house_facade",
            client_code="NBFC1",
            task_id=101,
            item_index=1,
            image_file="house.png",
            img_width=100,
            img_height=100
        )
        
        sig_business = generate_signature(
            image=img,
            polygon_points=pts,
            category="business_signboard",
            client_code="NBFC1",
            task_id=102,
            item_index=1,
            image_file="business.png",
            img_width=100,
            img_height=100
        )
        
        assert sig_house is not None
        assert sig_business is not None
        assert sig_house["category"] == "house_facade"
        assert sig_business["category"] == "business_signboard"
        assert sig_house["category"] != sig_business["category"]


# ===========================================================================
# 7. Test Suite: Text-Only Conversation Transcript Pipeline
# ===========================================================================
class TestTranscriptPipeline:
    def test_parse_txt_transcript(self):
        from transcript_parser import parse_transcript_content
        raw_txt = b"Agent: Hello Rajesh.\nBorrower: Yes hi, how are you?"
        segments = parse_transcript_content(raw_txt, "conversation.txt")
        assert len(segments) == 2
        assert segments[0]["speaker"] == "Agent"
        assert segments[0]["transcript"] == "Hello Rajesh."
        assert segments[1]["speaker"] == "Borrower"
        assert segments[1]["transcript"] == "Yes hi, how are you?"

    def test_parse_json_transcript(self):
        from transcript_parser import parse_transcript_content
        import json
        raw_json = json.dumps([
            {"speaker": "Agent", "transcript": "Hello."},
            {"speaker": "Borrower", "transcript": "Hi there."}
        ]).encode("utf-8")
        segments = parse_transcript_content(raw_json, "transcript.json")
        assert len(segments) == 2
        assert segments[0]["speaker"] == "Agent"
        assert segments[0]["transcript"] == "Hello."
        assert segments[1]["speaker"] == "Borrower"
        assert segments[1]["transcript"] == "Hi there."

    def test_parse_empty_transcript_simulation(self):
        from transcript_parser import parse_transcript_content
        # Ingest an empty file to trigger realistic simulated dialogues for pilot POCs
        segments = parse_transcript_content(b"", "empty_conversation.txt")
        assert len(segments) == 5
        assert segments[0]["speaker"] == "Agent"
        assert "21BUAPB0758D1ZH" in segments[3]["transcript"]


