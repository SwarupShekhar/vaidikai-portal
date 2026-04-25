"""
End-to-end pipeline test: upload → transcribe → push to Labelbox.
Run from the project root:
    cd vaidikai-portal && python -m pytest tests/test_e2e_pipeline.py -v -s
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime

import pytest
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
TEST_CLIENT = "TEST_E2E"
AUDIO_FILE = Path(__file__).parent.parent / "Hindi Tutoring Audio (Science).m4a"
AZURE_CONN = os.getenv("AZURE_STORAGE_CONNECTION_STRING")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def blob_name():
    """Upload the local audio file to Azure and return the blob path."""
    assert AUDIO_FILE.exists(), f"Audio file not found: {AUDIO_FILE}"
    assert AZURE_CONN, "AZURE_STORAGE_CONNECTION_STRING not set"

    from azure.storage.blob import BlobServiceClient
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONN)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    name = f"{TEST_CLIENT}/{timestamp}_{AUDIO_FILE.name}"

    blob_client = blob_service.get_blob_client(container="client-intake", blob=name)
    with open(AUDIO_FILE, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    print(f"\n[SETUP] Uploaded → {name}")
    yield name

    # Teardown: remove test blob
    blob_client.delete_blob()
    print(f"\n[TEARDOWN] Deleted blob → {name}")


@pytest.fixture(scope="module")
def processed_file(blob_name):
    """Run the full transcription pipeline and return the processed file path."""
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from processor import process_audio

    result = process_audio(blob_name, TEST_CLIENT, language="hi")
    print(f"\n[PIPELINE] Result: {result}")
    return result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestTranscription:
    def test_pipeline_succeeds(self, processed_file):
        assert processed_file["status"] == "success", (
            f"Pipeline failed: {processed_file.get('error')}"
        )

    def test_segments_returned(self, processed_file):
        count = processed_file.get("segments", 0)
        assert count > 0, "No segments returned — transcription produced nothing"
        print(f"\n  ✓ {count} segments transcribed")

    def test_minimum_segments_for_5min_audio(self, processed_file):
        count = processed_file.get("segments", 0)
        assert count >= 10, (
            f"Only {count} segments for a ~5-min audio — too few, likely chunking issue"
        )

    def test_processed_file_exists_on_disk(self, processed_file):
        path = Path(processed_file["processed_file"])
        assert path.exists(), f"Processed JSON not found at {path}"

    def test_processed_file_is_valid_json_list(self, processed_file):
        path = Path(processed_file["processed_file"])
        with open(path) as f:
            data = json.load(f)
        assert isinstance(data, list), (
            f"Processed file must be a JSON list, got {type(data).__name__}"
        )
        assert len(data) > 0, "Processed file is empty list"

    def test_segments_have_required_fields(self, processed_file):
        path = Path(processed_file["processed_file"])
        with open(path) as f:
            segments = json.load(f)
        required = {"segment_id", "transcript", "speaker", "start_time", "end_time", "intent", "sentiment"}
        first = segments[0]
        missing = required - set(first.keys())
        assert not missing, f"Segments missing fields: {missing}"

    def test_transcripts_are_non_empty(self, processed_file):
        path = Path(processed_file["processed_file"])
        with open(path) as f:
            segments = json.load(f)
        empty = [s["segment_id"] for s in segments if not s.get("transcript", "").strip()]
        assert not empty, f"Segments with empty transcript: {empty}"

    def test_language_detected(self, processed_file):
        lang = processed_file.get("language", "")
        assert lang, "Language not detected"
        print(f"\n  ✓ Detected language: {lang}")


class TestLabelboxPush:
    def test_labelbox_push_succeeds(self, processed_file):
        assert os.getenv("LABELBOX_API_KEY"), "LABELBOX_API_KEY not set"
        assert os.getenv("LABELBOX_PROJECT_ID"), "LABELBOX_PROJECT_ID not set"

        sys.path.insert(0, str(Path(__file__).parent.parent))
        from labelbox_client import push_to_labelbox

        result = push_to_labelbox(
            processed_file["processed_file"],
            TEST_CLIENT,
            processed_file["original_filename"],
        )
        print(f"\n[LABELBOX] Result: {result}")
        assert result["status"] == "success", (
            f"Labelbox push failed: {result.get('error')}"
        )

    def test_labelbox_labels_uploaded(self, processed_file):
        from labelbox_client import push_to_labelbox

        result = push_to_labelbox(
            processed_file["processed_file"],
            TEST_CLIENT,
            processed_file["original_filename"],
        )
        labels = result.get("labels_uploaded", 0)
        assert labels > 0, f"0 labels uploaded to Labelbox — predictions list was empty"
        print(f"\n  ✓ {labels} labels pushed to Labelbox")
