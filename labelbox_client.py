import json
import os
import uuid
import subprocess
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, ContentSettings

load_dotenv()

try:
    import labelbox as lb
    from labelbox.schema.ontology import Ontology
except ImportError:
    print("Labelbox SDK not installed. Install with: pip install labelbox")
    raise

def transcode_to_mp3(input_data: bytes, original_ext: str = ".m4a") -> bytes:
    """Transcode audio bytes to MP3 using ffmpeg."""
    with tempfile.NamedTemporaryFile(suffix=original_ext, delete=False) as temp_in:
        temp_in.write(input_data)
        temp_in_path = temp_in.name
    
    temp_out_path = temp_in_path.replace(original_ext, ".mp3")
    try:
        subprocess.run(
            ['ffmpeg', '-y', '-i', temp_in_path, '-codec:a', 'libmp3lame', '-qscale:a', '2', temp_out_path],
            check=True, capture_output=True
        )
        with open(temp_out_path, "rb") as f:
            return f.read()
    finally:
        if os.path.exists(temp_in_path): os.remove(temp_in_path)
        if os.path.exists(temp_out_path): os.remove(temp_out_path)

def format_timestamp(seconds: float) -> str:
    """Format seconds into HH:MM:SS string."""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours:02}:{minutes:02}:{secs:02}"

def generate_sas_url(filename: str, client_code: str) -> str:
    """Generate a SAS URL for the audio file in Azure Blob Storage."""
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
        
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_name = "client-intake"
    
    # Try direct match first (client_code/filename)
    blob_name = f"{client_code}/{filename}"
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    
    try:
        if not blob_client.exists():
            # Search for blob with timestamp prefix (client_code/YYYYMMDD_HHMMSS_filename)
            print(f"Direct match failed for {blob_name}, searching for timestamped blob...")
            container_client = blob_service_client.get_container_client(container_name)
            prefix = f"{client_code}/"
            blobs = list(container_client.list_blobs(name_starts_with=prefix))
            
            # Find the latest blob that ends with the filename
            matching_blobs = [b.name for b in blobs if b.name.endswith(filename)]
            if matching_blobs:
                blob_name = sorted(matching_blobs)[-1] # Take the latest one lexicographically
                blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                print(f"Found matching blob: {blob_name}")
            else:
                raise ValueError(f"No blob found in container '{container_name}' matching {client_code}/*{filename}")
    except Exception as e:
        print(f"Error during blob search: {str(e)}")
        # If listing failed, try to continue with the original guess
        pass

    # Generate SAS token valid for 30 days
    # SPECIAL CASE: Labelbox Audio projects often reject .m4a and .wav
    # We transcode to .mp3 and store in 'processing' container for best compatibility
    if blob_name.lower().endswith((".m4a", ".wav")):
        try:
            ext = os.path.splitext(blob_name)[1]
            mp3_blob_name = blob_name.replace(ext, ".mp3")
            processing_container = "processing"
            mp3_blob_client = blob_service_client.get_blob_client(container=processing_container, blob=mp3_blob_name)
            
            if not mp3_blob_client.exists():
                print(f"Transcoding {blob_name} to MP3 for Labelbox compatibility...")
                audio_data = blob_client.download_blob().readall()
                mp3_data = transcode_to_mp3(audio_data, ext)
                mp3_blob_client.upload_blob(mp3_data, overwrite=True, content_settings=ContentSettings(content_type="audio/mpeg"))
                print(f"Uploaded transcoded MP3 to {processing_container}/{mp3_blob_name}")
            
            # Use the MP3 blob for Labelbox
            blob_name = mp3_blob_name
            container_name = processing_container
            blob_client = mp3_blob_client
        except Exception as te:
            print(f"Transcoding failed (falling back to original): {str(te)}")

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=30)
    )
    
    return f"{blob_client.url}?{sas_token}"

def get_ontology_mapping(project: lb.Project) -> Dict[str, Any]:
    """Discover schema IDs for the project's ontology using normalized JSON."""
    try:
        ont = project.ontology().normalized
    except Exception as e:
        print(f"Error fetching normalized ontology: {e}")
        return {"tools": {}, "classifications": {}}

    def walk_features(features):
        mapping = {}
        for f in features:
            # Use 'name' or 'instructions' for the feature identifier
            name = f.get('name', f.get('instructions', '')).lower().replace(" ", "_")
            feature_data = {
                "id": f.get('featureSchemaId'),
                "options": {}
            }
            
            # Options (for Radio/Checklist) can have nested classifications
            for o in f.get('options', []):
                opt_label = o.get('label', '').lower()
                opt_data = {
                    "id": o.get('featureSchemaId'),
                    "classifications": {}
                }
                # In normalized JSON, nested features are in the 'options' list of the RadioOption
                if o.get('options'):
                    opt_data["classifications"] = walk_features(o['options'])
                feature_data["options"][opt_label] = opt_data
            
            mapping[name] = feature_data
        return mapping

    mapping = {
        "tools": walk_features(ont.get('tools', [])),
        "classifications": walk_features(ont.get('classifications', []))
    }
    
    print(f"Discovered Tools: {list(mapping['tools'].keys())}")
    print(f"Discovered Classifications: {list(mapping['classifications'].keys())}")
    return mapping

def push_to_labelbox(
    processed_file_path: str,
    client_code: str,
    original_filename: str
) -> Dict[str, Any]:
    """
    Push processed audio and MAL labels to Labelbox.
    """
    try:
        print(f"Starting Labelbox upload for {client_code}/{original_filename}")
        
        api_key = os.getenv("LABELBOX_API_KEY")
        project_id = os.getenv("LABELBOX_PROJECT_ID")
        
        if not api_key or not project_id:
            raise ValueError("Labelbox credentials not found in environment")
        
        # STEP 1: Load processed JSON
        # processor.py writes orient='records' → plain list; handle both list and dict formats
        with open(processed_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        segments = data if isinstance(data, list) else data.get('segments', [])

        if not segments:
            raise ValueError(f"No segments found in {processed_file_path}")
        
        # STEP 2: Connect and Discover Ontology
        lb_client = lb.Client(api_key=api_key)
        project = lb_client.get_project(project_id)
        ontology_map = get_ontology_mapping(project)
        print(f"Discovered ontology for project: {project.name}")
        
        # STEP 3: Prepare Data Row (Direct Upload Strategy)
        print(f"Preparing direct upload for {original_filename}...")
        
        # Download the blob locally first to ensure we can upload it directly
        # This is more robust than giving Labelbox a SAS URL
        temp_dir = tempfile.mkdtemp()
        local_audio_path = os.path.join(temp_dir, original_filename)
        
        try:
            # We already have the logic to find the blob in generate_sas_url's helper or we can just do it here
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            
            # Find the actual blob name (it might have a timestamp prefix)
            container_name = "client-intake"
            container_client = blob_service_client.get_container_client(container_name)
            blobs = container_client.list_blobs(name_starts_with=f"{client_code}/")
            matching_blobs = [b.name for b in blobs if original_filename in b.name]
            
            if not matching_blobs:
                raise ValueError(f"Could not find {original_filename} on Azure for direct upload.")
            
            latest_blob = sorted(matching_blobs)[-1]
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=latest_blob)
            
            print(f"Downloading {latest_blob} for direct Labelbox upload...")
            audio_data = blob_client.download_blob().readall()
            
            # ALWAYS transcode to MP3 for stability and smaller upload size
            print("Compressing audio to MP3 for faster, more stable upload...")
            ext = os.path.splitext(latest_blob)[1]
            mp3_data = transcode_to_mp3(audio_data, ext)
            
            # Save as MP3 locally
            mp3_filename = os.path.splitext(original_filename)[0] + ".mp3"
            local_mp3_path = os.path.join(temp_dir, mp3_filename)
            with open(local_mp3_path, "wb") as f:
                f.write(mp3_data)
            
            # Use the local MP3 path for direct upload
            row_data = local_mp3_path
        except Exception as de:
            print(f"Direct compression/upload failed: {de}. Falling back to SAS URL...")
            row_data = generate_sas_url(original_filename, client_code)

        global_key = f"{client_code}_{original_filename}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        
        # Metadata: Intent
        metadata_fields = []
        try:
            mdo = lb_client.get_data_row_metadata_ontology()
            intent_field = next((f for f in mdo.fields if f.name == "Intent"), None)
            if intent_field:
                unique_intents = list(set(s.get('intent', 'Other') for s in segments))
                metadata_fields.append({
                    "schema_id": intent_field.uid,
                    "value": ", ".join(unique_intents)[:500]
                })
        except Exception as me:
            print(f"Metadata fetch failed (skipping): {str(me)}")

        # Prepare data row
        display_name = f"[{client_code}] {original_filename} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
        data_rows = [{
            "row_data": row_data,
            "global_key": global_key,
            "external_id": display_name,
            "metadata_fields": metadata_fields,
            "media_type": "AUDIO",
            "attachments": [
                {"type": "RAW_TEXT", "value": f"[{s.get('speaker', 'Unknown')} - {s.get('start_time', 0):.2f}-{s.get('end_time', 0):.2f}] {s.get('transcript', '')}"}
                for s in segments
            ]
        }]
        
        # Upload Data Row
        dataset_name = f"{client_code}_{original_filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        dataset = lb_client.create_dataset(name=dataset_name)
        upload_task = dataset.create_data_rows(data_rows)
        upload_task.wait_till_done()
        
        if upload_task.errors:
            print(f"Upload task errors: {upload_task.errors}")
            raise ValueError(f"Failed to upload data rows: {upload_task.errors[0].get('message')}")
            
        print(f"Data row uploaded to dataset: {dataset_name}")
        
        # Create Batch
        data_row_ids = [dr.uid for dr in dataset.data_rows()]
        project.create_batch(name=dataset_name, data_rows=data_row_ids)
        
        # STEP 4: Build ONE prediction per data row, grouping all frames by speaker.
        # Labelbox TemporalNDJSON format: answer is an array where each element
        # contains ALL frames for that speaker label, with nested transcript text entries.
        predictions = []
        speaker_feat = ontology_map["classifications"].get("speaker")

        if speaker_feat:
            # Collect frames + transcripts grouped by speaker label
            speaker_data = {}  # {label: {"frames": [...], "transcripts": [...]}}

            for segment in segments:
                start_ms = int(segment.get('start_time', 0) * 1000)
                end_ms = int(segment.get('end_time', 0) * 1000)
                speaker_type = segment.get('speaker', 'Unknown')
                transcript_text = segment.get('transcript', '')

                speaker_lower = speaker_type.lower()
                label = "Agent" if ("speaker 1" in speaker_lower or "agent" in speaker_lower) else "Customer"

                if label not in speaker_data:
                    speaker_data[label] = {"frames": [], "transcripts": []}

                frame = {"start": start_ms, "end": end_ms}
                speaker_data[label]["frames"].append(frame)
                speaker_data[label]["transcripts"].append({
                    "value": transcript_text,
                    "frames": [frame]
                })

            # Build answer array — one entry per speaker with all their frames
            answer = []
            for label, data in speaker_data.items():
                answer.append({
                    "name": label,
                    "frames": data["frames"],
                    "classifications": [
                        {
                            "name": "transcript",
                            "answer": data["transcripts"]
                        }
                    ]
                })

            if answer:
                predictions.append({
                    "uuid": str(uuid.uuid4()),
                    "dataRow": {"globalKey": global_key},
                    "name": "speaker",
                    "answer": answer
                })

        # STEP 5: Upload as MAL pre-labels (editable by labelers)
        if predictions:
            import json as _json
            print(f"Uploading {len(predictions)} MAL prediction(s) ({len(segments)} segments)...")
            print(f"DEBUG prediction sample: {_json.dumps(predictions[0], indent=2, ensure_ascii=False)[:500]}")
            upload_job = lb.MALPredictionImport.create_from_objects(
                lb_client, project_id, f"MAL_{global_key}", predictions
            )
            upload_job.wait_till_done()
            print(f"MAL upload complete. Status: {upload_job.state}")
            if upload_job.errors:
                print(f"MAL upload errors (FULL): {_json.dumps(upload_job.errors, indent=2, default=str)}")

        return {
            "status": "success",
            "dataset_name": dataset_name,
            "rows_uploaded": 1,
            "labels_uploaded": len(predictions),
            "labelbox_project_id": project_id
        }
        
    except Exception as e:
        error_msg = f"Error in Labelbox Audio upload: {str(e)}"
        print(error_msg)
        return {"status": "error", "error": error_msg}

def test_labelbox_connection() -> bool:
    try:
        lb_client = lb.Client(api_key=os.getenv("LABELBOX_API_KEY"))
        project = lb_client.get_project(os.getenv("LABELBOX_PROJECT_ID"))
        print(f"Connected to: {project.name}")
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False
