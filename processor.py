import os
import json
import time
import tempfile
import shutil
import ssl
import gc
import threading
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import pandas as pd
from azure.storage.blob import BlobServiceClient
from openai import OpenAI
from dotenv import load_dotenv

# Fix SSL certificate verification issues for model download
ssl._create_default_https_context = ssl._create_unverified_context

load_dotenv()

# Global lock to ensure only one heavy transcription task runs at a time
_PROCESSING_LOCK = threading.Lock()

# Initialize OpenAI Client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# Client-specific role labels (Overridable)
# Format: { 'CLIENT_CODE': {'Agent': 'AgentLabel', 'Customer': 'CustomerLabel'} }
CLIENT_ROLE_CONFIG = {
    'DEFAULT': {'Agent': 'Speaker 1', 'Customer': 'Speaker 2'},
}

# Rule-based tagging rules
INTENT_RULES = {
    'Greeting': ['namaskar', 'hello', 'sahayta', 'kis prakar', 'namaskar', 'hello'],
    'Product Enquiry': ['credit card', 'kharidna', 'apply', 'card chahiye', 'credit card', 'apply'],
    'Verification': ['nam', 'salary', 'designation', 'company', 'bank account', 'salary', 'name', 'company'],
    'Annual Fee Discussion': ['annual charge', 'do hazaar', 'fees', 'annual', 'fee', 'charge'],
    'Benefit Explanation': ['benefit', 'cashback', 'launch', 'reward', 'cashback', 'lounge', 'benefit', 'offer'],
    'Objection': ['nahin bharna', 'nahin hai', 'no sir', 'cannot', 'nahin'],
    'Clarification Request': ['matlab', 'kaise', 'kab', 'kahan', 'meaning', 'how', 'when'],
    'Confirmation': ['oke sar', 'han ji', 'thik hai', 'okay', 'yes sir', 'confirmed'],
    'Call Issue': ['hello', 'avaj', 'hello', 'can you hear', 'network'],
}

SENTIMENT_RULES = {
    'Positive': ['oke sar', 'han ji', 'thik hai', 'benefit', 'okay', 'yes', 'good', 'great', 'reward'],
    'Negative': ['nahin bharna', 'nahin hai', 'no sir', 'cannot', 'not', 'nahin'],
    'Curious': ['matlab', 'kaise', 'kab', 'kya', 'what', 'how', 'when', 'really'],
    'Frustrated': ['hello', 'avaj nahin', 'hello hello', 'not working', 'again'],
}

OUTCOME_RULES = {
    'Call Opened': ['namaskar', 'sahayta', 'namaskar', 'help you'],
    'Lead Identified': ['credit card', 'kharidna', 'credit card', 'want to buy'],
    'KYC In Progress': ['nam', 'salary', 'company', 'name', 'salary', 'company'],
    'Fee Objection Raised': ['annual charge', 'nahin bharna', 'annual charge', 'not pay'],
    'Objection Handled': ['lekin', 'benefit', 'reward', 'but', 'benefit', 'reward'],
    'Benefit Communicated': ['cashback', 'launch', 'reward', 'cashback', 'lounge', 'offer'],
    'Customer Clarifying': ['matlab', 'really', 'meaning'],
    'Customer Interested': ['cashback', '5%', 'cashback', 'lounge', 'interested'],
    'Call Disruption': ['hello', 'avaj', 'hello hello'],
}

def tag(text: str, rules: Dict[str, List[str]], default: str = 'Other') -> str:
    """Apply rule-based tagging to text."""
    text_lower = text.lower()
    for label, keywords in rules.items():
        if any(kw.lower() in text_lower for kw in keywords):
            return label
    return default

def get_key_signal(text: str, max_words: int = 8) -> str:
    """Extract first N words as key signal."""
    words = text.split()
    return ' '.join(words[:max_words])

def calculate_qa_status(confidence: float) -> str:
    """Determine QA status based on confidence."""
    if confidence >= 65:
        return 'HIGH CONFIDENCE'
    elif confidence >= 40:
        return 'REVIEW NEEDED'
    else:
        return 'LOW CONFIDENCE'

def filter_segment(text: str, avg_logprob: float) -> bool:
    """Filter out low-quality segments."""
    if not text or len(text.strip()) == 0:
        return False
    # Relaxed filter: Allow short words like 'Ok', 'Ji', 'Yes'
    if avg_logprob < -1.5: # Hard filter only on extremely low quality
        return False
    return True

def identify_speaker_roles(segments: List[Dict], client_code: str) -> Dict[str, str]:
    """
    Use GPT-4o to identify which speaker ID corresponds to which role.
    """
    if not segments:
        return {}
    
    # 1. Get unique speaker IDs from the diarized output
    speaker_ids = sorted(list(set(s.get('speaker', 'Unknown') for s in segments)))
    speaker_ids = [sid for sid in speaker_ids if sid != 'Unknown']
    
    if len(speaker_ids) == 0:
        return {}
    
    if len(speaker_ids) == 1:
        return {speaker_ids[0]: "Agent"}

    # 2. Prepare sample transcript (first 15 segments)
    sample_lines = [f"{s.get('speaker')}: {s.get('text')}" for s in segments[:15]]
    sample = "\n".join(sample_lines)
    
    # 3. Get labels
    config = CLIENT_ROLE_CONFIG.get(client_code, CLIENT_ROLE_CONFIG['DEFAULT'])
    agent_label = config.get('Agent', 'Agent')
    customer_label = config.get('Customer', 'Customer')

    try:
        print(f"Identifying speaker roles for {client_code} using AI context...")
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system", 
                    "content": f"Analyze the call transcript and map speaker IDs to roles. Return JSON: {{'SpeakerID': '{agent_label}', 'SpeakerID': '{customer_label}'}}"
                },
                {
                    "role": "user", 
                    "content": f"Speaker IDs: {speaker_ids}\nSample:\n{sample}"
                }
            ],
            response_format={"type": "json_object"}
        )
        mapping = json.loads(response.choices[0].message.content)
        print(f"AI Role Mapping: {mapping}")
        return mapping
    except Exception as e:
        print(f"Role identification failed: {e}")
        return {sid: sid for sid in speaker_ids}

def process_audio(blob_filename: str, client_code: str, language: str = 'hi') -> Dict[str, Any]:
    """
    Process audio file using OpenAI gpt-4o-transcribe-diarize for high performance.
    """
    with _PROCESSING_LOCK:
        try:
            gc.collect()
            print(f"Starting cloud-based processing for {client_code}/{blob_filename}")
            
            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            base_temp_dir = Path(f"/tmp/vaidikai/{client_code}")
            base_temp_dir.mkdir(parents=True, exist_ok=True)
            transcripts_dir = base_temp_dir / "transcripts"
            outputs_dir = base_temp_dir / "outputs"
            transcripts_dir.mkdir(exist_ok=True)
            outputs_dir.mkdir(exist_ok=True)
            
            # STEP 1: Download with Fuzzy Matching
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client("client-intake")
            
            # List blobs in client folder and find the best match
            prefix = f"{client_code}/"
            blobs = list(container_client.list_blobs(name_starts_with=prefix))
            matching_blobs = [b.name for b in blobs if blob_filename.split("/")[-1] in b.name]
            
            if not matching_blobs:
                raise ValueError(f"No matching blob found for {blob_filename} in {prefix}")
            
            full_blob_path = sorted(matching_blobs)[-1] # Take the latest matching version
            print(f"Matched blob: {full_blob_path}")
            
            pure_filename = full_blob_path.split("/")[-1]
            local_audio_path = base_temp_dir / pure_filename
            
            blob_client = blob_service_client.get_blob_client(container="client-intake", blob=full_blob_path)
            with open(local_audio_path, "wb") as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            # STEP 2: Transcribe with Multi-Stage Fallback
            max_retries = 3
            last_error = None
            response = None
            mode = "PRIMARY (gpt-4o-diarize)"
            
            print(f"Mode: {mode}")
            for attempt in range(max_retries):
                try:
                    with open(local_audio_path, "rb") as f:
                        # ENGINE 1: OpenAI High-Precision (Diarized)
                        response = client.audio.transcriptions.create(
                            file=f,
                            model="gpt-4o-transcribe-diarize",
                            response_format="diarized_json",
                            language=language,
                            chunking_strategy="auto"
                        )
                    break
                except Exception as e:
                    last_error = e
                    print(f"Transcription attempt {attempt+1} failed: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(3 * (attempt + 1))
                    else:
                        try:
                            print("PRIMARY failed. Trying STABLE FALLBACK (whisper-1)...")
                            mode = "FALLBACK (whisper-1)"
                            with open(local_audio_path, "rb") as audio_file:
                                response = client.audio.transcriptions.create(
                                    model="whisper-1",
                                    file=audio_file,
                                    language=language if language else None,
                                    response_format="verbose_json"
                                )
                        except Exception as fe:
                            print(f"Whisper-1 also failed: {fe}. Attempting LOCAL FALLBACK (faster-whisper)...")
                            mode = "LOCAL (faster-whisper)"
                            try:
                                from faster_whisper import WhisperModel
                                # Using SMALL model for much better accuracy than BASE
                                print("Loading 'small' model for high-accuracy local fallback...")
                                local_model = WhisperModel("small", device="cpu", compute_type="int8")
                                segments, info = local_model.transcribe(str(local_audio_path), beam_size=10, language=language)
                                
                                # Convert generator to list of dicts to match OpenAI format
                                local_segments = []
                                for s in segments:
                                    local_segments.append({
                                        'start': s.start,
                                        'end': s.end,
                                        'text': s.text,
                                        'avg_logprob': s.avg_logprob
                                    })
                                
                                # Create a mock response object
                                class MockResponse:
                                    def __init__(self, segments, lang):
                                        self.segments = segments
                                        self.language = lang
                                
                                response = MockResponse(local_segments, info.language)
                            except Exception as local_e:
                                raise Exception(f"Ultimate failure: Local fallback failed too. Error: {local_e}")
            
            print(f"Transcription logic finished using {mode}")
            
            detected_language = getattr(response, 'language', 'unknown')
            raw_segments = getattr(response, 'segments', [])
            temp_segments = []
            
            # STEP 3: Unified Segment Processing
            if mode == "PRIMARY (gpt-4o-diarize)":
                for s in raw_segments:
                    is_dict = isinstance(s, dict)
                    text = s.get('text', '').strip() if is_dict else s.text.strip()
                    speaker = s.get('speaker', 'Unknown') if is_dict else getattr(s, 'speaker', 'Unknown')
                    start = s.get('start', 0) if is_dict else s.start
                    end = s.get('end', 0) if is_dict else s.end
                    avg_logprob = s.get('avg_logprob', 0) if is_dict else getattr(s, 'avg_logprob', 0)
                    
                    if filter_segment(text, avg_logprob):
                        temp_segments.append({
                            "start": start, "end": end, "text": text, 
                            "speaker": speaker, "avg_logprob": avg_logprob
                        })
                
                # Identify roles for Primary mode
                role_mapping = identify_speaker_roles(temp_segments, client_code)
            else:
                # FALLBACK MODE: Gap-based best-effort diarization
                current_speaker = "Speaker A"
                last_end_time = 0
                for s in raw_segments:
                    is_dict = isinstance(s, dict)
                    text = s.get('text', '').strip() if is_dict else s.text.strip()
                    start = s.get('start', 0) if is_dict else s.start
                    end = s.get('end', 0) if is_dict else s.end
                    avg_logprob = s.get('avg_logprob', 0) if is_dict else getattr(s, 'avg_logprob', 0)
                    
                    if start - last_end_time > 1.2:
                        current_speaker = "Speaker B" if current_speaker == "Speaker A" else "Speaker A"
                    
                    if filter_segment(text, avg_logprob):
                        temp_segments.append({
                            "start": start, "end": end, "text": text, 
                            "speaker": current_speaker, "avg_logprob": avg_logprob
                        })
                    last_end_time = end
                
                # In fallback, map A/B to Speaker 1/2
                role_mapping = {"Speaker A": "Speaker 1", "Speaker B": "Speaker 2"}
            
            # STEP 5: Final Enrichment and Role Mapping
            processed_segments = []
            final_raw_segments = []
            
            for i, s in enumerate(temp_segments):
                speaker_id = s['speaker']
                role = role_mapping.get(speaker_id, speaker_id)
                text = s['text']
                confidence = round((1 + s['avg_logprob']) * 100, 1)
                confidence = max(0, min(100, confidence))
                
                segment_data = {
                    'segment_id': f"SEG-{i+1:03d}",
                    'language': language or detected_language,
                    'start_time': round(s['start'], 2),
                    'end_time': round(s['end'], 2),
                    'speaker': role,
                    'transcript': text,
                    'intent': tag(text, INTENT_RULES),
                    'sentiment': tag(text, SENTIMENT_RULES),
                    'outcome': tag(text, OUTCOME_RULES),
                    'key_signal': get_key_signal(text),
                    'confidence': confidence,
                    'qa_status': calculate_qa_status(confidence),
                    'notes': ''
                }
                processed_segments.append(segment_data)
                final_raw_segments.append({**s, "speaker": role})

            # STEP 6: Save Results
            transcript_filename = f"{pure_filename}_transcript.json"

            transcript_path = transcripts_dir / transcript_filename
            with open(transcript_path, 'w', encoding='utf-8') as f:
                json.dump({"language": detected_language, "segments": final_raw_segments}, f, indent=2, ensure_ascii=False)
            
            processed_filename = f"{pure_filename}_processed.json"

            processed_path = outputs_dir / processed_filename
            with open(processed_path, 'w', encoding='utf-8') as f:
                json.dump({"segments": processed_segments, "language": detected_language}, f, indent=2, ensure_ascii=False)
            
            # STEP 7: Upload transcript + processed JSON to Azure, cleanup local audio
            for blob_name, local_path in [
                (f"{client_code}/{transcript_filename}", transcript_path),
                (f"{client_code}/{processed_filename}", processed_path),
            ]:
                bc = blob_service_client.get_blob_client(container="processing", blob=blob_name)
                with open(local_path, 'rb') as f:
                    bc.upload_blob(f, overwrite=True)

            if os.path.exists(local_audio_path):
                os.remove(local_audio_path)

            high_conf = sum(1 for s in processed_segments if s['confidence'] >= 65)
            review_req = sum(1 for s in processed_segments if 40 <= s['confidence'] < 65)

            processed_blob = f"{client_code}/{processed_filename}"

            return {
                "status": "success", "segments": len(processed_segments),
                "high_confidence": high_conf, "review_needed": review_req,
                "processed_file": str(processed_path),
                "processed_blob": processed_blob,
                "client_code": client_code,
                "original_filename": pure_filename, "engine": "gpt-4o", "language": detected_language
            }

        except Exception as e:
            error_msg = f"Error in processor: {str(e)}"
            print(error_msg)
            return {"status": "error", "error": error_msg}
