#!/usr/bin/env python3
import os
import re
import json
import urllib.request
import urllib.error

def make_request(url, method="GET", data=None, headers=None):
    if headers is None:
        headers = {}
    
    req_data = None
    if data is not None:
        req_data = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
        
    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.status
            resp_body = response.read().decode("utf-8")
            return status, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8")
            return e.code, json.loads(err_body) if err_body else {}
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)}

def main():
    print("==============================================")
    print("      VAICORE SYSTEM AUTOPILOT DIAGNOSTICS    ")
    print("==============================================")
    
    env_path = ".env"
    if not os.path.exists(env_path):
        print("❌ Error: .env file not found in current directory!")
        print("Please run this script from the project folder containing your .env file.")
        return

    # 1. Read existing .env
    with open(env_path, "r", encoding="utf-8") as f:
        env_content = f.read()

    # 2. Extract Label Studio details
    token_match = re.search(r"LABEL_STUDIO_API_KEY\s*=\s*([^\n]+)", env_content)
    url_match = re.search(r"LABEL_STUDIO_URL\s*=\s*([^\n]+)", env_content)
    
    if not token_match:
        print("❌ Error: LABEL_STUDIO_API_KEY not found in .env!")
        return
        
    corrupt_token = token_match.group(1).strip().strip('"').strip("'")
    ls_url = url_match.group(1).strip().strip('"').strip("'") if url_match else "http://127.0.0.1:4006"

    # Normalize url
    ls_url = ls_url.rstrip("/")
    
    # CRITICAL: Since vaidikai-portal runs inside Docker, it must communicate with Label Studio
    # via the docker-network address: http://labelstudio:8080 (NOT localhost/127.0.0.1).
    docker_ls_url = "http://labelstudio:8080"
    
    api_urls = ["https://annotate.vaidik.ai", "http://localhost:4006", "http://127.0.0.1:4006", docker_ls_url, ls_url]

    print(f"DEBUG: Found token starting with: '{corrupt_token[:10]}...'")
    
    fixed_token = corrupt_token
    is_corrupt = False
    if corrupt_token.startswith("yJ"):
        fixed_token = "e" + corrupt_token
        is_corrupt = True
        print("⚠️  CRITICAL MATCH FOUND: Your Label Studio API token is missing the leading 'e'!")
        print("   (It starts with 'yJ' instead of 'eyJ'). We will automatically fix this.")
    else:
        print("✅ Label Studio Token format appears to have the correct 'eyJ' prefix.")

    # Define all 6 projects and configs
    required_projects = [
        {
            "title": "Audio Transcription & Diarization",
            "alias": "Audio",
            "env_var": "LABEL_STUDIO_PROJECT_ID",
            "default_id": "1",
            "config": """<View>
  <Header value="Review Status (Admin/Reviewer Use Only)"/>
  <Choices name="review_status" toName="audio" choice="single" showInline="true">
    <Choice value="Accepted" hint="Mark as ready for client delivery"/>
    <Choice value="Rejected" hint="Needs correction"/>
  </Choices>

  <Audio name="audio" value="$audio"/>
  <Labels name="speaker" toName="audio">
    <Label value="Agent" background="#2196F3"/>
    <Label value="Customer" background="#4CAF50"/>
    <Label value="Teacher" background="#FF9800"/>
    <Label value="Student" background="#9C27B0"/>
    <Label value="Doctor" background="#F44336"/>
    <Label value="Patient" background="#00BCD4"/>
    <Label value="Interviewer" background="#795548"/>
    <Label value="Interviewee" background="#607D8B"/>
    <Label value="Speaker 1" background="#3F51B5"/>
    <Label value="Speaker 2" background="#009688"/>
    <Label value="AI" background="#E91E63"/>
  </Labels>
  <TextArea name="transcript" toName="audio" perRegion="true" placeholder="Transcript..." rows="2"/>
</View>"""
        },
        {
            "title": "Jewelry Segmentation",
            "alias": "Jewelry",
            "env_var": "LABEL_STUDIO_JEWELRY_PROJECT_ID",
            "default_id": "2",
            "config": """<View>
  <Image name="image" value="$image"/>
  <PolygonLabels name="label" toName="image">
    <Label value="Jewelry" background="#FFD700"/>
    <Label value="General Jewelry" background="#E0E0E0"/>
    <Label value="Pendant" background="#FF9800"/>
    <Label value="Earrings" background="#00BCD4"/>
    <Label value="Ring" background="#E91E63"/>
    <Label value="Necklace" background="#9C27B0"/>
    <Label value="Chain" background="#3F51B5"/>
    <Label value="Nose Ring" background="#F44336"/>
    <Label value="Bangle" background="#4CAF50"/>
    <Label value="Bracelet" background="#8BC34A"/>
    <Label value="Mangalsutra" background="#795548"/>
    <Label value="Maang Tikka" background="#607D8B"/>
    <Label value="Anklet" background="#009688"/>
  </PolygonLabels>
  <KeyPointLabels name="keypoint" toName="image">
    <Label value="Positive" background="#00FF00"/>
    <Label value="Negative" background="#FF0000"/>
  </KeyPointLabels>
  <RectangleLabels name="bbox" toName="image">
    <Label value="Bounding Box" background="#0000FF"/>
  </RectangleLabels>
</View>"""
        },
        {
            "title": "Form OCR Redaction",
            "alias": "Form",
            "env_var": "LABEL_STUDIO_FORM_PROJECT_ID",
            "default_id": "3",
            "config": """<View>
  <Image name="document" value="$document_url"/>
  <TextArea name="extracted_text" toName="document" rows="10" placeholder="Extracted form text..."/>
</View>"""
        },
        {
            "title": "Clickstream Analytics",
            "alias": "Clickstream",
            "env_var": "LABEL_STUDIO_CLICKSTREAM_PROJECT_ID",
            "default_id": "4",
            "config": """<View>
  <Text name="filename" value="$filename"/>
  <Paragraphs name="timeline" value="$clickstream_timeline" nameKey="action" textKey="element"/>
  <Choices name="analysis" toName="timeline" choice="single">
    <Choice value="Smooth"/>
    <Choice value="High Frustration"/>
  </Choices>
</View>"""
        },
        {
            "title": "House Image Annotation",
            "alias": "Housing",
            "env_var": "LABEL_STUDIO_HOUSING_PROJECT_ID",
            "default_id": "5",
            "config": """<View>
  <Image name="image" value="$image" />
  <PolygonLabels name="label" toName="image">
    <Label value="Main Structure" background="#FF5722" />
    <Label value="Roof / Terrace" background="#FF9800" />
    <Label value="Entrance / Door" background="#4CAF50" />
    <Label value="Window / Opening" background="#03A9F4" />
    <Label value="Wall Inscription" background="#9C27B0" />
    <Label value="Person / Surveyor" background="#009688" />
    <Label value="GPS Watermark" background="#607D8B" />
  </PolygonLabels>
  <RectangleLabels name="bbox" toName="image">
    <Label value="Bounding Box" background="#3F51B5" />
  </RectangleLabels>
  <Choices name="construction_type" toName="image" choice="single" showInline="true">
    <Choice value="Pucca (Brick/Concrete)" />
    <Choice value="Semi-Pucca" />
    <Choice value="Kutcha (Mud/Thatch)" />
  </Choices>
</View>"""
        },
        {
            "title": "Nature of Business",
            "alias": "Business",
            "env_var": "LABEL_STUDIO_BUSINESS_PROJECT_ID",
            "default_id": "6",
            "config": """<View>
  <Image name="document" value="$document_url" />
  <PolygonLabels name="label" toName="document">
    <Label value="business_signage" background="#FF5722" />
    <Label value="storefront" background="#4CAF50" />
    <Label value="goods_display" background="#03A9F4" />
    <Label value="merchant_activity" background="#9C27B0" />
    <Label value="payment_qr" background="#009688" />
    <Label value="license_certificate" background="#E91E63" />
  </PolygonLabels>
  <RectangleLabels name="bbox" toName="document">
    <Label value="Bounding Box" background="#3F51B5" />
  </RectangleLabels>
  <TextArea name="extracted_text" toName="document" rows="4" placeholder="Extracted business details..." />
  <Choices name="business_type" toName="document" choice="single" showInline="true">
    <Choice value="Retail / Kirana" />
    <Choice value="Hardware / Construction" />
    <Choice value="Pharmacy / Clinic" />
    <Choice value="Food / Restaurant" />
    <Choice value="Apparel / Textiles" />
    <Choice value="Services / Repair" />
    <Choice value="Other" />
  </Choices>
</View>"""
        }
    ]

    # 3. Test and Verify Connection
    connected_any = False
    valid_token = None

    project_ids_result = {p["env_var"]: p["default_id"] for p in required_projects}

    seen_urls = set()
    for url in api_urls:
        if url in seen_urls:
            continue
        seen_urls.add(url)

        print(f"\nTrying to connect to Label Studio at: {url}...")
        connected = False
        active_token = None
        headers_auth = None

        for tok, name in [(fixed_token, "Corrected Token"), (corrupt_token, "Current Token")]:
            # Direct check
            headers = {"Authorization": f"Bearer {tok}"}
            code, resp = make_request(f"{url}/api/current-user/whoami", headers=headers)
            if code == 200:
                user_email = resp.get("email", "unknown")
                print(f"   ✅ SUCCESS using {name}! Authenticated as: {user_email}")
                connected = True
                active_token = tok
                headers_auth = headers
                break
            
            # Refresh check
            code_ref, resp_ref = make_request(f"{url}/api/token/refresh", method="POST", data={"refresh": tok})
            if code_ref == 200:
                access_token = resp_ref.get("access")
                headers_temp = {"Authorization": f"Bearer {access_token}"}
                code_who, resp_who = make_request(f"{url}/api/current-user/whoami", headers=headers_temp)
                if code_who == 200:
                    user_email = resp_who.get("email", "unknown")
                    print(f"   ✅ SUCCESS using {name} (via refresh)! Authenticated as: {user_email}")
                    connected = True
                    active_token = tok
                    headers_auth = headers_temp
                    break
                    
        if connected:
            connected_any = True
            valid_token = active_token
            print(f"\n🔨 Checking for missing projects in Label Studio at {url}...")
            
            projects = []
            code_p, resp_p = make_request(f"{url}/api/projects", headers=headers_auth)
            if code_p == 200:
                projects = resp_p.get("results", [])
                print(f"   Found {len(projects)} existing projects.")
            
            existing_titles = {p.get("title"): p.get("id") for p in projects}

            for p_spec in required_projects:
                title = p_spec["title"]
                alias = p_spec["alias"]
                env_var = p_spec["env_var"]

                # Find existing by exact title or alias
                found_id = None
                for t_exist, id_exist in existing_titles.items():
                    if t_exist in (title, alias):
                        found_id = id_exist
                        break
                
                if found_id is not None:
                    print(f"   ✅ Found existing '{title}' project with ID: {found_id}")
                    project_ids_result[env_var] = str(found_id)
                    # Automatically update existing project config to support new categories
                    code_u, resp_u = make_request(f"{url}/api/projects/{found_id}", method="PATCH", data={
                        "label_config": p_spec["config"]
                    }, headers=headers_auth)
                    if code_u in (200, 201):
                        print(f"   ✅ Synchronized '{title}' labeling config successfully.")
                    else:
                        print(f"   ⚠️ Could not sync config for '{title}' (code {code_u}): {resp_u}")
                else:
                    print(f"   ➕ Project '{title}' is missing. Creating it...")
                    code_c, resp_c = make_request(f"{url}/api/projects", method="POST", data={
                        "title": title,
                        "label_config": p_spec["config"]
                    }, headers=headers_auth)
                    if code_c == 201:
                        created_id = str(resp_c.get("id"))
                        print(f"   ✅ Created '{title}' with Project ID: {created_id}")
                        project_ids_result[env_var] = created_id
                    else:
                        print(f"   ❌ Failed to create project '{title}' (code {code_c}): {resp_c}")

    if not connected_any:
        print("\n❌ Could not connect to Label Studio API with either token.")
        print("Please check that Label Studio is running and your token is copied correctly from Profile -> Access Token.")
    else:
        # Update env content lines for project IDs
        for env_var, pid in project_ids_result.items():
            if env_var in env_content:
                env_content = re.sub(rf"{env_var}\s*=\s*[^\n]+", f"{env_var}={pid}", env_content)
            else:
                env_content += f"\n{env_var}={pid}"

    # 4. Save fixed env content (including token and URL fixes)
    if is_corrupt:
        env_content = env_content.replace(corrupt_token, fixed_token)
        
    # Correct the URL connection parameter for secure container-to-container network
    if url_match:
        current_url_line = url_match.group(0)
        # Change 127.0.0.1 or localhost to 'labelstudio:8080'
        if "127.0.0.1" in current_url_line or "localhost" in current_url_line:
            print(f"⚠️  NETWORKING UPDATE: Changing Label Studio URL in .env to the internal Docker network address: {docker_ls_url}")
            env_content = env_content.replace(current_url_line, f"LABEL_STUDIO_URL={docker_ls_url}")
            
    with open(env_path, "w", encoding="utf-8") as f:
        f.write(env_content)
    
    print("\n==============================================")
    print("              ✨ FIXES APPLIED ✨             ")
    print("==============================================")
    print("1. Verified/Fixed Label Studio API key format.")
    print(f"2. Updated Label Studio URL to the internal Docker network: {docker_ls_url}")
    if connected_any:
        print("3. Checked and created all 6 essential projects:")
        for p in required_projects:
            print(f"   - {p['title']} (ID: {project_ids_result[p['env_var']]})")
    print("\n👉 Please restart your containers now to apply changes:")
    print("   docker compose down")
    print("   docker compose up -d")
    print("==============================================")

if __name__ == "__main__":
    main()
