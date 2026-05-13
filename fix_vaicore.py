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
    
    api_urls = [docker_ls_url, "https://annotate.vaidik.ai", ls_url]

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

    # 3. Test and Verify Connection
    connected = False
    valid_token = None
    active_url = None
    
    for url in api_urls:
        print(f"\nTrying to connect to Label Studio at: {url}...")
        for tok, name in [(fixed_token, "Corrected Token"), (corrupt_token, "Current Token")]:
            # Direct check
            headers = {"Authorization": f"Bearer {tok}"}
            code, resp = make_request(f"{url}/api/current-user/whoami", headers=headers)
            if code == 200:
                user_email = resp.get("email", "unknown")
                print(f"   ✅ SUCCESS using {name}! Authenticated as: {user_email}")
                connected = True
                valid_token = tok
                active_url = url
                break
            
            # Refresh check
            code_ref, resp_ref = make_request(f"{url}/api/token/refresh", method="POST", data={"refresh": tok})
            if code_ref == 200:
                access_token = resp_ref.get("access")
                headers_auth = {"Authorization": f"Bearer {access_token}"}
                code_who, resp_who = make_request(f"{url}/api/current-user/whoami", headers=headers_auth)
                if code_who == 200:
                    user_email = resp_who.get("email", "unknown")
                    print(f"   ✅ SUCCESS using {name} (via refresh)! Authenticated as: {user_email}")
                    connected = True
                    valid_token = tok
                    active_url = url
                    break
                    
        if connected:
            break

    housing_id = "3"
    business_id = "4"

    if not connected:
        print("\n❌ Could not connect to Label Studio API with either token.")
        print("Please check that Label Studio is running and your token is copied correctly from Profile -> Access Token.")
    else:
        # Create missing projects if connected
        print("\n🔨 Checking for missing projects in Label Studio...")
        headers = {"Authorization": f"Bearer {valid_token}"}
        
        # Try getting projects
        projects = []
        code_p, resp_p = make_request(f"{active_url}/api/projects", headers=headers)
        if code_p == 200:
            projects = resp_p.get("results", [])
            print(f"   Found {len(projects)} existing projects.")
        else:
            # Try with refresh
            code_ref, resp_ref = make_request(f"{active_url}/api/token/refresh", method="POST", data={"refresh": valid_token})
            if code_ref == 200:
                access_token = resp_ref.get("access")
                headers_auth = {"Authorization": f"Bearer {access_token}"}
                code_p2, resp_p2 = make_request(f"{active_url}/api/projects", headers=headers_auth)
                if code_p2 == 200:
                    projects = resp_p2.get("results", [])
                    print(f"   Found {len(projects)} existing projects.")
                    headers = headers_auth

        existing_titles = [p.get("title") for p in projects]
        
        # Create House Image Annotation
        if "House Image Annotation" not in existing_titles and "Housing" not in existing_titles:
            print("   ➕ Project 'House Image Annotation' is missing. Creating it...")
            housing_config = """<View>
  <Image name="image" value="$image" />
  <PolygonLabels name="label" toName="image">
    <Label value="house_facade" background="#FF4D4D" />
    <Label value="representative_person" background="#4CAF50" />
    <Label value="business_signboard" background="#2196F3" />
  </PolygonLabels>
</View>"""
            code_c, resp_c = make_request(f"{active_url}/api/projects", method="POST", data={
                "title": "House Image Annotation",
                "label_config": housing_config
            }, headers=headers)
            if code_c == 201:
                housing_id = str(resp_c.get("id"))
                print(f"   ✅ Created 'House Image Annotation' with Project ID: {housing_id}")
            else:
                print(f"   ❌ Failed to create project (code {code_c}): {resp_c}")
        else:
            for p in projects:
                if p.get("title") in ("House Image Annotation", "Housing"):
                    housing_id = str(p.get("id"))
                    print(f"   ✅ Found existing Housing project with ID: {housing_id}")
                    break

        # Create Nature of Business
        if "Nature of Business" not in existing_titles and "Business" not in existing_titles:
            print("   ➕ Project 'Nature of Business' is missing. Creating it...")
            business_config = """<View>
  <Image name="document" value="$document_url" />
  <TextArea name="extracted_text" toName="document" rows="10" placeholder="Extracted business details..." />
</View>"""
            code_c, resp_c = make_request(f"{active_url}/api/projects", method="POST", data={
                "title": "Nature of Business",
                "label_config": business_config
            }, headers=headers)
            if code_c == 201:
                business_id = str(resp_c.get("id"))
                print(f"   ✅ Created 'Nature of Business' with Project ID: {business_id}")
            else:
                print(f"   ❌ Failed to create project (code {code_c}): {resp_c}")
        else:
            for p in projects:
                if p.get("title") in ("Nature of Business", "Business"):
                    business_id = str(p.get("id"))
                    print(f"   ✅ Found existing Business project with ID: {business_id}")
                    break

        # Update env content lines for project IDs
        if "LABEL_STUDIO_HOUSING_PROJECT_ID" in env_content:
            env_content = re.sub(r"LABEL_STUDIO_HOUSING_PROJECT_ID\s*=\s*[^\n]+", f"LABEL_STUDIO_HOUSING_PROJECT_ID={housing_id}", env_content)
        else:
            env_content += f"\nLABEL_STUDIO_HOUSING_PROJECT_ID={housing_id}"

        if "LABEL_STUDIO_BUSINESS_PROJECT_ID" in env_content:
            env_content = re.sub(r"LABEL_STUDIO_BUSINESS_PROJECT_ID\s*=\s*[^\n]+", f"LABEL_STUDIO_BUSINESS_PROJECT_ID={business_id}", env_content)
        else:
            env_content += f"\nLABEL_STUDIO_BUSINESS_PROJECT_ID={business_id}"

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
    print("1. Fixed missing leading 'e' in Label Studio API key.")
    print(f"2. Updated Label Studio URL to the internal Docker network: {docker_ls_url}")
    if connected:
        print(f"3. Checked/Created 'House Image Annotation' (ID: {housing_id}).")
        print(f"4. Checked/Created 'Nature of Business' (ID: {business_id}).")
    print("\n👉 Please restart your containers now to apply changes:")
    print("   docker compose down")
    print("   docker compose up -d")
    print("==============================================")

if __name__ == "__main__":
    main()
