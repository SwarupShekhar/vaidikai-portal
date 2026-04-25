# Vaidik AI Client Portal

A secure data intake portal for enterprise clients to upload data files and download completed annotations.

## Features

- Client authentication via access codes
- Secure file uploads to Azure Blob Storage
- File status tracking (Received, Processing, Completed)
- Direct downloads via time-limited SAS URLs
- Mobile-responsive dark theme UI

## Setup

1. Clone or create the project directory:
   ```
   mkdir vaidikai-portal
   cd vaidikai-portal
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Copy environment template:
   ```
   cp .env.example .env
   ```

4. Fill in your Azure credentials in `.env`:
   - `AZURE_STORAGE_ACCOUNT_NAME=vaidikaidata`
   - `AZURE_STORAGE_ACCOUNT_KEY=your_key_here`
   - `AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here`
   - `AZURE_TENANT_ID=your_tenant_id_here`
   - `SECRET_KEY=your_random_secret_for_tokens` (generate a random string)
   - `ALLOWED_ORIGINS=https://vaidik.ai,http://localhost:8000`

5. Run the application:
   ```
   uvicorn main:app --reload --port 8000
   ```

6. Open your browser to `http://localhost:8000/static/index.html`

## Deployment

Deploy to Azure App Service with Python 3.11:

- Startup command: `uvicorn main:app --host 0.0.0.0 --port 8000`
- Ensure `.env` is configured with production values
- Set CORS origins appropriately for your domain

## Client Access Codes

Currently hardcoded for demo:
- `CLIENT001` - Client One
- `CLIENT002` - Client Two

## File Requirements

- Max size: 500MB
- Allowed types: MP3, WAV, M4A, PDF, DOCX, JPG, PNG, JSON, CSV, TXT

## Security

- All credentials stored in `.env` (gitignored)
- Client-specific subfolders in Azure containers
- SAS tokens for downloads (1-hour expiry)
- File type and size validation
- No direct Azure URLs exposed to clients