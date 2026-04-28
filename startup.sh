#!/bin/bash
apt-get install -y ffmpeg -qq 2>/dev/null || true
exec gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app
