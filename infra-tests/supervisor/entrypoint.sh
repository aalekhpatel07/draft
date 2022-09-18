#!/usr/bin/sh

set -eux;

source /var/venv/supervisor/bin/activate
pip install -r requirements.txt

uvicorn main:app --reload --host 0.0.0.0 --port 8000