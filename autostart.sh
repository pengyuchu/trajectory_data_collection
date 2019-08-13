#!/bin/sh

cd
. ~/superset_venv/bin/activate

python ~/trans_firebase_2_sql/regular_task.py 

gunicorn -b :8080 superset:app
