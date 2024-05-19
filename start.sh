#!/usr/bin/env bash

docker-compose down

set -e

docker-compose up -d
python3 Transport_Layer/manage.py runserver --noreload 127.0.0.1:8000

