#!/bin/sh

gunicorn --bind 0.0.0.0:8000 datapipes.server:app --worker-class aiohttp.GunicornWebWorker
