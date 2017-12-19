#!/bin/sh
docker build . -t datapipes:latest && docker run -it -p 8000:8000 datapipes:latest
