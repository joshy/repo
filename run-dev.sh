#!/bin/sh
echo "Running on port 7777"
flask --app "web:create_app()" --debug run --host 0.0.0.0 --port 7777