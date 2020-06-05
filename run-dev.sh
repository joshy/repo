#!/bin/sh
nodemon -e py --ignore notebooks --ignore logs --ignore venv --exec "python" runserver.py
