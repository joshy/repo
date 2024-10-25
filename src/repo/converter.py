import json
import logging
import re
import subprocess

from striprtf.striprtf import rtf_to_text


def jjson(meta_data_file):
    if meta_data_file is None:
        return None
    with open(meta_data_file) as f:
        return json.load(f)


def text(report_file):
    if report_file is None:
        return None
    with open(report_file) as f:
        text = f.read()
        return rtf_to_text(text)
