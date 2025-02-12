#!/usr/bin/env python

"""
Adaptor to dummy device for debugging purposes
"""

import argparse
import json
import requests

response = requests.get('127.0.0.1:8000')
data = response.text
json.loads(data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:8000")

    flags = parser.parse_args()
