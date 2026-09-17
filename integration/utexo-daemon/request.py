#!/usr/bin/env python3
"""Sign a test request locally and call a daemon external L1 endpoint.

Usage: request.py SIGNER TEST_WIF API_BASE PAYLOAD_JSON
Payloads and signing keys remain outside the repository. Prepare/finalize must
be explicitly requested; this client never rebuilds or retries a payment.
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request

ROUTES = {
    "receive": "receives/create", "prepare": "transfers/prepare",
    "finalize": "transfers/finalize", "get": "operations/get",
    "list": "operations/list", "refresh": "operations/refresh",
    "cancel": "operations/cancel",
}


def main():
    signer, key, api, path = sys.argv[1:]
    with open(path, encoding="utf-8") as source:
        payload = json.load(source)
    route = ROUTES[payload["action"]["type"]]
    signed = subprocess.check_output([signer, "external-request", key, path])
    request = urllib.request.Request(
        api.rstrip("/") + "/v1/external/" + route,
        data=signed, headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        print(json.dumps({"http_status": error.code, "error": error.read().decode()}))
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
