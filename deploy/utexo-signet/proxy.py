#!/usr/bin/env python3
"""Explicit UTEXO test proxy operations; receiving does not imply RGB validation."""
import argparse
import base64
import hashlib
import json
import pathlib
import urllib.request

ENDPOINT = "https://rgb-proxy.utexo.com/json-rpc"
MAX_RESPONSE = 12 * 1024 * 1024


def rpc(method, params):
    request = urllib.request.Request(
        ENDPOINT,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read(MAX_RESPONSE + 1)
    if len(body) > MAX_RESPONSE:
        raise ValueError("proxy response exceeds limit")
    result = json.loads(body)
    if result.get("error"):
        raise RuntimeError(result["error"])
    return result["result"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["get", "ack", "nack", "ack-status"])
    parser.add_argument("recipient")
    parser.add_argument("--output", type=pathlib.Path)
    args = parser.parse_args()
    if args.mode == "get":
        if args.output is None:
            parser.error("get requires --output")
        result = rpc("consignment.get", {"recipient_id": args.recipient})
        proof = base64.b64decode(result.pop("consignment"), validate=True)
        digest = hashlib.sha256(proof).hexdigest()
        # Never overwrite a different proof under an existing name.
        if args.output.exists():
            if args.output.read_bytes() != proof:
                raise ValueError("existing proof differs")
        else:
            with args.output.open("xb") as target:
                target.write(proof)
        print(json.dumps({**result, "sha256": digest, "bytes": len(proof), "recipient_id": args.recipient}))
    elif args.mode == "ack-status":
        print(json.dumps(rpc("ack.get", {"recipient_id": args.recipient})))
    else:
        print(json.dumps(rpc("ack.post", {"recipient_id": args.recipient, "ack": args.mode == "ack"})))


if __name__ == "__main__":
    main()
