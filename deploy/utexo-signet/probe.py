#!/usr/bin/env python3
"""Read-only UTEXO chain/deployment probe. Never reads wallet secrets."""
import argparse
import json
import urllib.request
from datetime import datetime, timezone

CHECKPOINT = "0000027606cb73bbf383cb8666bd402dd0de1e154c39688db7b72a6cb4b56a17"


def get(url):
    with urllib.request.urlopen(url, timeout=20) as response:
        return response.read().decode().strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--esplora", default="https://esplora-api.utexo.com")
    parser.add_argument("--daemon", default="http://127.0.0.1:18787")
    parser.add_argument("--address")
    args = parser.parse_args()
    chain = args.esplora.rstrip("/")
    checkpoint = get(chain + "/block-height/100")
    if checkpoint != CHECKPOINT:
        raise SystemExit("Wrong chain: height-100 checkpoint does not match UTEXO integration baseline")
    report = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "esplora": chain,
        "height_100": checkpoint,
        "tip_height": int(get(chain + "/blocks/tip/height")),
        "catalog": json.loads(get(args.daemon.rstrip("/") + "/v1/tokens/list")),
    }
    if args.address:
        report["address"] = args.address
        report["utxos"] = json.loads(get(chain + "/address/" + args.address + "/utxo"))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
