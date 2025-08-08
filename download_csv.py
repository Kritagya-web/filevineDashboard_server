#!/usr/bin/env python
import requests
from pathlib import Path
import sys

def main():
    # 1) Endpoint you want to fetch (adjust if you're using your cloudflare tunnel URL)
    URL = "http://localhost:8000/export/full.csv"

    # 2) Build your target path in Downloads
    downloads = Path.home() / "Downloads"
    downloads.mkdir(exist_ok=True)
    out_file = downloads / "filevine_full_export.csv"

    # 3) Fetch + stream to disk
    try:
        resp = requests.get(URL, stream=True, timeout=30)
        resp.raise_for_status()
        with open(out_file, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"✅ CSV saved to {out_file}")
    except Exception as e:
        print(f"❌ Failed to download: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
