from __future__ import annotations

import os
import sys
from typing import Iterable

import requests


def check(url: str) -> bool:
    try:
        response = requests.get(url, timeout=5)
        print(f"{url} -> {response.status_code} {response.text[:120]}")
        return response.ok
    except Exception as exc:
        print(f"{url} -> ERROR {exc}")
        return False


def main() -> int:
    camera_url = os.getenv("CAMERA_SERVER_URL", "http://localhost:8001")
    storage_url = os.getenv("STORAGE_SERVER_URL", "http://localhost:8003")
    urls: Iterable[str] = [
        f"{camera_url}/health",
        f"{storage_url}/health",
    ]
    ok = all(check(url) for url in urls)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

