#!/usr/bin/env python3

import json
from typing import Any, Dict

import google.auth.transport.requests
import google.oauth2.id_token
import requests

# Your engine URLs
ENGINES = {
    "pandas": "https://pandas-processor-253940201126.us-central1.run.app",
    "polars": "https://polars-processor-253940201126.us-central1.run.app",
    "dask": "https://dask-processor-253940201126.us-central1.run.app",
}


def get_auth_token(url: str) -> str:
    """Get authentication token for Cloud Run."""
    try:
        auth_req = google.auth.transport.requests.Request()
        token = google.oauth2.id_token.fetch_id_token(auth_req, url)
        return token
    except Exception as e:
        print(f"Failed to get auth token: {e}")
        return None


def check_engine_health(name: str, url: str) -> Dict[str, Any]:
    """Check health of a single engine."""
    print(f"\nTesting {name} engine...")
    print(f"URL: {url}")

    # Try with authentication first
    try:
        token = get_auth_token(url)
        if token:
            headers = {"Authorization": f"Bearer {token}"}
            response = requests.get(f"{url}/health", headers=headers, timeout=60)
        else:
            # Try without auth
            response = requests.get(f"{url}/health", timeout=60)

        if response.status_code == 200:
            data = response.json()
            print(
                f"PASS {name}: {data.get('status', 'unknown')} - Engine: {data.get('engine', 'unknown')}"
            )
            return {"status": "healthy", "details": data}
        else:
            print(f"FAIL {name}: HTTP {response.status_code} - {response.text}")
            return {
                "status": "unhealthy",
                "error": f"HTTP {response.status_code}",
                "details": response.text,
            }

    except requests.exceptions.Timeout:
        print(f"TIMEOUT {name}: Timeout (service may be cold starting)")
        return {"status": "timeout", "error": "Request timeout"}
    except requests.exceptions.ConnectionError as e:
        print(f"CONNECTION_ERROR {name}: Connection Error - {str(e)}")
        return {"status": "connection_error", "error": str(e)}
    except Exception as e:
        print(f"ERROR {name}: Unexpected Error - {str(e)}")
        return {"status": "error", "error": str(e)}


def main():
    """Test all processing engines."""
    print("Testing Multi-Tenant ML Pipeline Processing Engines\n")

    results = {}
    healthy_count = 0

    for name, url in ENGINES.items():
        result = check_engine_health(name, url)
        results[name] = result
        if result["status"] == "healthy":
            healthy_count += 1

    # Summary
    print(f"\nSUMMARY:")
    print(f"Healthy engines: {healthy_count}/{len(ENGINES)}")

    if healthy_count == len(ENGINES):
        print("All engines are healthy!")
    elif healthy_count > 0:
        print("Some engines have issues")
    else:
        print("All engines are unavailable")

    # Detailed results
    print(f"\nDETAILED RESULTS:")
    print(json.dumps(results, indent=2))

    return results


if __name__ == "__main__":
    main()
