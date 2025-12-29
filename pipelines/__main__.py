import os
import httpx
from prefect import flow


@flow
def my_flow():
    print("Hello, Prefect!")


if __name__ == "__main__":
    # Debug: Print Prefect API URL
    api_url = os.getenv("PREFECT_API_URL", "NOT SET")
    print(f"PREFECT_API_URL: {api_url}")

    # Debug: Test connectivity to Prefect server
    if api_url != "NOT SET":
        try:
            print(f"Testing connection to {api_url}...")
            response = httpx.get(api_url.replace("/api", "/api/health"), timeout=10.0)
            print(f"Health check status: {response.status_code}")
            print(f"Health check response: {response.text[:200]}")
        except Exception as e:
            print(f"Connection test failed: {e}")

    my_flow.serve(name="my-first-deployment", cron="* * * * *")