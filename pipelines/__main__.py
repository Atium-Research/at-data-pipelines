import os
from prefect import flow


@flow
def my_flow():
    print("Hello, Prefect!")


if __name__ == "__main__":
    cf_access_client_id = os.getenv("CF_ACCESS_CLIENT_ID")
    cf_access_client_secret = os.getenv("CF_ACCESS_CLIENT_SECRET")

    if cf_access_client_id and cf_access_client_secret:
        os.environ["PREFECT_CLIENT_CUSTOM_HEADERS"] = f'{{"CF-Access-Client-Id": "{cf_access_client_id}", "CF-Access-Client-Secret": "{cf_access_client_secret}"}}'

    my_flow.serve(name="my-first-deployment", cron="* * * * *")