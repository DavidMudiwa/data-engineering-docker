import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
from dotenv import load_dotenv
from tqdm import tqdm
import time

#RUN USING: >docker run --rm   --env-file 3_data-warehouse/.env   -v /home/codespace/.config/gcloud:/root/.config/gcloud:ro   -e GOOGLE_APPLICATION_CREDENTIALS=/root/.config/gcloud/application_default_credentials.json   my-app

load_dotenv()

BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BASE_URL = os.getenv("URL")
MONTHS = [f"{i:02d}" for i in range(1, 7)]
DOWNLOAD_DIR = "."
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 4

os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_gcs_client() -> storage.Client:
    return storage.Client(project=PROJECT_ID)


client = get_gcs_client()
bucket = client.bucket(BUCKET_NAME)


def download_file(month: str) -> str | None:
    url = f"{BASE_URL}{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, f"yellow_tripdata_2024-{month}.parquet")

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name: str) -> None:
    try:
        client.get_bucket(bucket_name)
        project_bucket_names = [b.name for b in client.list_buckets(project=PROJECT_ID)]

        if bucket_name in project_bucket_names:
            print(f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding...")
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, "
                "but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        new_bucket = client.bucket(bucket_name)
        client.create_bucket(new_bucket, project=PROJECT_ID)
        print(f"Created bucket '{bucket_name}'")

    except Forbidden:
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. "
            "Bucket name is taken or you do not have permission."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name: str) -> bool:
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path: str, max_retries: int = 3) -> None:
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return

            print(f"Verification failed for {blob_name}, retrying...")

        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


def run_downloads(months: list[str]) -> list[str]:
    file_paths: list[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_month = {executor.submit(download_file, month): month for month in months}

        with tqdm(total=len(months), desc="Downloading files", unit="file") as pbar:
            for future in as_completed(future_to_month):
                result = future.result()
                if result is not None:
                    file_paths.append(result)
                pbar.update(1)

    return file_paths


def run_uploads(file_paths: list[str]) -> None:
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(upload_to_gcs, file_path) for file_path in file_paths]

        with tqdm(total=len(futures), desc="Uploading files", unit="file") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)


if __name__ == "__main__":
    create_bucket(BUCKET_NAME)

    file_paths = run_downloads(MONTHS)
    run_uploads(file_paths)

    print("All files processed and verified.")