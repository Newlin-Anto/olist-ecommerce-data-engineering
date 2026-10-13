
"""Upload the nine raw Olist CSVs to a private S3 Bronze/raw prefix."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def collect_raw_manifest(
    root: Path,
    prefix: str = "olist-ecommerce",
) -> list[tuple[Path, str]]:
    """Find local CSV files and their target S3 object keys."""

    raw_dir = root / "data" / "raw"

    files = sorted(
        path for path in raw_dir.glob("*.csv")
        if path.is_file()
    )

    if not files:
        raise FileNotFoundError(
            f"No CSV files found in {raw_dir}. "
            "Copy the Olist CSV files into data/raw/."
        )

    clean_prefix = prefix.strip("/")
    manifest = []

    for path in files:
        key_parts = [
            part
            for part in (
                clean_prefix,
                "bronze",
                "raw",
                path.name,
            )
            if part
        ]

        manifest.append((path, "/".join(key_parts)))

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the upload without sending files to S3.",
    )

    args = parser.parse_args()

    # Import optional AWS dependencies.
    try:
        import boto3
        from boto3.s3.transfer import TransferConfig
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
        from dotenv import load_dotenv

    except ImportError as exc:
        raise SystemExit(
            "Required S3 dependencies are missing. Run:\n"
            "python -m pip install -r requirements-s3.txt"
        ) from exc

    # Load configuration from the project's .env file.
    load_dotenv(ROOT / ".env")

    bucket = os.getenv("S3_BUCKET", "").strip()
    prefix = os.getenv("S3_PREFIX", "olist-ecommerce").strip("/")
    region = os.getenv("AWS_REGION", "").strip() or None
    profile = os.getenv("AWS_PROFILE", "").strip() or None

    if not bucket or bucket == "your-private-bucket-name":
        raise SystemExit(
            "Set S3_BUCKET in your .env file to your private bucket name."
        )

    # Discover the raw files.
    try:
        manifest = collect_raw_manifest(ROOT, prefix)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    total_size = sum(path.stat().st_size for path, _ in manifest)

    print(f"Target bucket: s3://{bucket}/")
    print(f"Region: {region or 'AWS profile default'}")
    print(f"CSV files: {len(manifest)}")
    print(f"Total bytes: {total_size:,}")
    print()

    for path, key in manifest:
        print(
            f"{path.relative_to(ROOT)}"
            f" -> s3://{bucket}/{key}"
        )

    if args.dry_run:
        print("\nDry run only. No files were uploaded.")
        return

    # Use the configured AWS CLI profile.
    session_kwargs = {}

    if profile:
        session_kwargs["profile_name"] = profile

    if region:
        session_kwargs["region_name"] = region

    session = boto3.Session(**session_kwargs)

    # Retry transient AWS API errors and allow longer network responses.
    client_config = Config(
        retries={
            "max_attempts": 10,
            "mode": "standard",
        },
        connect_timeout=30,
        read_timeout=120,
    )

    s3 = session.client(
        "s3",
        config=client_config,
    )

    # Use fewer concurrent transfers and larger multipart chunks.
    transfer_config = TransferConfig(
        multipart_threshold=8 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=2,
        use_threads=True,
    )

    # Upload one file at a time.
    for index, (path, key) in enumerate(manifest, start=1):
        destination = f"s3://{bucket}/{key}"

        print(
            f"\n[{index}/{len(manifest)}] Uploading "
            f"{path.name} ({path.stat().st_size:,} bytes)...",
            flush=True,
        )

        try:
            s3.upload_file(
                str(path),
                bucket,
                key,
                Config=transfer_config,
            )

        except Exception as exc:
            raise SystemExit(
                f"\nUpload failed for: {path.name}\n"
                f"Destination: {destination}\n"
                f"Error type: {type(exc).__name__}\n"
                f"Details: {exc}\n\n"
                "Check network connectivity, AWS credentials, "
                "bucket region, and S3 permissions. "
                "Rerun the script after resolving the issue."
            ) from exc

        print(f"SUCCESS: {destination}", flush=True)

    print("\nRaw upload completed successfully.")
    print(f"Uploaded files: {len(manifest)}")
    print("No public ACLs were added.")


if __name__ == "__main__":
    main()
