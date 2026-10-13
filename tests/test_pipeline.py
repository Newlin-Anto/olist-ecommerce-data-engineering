from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from ecommerce_de.pipeline import run_pipeline  # noqa: E402


def write_csv(folder: Path, filename: str, rows: list[dict]) -> None:
    pd.DataFrame(rows).to_csv(folder / filename, index=False)


@pytest.fixture
def sample_project(tmp_path: Path) -> Path:
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    write_csv(raw, "olist_customers_dataset.csv", [
        {"customer_id": "c1", "customer_unique_id": "u1", "customer_zip_code_prefix": "2140", "customer_city": " sao paulo ", "customer_state": "sp"},
    ])
    write_csv(raw, "olist_geolocation_dataset.csv", [
        {"geolocation_zip_code_prefix": "02140", "geolocation_lat": -23.5, "geolocation_lng": -46.6, "geolocation_city": "sao paulo", "geolocation_state": "sp"},
    ])
    write_csv(raw, "olist_order_items_dataset.csv", [
        {"order_id": "o1", "order_item_id": 1, "product_id": "p1", "seller_id": "s1", "shipping_limit_date": "2024-01-03 12:00:00", "price": 100.0, "freight_value": 10.0},
    ])
    write_csv(raw, "olist_order_payments_dataset.csv", [
        {"order_id": "o1", "payment_sequential": 1, "payment_type": "credit_card", "payment_installments": 1, "payment_value": 110.0},
    ])
    write_csv(raw, "olist_order_reviews_dataset.csv", [
        {"review_id": "r1", "order_id": "o1", "review_score": 5, "review_comment_title": None, "review_comment_message": None, "review_creation_date": "2024-01-05 00:00:00", "review_answer_timestamp": "2024-01-06 00:00:00"},
        # The ETL should warn and preserve a repeated source review ID.
        {"review_id": "r1", "order_id": "o2", "review_score": 5, "review_comment_title": None, "review_comment_message": None, "review_creation_date": "2024-01-05 00:00:00", "review_answer_timestamp": "2024-01-06 00:00:00"},
    ])
    write_csv(raw, "olist_orders_dataset.csv", [
        {"order_id": "o1", "customer_id": "c1", "order_status": "delivered", "order_purchase_timestamp": "2024-01-01 10:00:00", "order_approved_at": "2024-01-01 10:05:00", "order_delivered_carrier_date": "2024-01-02 10:00:00", "order_delivered_customer_date": "2024-01-04 10:00:00", "order_estimated_delivery_date": "2024-01-06 00:00:00"},
        {"order_id": "o2", "customer_id": "c1", "order_status": "canceled", "order_purchase_timestamp": "2024-01-02 10:00:00", "order_approved_at": None, "order_delivered_carrier_date": None, "order_delivered_customer_date": None, "order_estimated_delivery_date": "2024-01-07 00:00:00"},
    ])
    write_csv(raw, "olist_products_dataset.csv", [
        {"product_id": "p1", "product_category_name": "beleza_saude", "product_name_lenght": 10, "product_description_lenght": 20, "product_photos_qty": 1, "product_weight_g": 100, "product_length_cm": 10, "product_height_cm": 5, "product_width_cm": 5},
    ])
    write_csv(raw, "olist_sellers_dataset.csv", [
        {"seller_id": "s1", "seller_zip_code_prefix": "2140", "seller_city": "sao paulo", "seller_state": "sp"},
    ])
    write_csv(raw, "product_category_name_translation.csv", [
        {"product_category_name": "beleza_saude", "product_category_name_english": "health_beauty"},
    ])
    return tmp_path


def test_full_pipeline_creates_outputs_and_preserves_rows(sample_project: Path) -> None:
    report = run_pipeline(sample_project)
    assert report["summary"]["checks_failed"] == 0
    assert (sample_project / "data/processed/staging/orders.csv").exists()
    assert (sample_project / "data/processed/curated/fact_order_items.csv").exists()
    assert (sample_project / "reports/data_quality_report.json").exists()

    customers = pd.read_csv(sample_project / "data/processed/curated/dim_customers.csv", dtype=str)
    assert customers.loc[0, "customer_zip_code_prefix"] == "02140"

    items = pd.read_csv(sample_project / "data/processed/curated/fact_order_items.csv")
    assert len(items) == 1
    assert items.loc[0, "item_total"] == pytest.approx(110.0)

    reviews = pd.read_csv(sample_project / "data/processed/curated/fact_reviews.csv")
    assert len(reviews) == 2
    assert reviews["review_record_id"].is_unique
    assert reviews["review_id"].duplicated().sum() == 1
    reuse_check = next(c for c in report["checks"] if c["name"] == "source_review_id_reuse")
    assert reuse_check["status"] == "WARN"


def test_pipeline_fails_when_required_file_is_missing(sample_project: Path) -> None:
    (sample_project / "data/raw/olist_sellers_dataset.csv").unlink()
    with pytest.raises(FileNotFoundError):
        run_pipeline(sample_project)


def test_pipeline_rejects_source_schema_missing_required_column(sample_project: Path) -> None:
    seller_file = sample_project / "data/raw/olist_sellers_dataset.csv"
    pd.DataFrame([{"seller_id": "s1", "seller_city": "sao paulo"}]).to_csv(
        seller_file, index=False
    )
    with pytest.raises(ValueError, match="missing required columns"):
        run_pipeline(sample_project)


def test_raw_s3_manifest_maps_only_csvs(tmp_path: Path) -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    from upload_raw_to_s3 import collect_raw_manifest

    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True)
    (raw / "olist_orders_dataset.csv").write_text("order_id\no1\n", encoding="utf-8")
    (raw / "notes.txt").write_text("not data", encoding="utf-8")

    manifest = collect_raw_manifest(tmp_path, "my-project")
    assert len(manifest) == 1
    assert manifest[0][1] == "my-project/bronze/raw/olist_orders_dataset.csv"


def test_raw_s3_manifest_fails_when_no_csvs_exist(tmp_path: Path) -> None:
    sys.path.insert(0, str(ROOT / "scripts"))
    from upload_raw_to_s3 import collect_raw_manifest

    (tmp_path / "data" / "raw").mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="No CSV files"):
        collect_raw_manifest(tmp_path)
