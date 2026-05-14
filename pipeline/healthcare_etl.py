from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_CSV_PATH = BASE_DIR / "healthcare_dataset.csv"
PARQUET_DIR = BASE_DIR / "processed_parquet"
PARQUET_FILE = PARQUET_DIR / "healthcare_encrypted.parquet"
EXPORT_DIR = BASE_DIR / "exports"
EXPORT_CSV_PATH = EXPORT_DIR / "name_age_export.csv"
VERIFY_REPORT_PATH = EXPORT_DIR / "verification_report.json"
WAREHOUSE_DIR = BASE_DIR / "warehouse"
WAREHOUSE_DB_PATH = WAREHOUSE_DIR / "healthcare_pipeline.db"
PUBLIC_KEY_PATH = BASE_DIR / "public_key.pem"
PRIVATE_KEY_PATH = BASE_DIR / "private_key.pem"
ENCRYPT_COLUMN = "Billing Amount"


def ensure_dirs() -> None:
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)


def validate_source() -> str:
    if not RAW_CSV_PATH.exists():
        raise FileNotFoundError(f"Raw CSV not found: {RAW_CSV_PATH}")
    return str(RAW_CSV_PATH)


def _ensure_keys() -> tuple[object, object]:
    if PUBLIC_KEY_PATH.exists() and PRIVATE_KEY_PATH.exists():
        public_key = serialization.load_pem_public_key(PUBLIC_KEY_PATH.read_bytes())
        private_key = serialization.load_pem_private_key(PRIVATE_KEY_PATH.read_bytes(), password=None)
        return public_key, private_key

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    PRIVATE_KEY_PATH.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )
    PUBLIC_KEY_PATH.write_bytes(
        public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    return public_key, private_key


def _encrypt_value(value: str, public_key: object) -> str:
    ciphertext = public_key.encrypt(
        value.encode("utf-8"),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return ciphertext.hex()


def _decrypt_value(value: str, private_key: object) -> str:
    plaintext = private_key.decrypt(
        bytes.fromhex(value),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )
    return plaintext.decode("utf-8")


def encrypt_csv_to_parquet() -> str:
    validate_source()
    ensure_dirs()
    public_key, _ = _ensure_keys()

    encrypted_rows: list[dict[str, object]] = []
    with RAW_CSV_PATH.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_id, row in enumerate(reader, start=1):
            encrypted_row = dict(row)
            billing_amount = encrypted_row.pop(ENCRYPT_COLUMN)
            encrypted_row["row_id"] = row_id
            encrypted_row[f"{ENCRYPT_COLUMN}_encrypted"] = _encrypt_value(billing_amount, public_key)
            encrypted_rows.append(encrypted_row)

    table = pa.Table.from_pylist(encrypted_rows)
    pq.write_table(table, PARQUET_FILE)
    return str(PARQUET_FILE)


def decrypt_verify_and_export_csv() -> str:
    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")

    ensure_dirs()
    _, private_key = _ensure_keys()

    parquet_rows = pq.read_table(PARQUET_FILE).to_pylist()

    source_by_row_id: dict[int, dict[str, str]] = {}
    with RAW_CSV_PATH.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row_id, row in enumerate(reader, start=1):
            source_by_row_id[row_id] = row

    checked_rows = 0
    with EXPORT_CSV_PATH.open("w", newline="", encoding="utf-8") as export_handle:
        writer = csv.writer(export_handle)
        writer.writerow(["Name", "Age"])

        for row in parquet_rows:
            row_id = int(row["row_id"])
            source_row = source_by_row_id[row_id]
            decrypted_amount = _decrypt_value(row[f"{ENCRYPT_COLUMN}_encrypted"], private_key)
            original_amount = source_row[ENCRYPT_COLUMN]

            if decrypted_amount != original_amount:
                raise ValueError(
                    f"Verification failed for row_id={row_id}: expected {original_amount}, got {decrypted_amount}"
                )

            writer.writerow([row["Name"], row["Age"]])
            checked_rows += 1

    VERIFY_REPORT_PATH.write_text(
        json.dumps(
            {
                "status": "verified",
                "rows_checked": checked_rows,
                "encrypted_column": ENCRYPT_COLUMN,
                "parquet_file": str(PARQUET_FILE),
                "export_csv": str(EXPORT_CSV_PATH),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return str(EXPORT_CSV_PATH)


def _load_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _normalize_raw_row(row_id: int, row: dict[str, str]) -> dict[str, object]:
    return {
        "row_id": row_id,
        "name": row["Name"],
        "age": int(row["Age"]),
        "gender": row["Gender"],
        "blood_type": row["Blood Type"],
        "medical_condition": row["Medical Condition"],
        "date_of_admission": row["Date of Admission"],
        "doctor": row["Doctor"],
        "hospital": row["Hospital"],
        "insurance_provider": row["Insurance Provider"],
        "billing_amount": float(row["Billing Amount"]),
        "room_number": row["Room Number"],
        "admission_type": row["Admission Type"],
        "discharge_date": row["Discharge Date"],
        "medication": row["Medication"],
        "test_results": row["Test Results"],
    }


def _normalize_encrypted_row(row: dict[str, object]) -> dict[str, object]:
    return {
        "row_id": int(row["row_id"]),
        "name": row["Name"],
        "age": int(row["Age"]),
        "gender": row["Gender"],
        "blood_type": row["Blood Type"],
        "medical_condition": row["Medical Condition"],
        "date_of_admission": row["Date of Admission"],
        "doctor": row["Doctor"],
        "hospital": row["Hospital"],
        "insurance_provider": row["Insurance Provider"],
        "room_number": row["Room Number"],
        "admission_type": row["Admission Type"],
        "discharge_date": row["Discharge Date"],
        "medication": row["Medication"],
        "test_results": row["Test Results"],
        "billing_amount_encrypted": row[f"{ENCRYPT_COLUMN}_encrypted"],
    }


def _normalize_safe_export_row(row_id: int, row: dict[str, str]) -> dict[str, object]:
    return {
        "row_id": row_id,
        "name": row["Name"],
        "age": int(row["Age"]),
    }


def _create_tables(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    cursor.executescript(
        """
        DROP TABLE IF EXISTS raw_healthcare;
        DROP TABLE IF EXISTS encrypted_healthcare;
        DROP TABLE IF EXISTS safe_export;
        CREATE TABLE raw_healthcare (
            row_id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            gender TEXT,
            blood_type TEXT,
            medical_condition TEXT,
            date_of_admission TEXT,
            doctor TEXT,
            hospital TEXT,
            insurance_provider TEXT,
            billing_amount REAL,
            room_number TEXT,
            admission_type TEXT,
            discharge_date TEXT,
            medication TEXT,
            test_results TEXT
        );
        CREATE TABLE encrypted_healthcare (
            row_id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            gender TEXT,
            blood_type TEXT,
            medical_condition TEXT,
            date_of_admission TEXT,
            doctor TEXT,
            hospital TEXT,
            insurance_provider TEXT,
            room_number TEXT,
            admission_type TEXT,
            discharge_date TEXT,
            medication TEXT,
            test_results TEXT,
            billing_amount_encrypted TEXT
        );
        CREATE TABLE safe_export (
            row_id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        );
        CREATE TABLE IF NOT EXISTS verification_audit (
            audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_status TEXT NOT NULL,
            rows_checked INTEGER NOT NULL,
            encrypted_column TEXT NOT NULL,
            source_csv_path TEXT NOT NULL,
            parquet_file_path TEXT NOT NULL,
            export_csv_path TEXT NOT NULL,
            verification_report_path TEXT NOT NULL,
            warehouse_db_path TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """
    )


def load_to_sqlite_warehouse() -> str:
    validate_source()
    ensure_dirs()

    if not PARQUET_FILE.exists():
        raise FileNotFoundError(f"Parquet file not found: {PARQUET_FILE}")
    if not EXPORT_CSV_PATH.exists():
        raise FileNotFoundError(f"Export CSV not found: {EXPORT_CSV_PATH}")
    if not VERIFY_REPORT_PATH.exists():
        raise FileNotFoundError(f"Verification report not found: {VERIFY_REPORT_PATH}")

    source_rows = _load_csv_rows(RAW_CSV_PATH)
    safe_export_rows = _load_csv_rows(EXPORT_CSV_PATH)
    encrypted_rows = pq.read_table(PARQUET_FILE).to_pylist()
    verify_report = json.loads(VERIFY_REPORT_PATH.read_text(encoding="utf-8"))

    connection = sqlite3.connect(WAREHOUSE_DB_PATH)
    try:
        _create_tables(connection)
        cursor = connection.cursor()

        cursor.executemany(
            """
            INSERT INTO raw_healthcare (
                row_id, name, age, gender, blood_type, medical_condition,
                date_of_admission, doctor, hospital, insurance_provider,
                billing_amount, room_number, admission_type, discharge_date,
                medication, test_results
            ) VALUES (
                :row_id, :name, :age, :gender, :blood_type, :medical_condition,
                :date_of_admission, :doctor, :hospital, :insurance_provider,
                :billing_amount, :room_number, :admission_type, :discharge_date,
                :medication, :test_results
            )
            """,
            [
                _normalize_raw_row(row_id, row)
                for row_id, row in enumerate(source_rows, start=1)
            ],
        )

        cursor.executemany(
            """
            INSERT INTO encrypted_healthcare (
                row_id, name, age, gender, blood_type, medical_condition,
                date_of_admission, doctor, hospital, insurance_provider,
                room_number, admission_type, discharge_date, medication,
                test_results, billing_amount_encrypted
            ) VALUES (
                :row_id, :name, :age, :gender, :blood_type, :medical_condition,
                :date_of_admission, :doctor, :hospital, :insurance_provider,
                :room_number, :admission_type, :discharge_date, :medication,
                :test_results, :billing_amount_encrypted
            )
            """,
            [_normalize_encrypted_row(row) for row in encrypted_rows],
        )

        cursor.executemany(
            """
            INSERT INTO safe_export (row_id, name, age)
            VALUES (:row_id, :name, :age)
            """,
            [
                _normalize_safe_export_row(row_id, row)
                for row_id, row in enumerate(safe_export_rows, start=1)
            ],
        )

        cursor.execute(
            """
            INSERT INTO verification_audit (
                run_status,
                rows_checked,
                encrypted_column,
                source_csv_path,
                parquet_file_path,
                export_csv_path,
                verification_report_path,
                warehouse_db_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verify_report["status"],
                verify_report["rows_checked"],
                verify_report["encrypted_column"],
                str(RAW_CSV_PATH),
                str(PARQUET_FILE),
                str(EXPORT_CSV_PATH),
                str(VERIFY_REPORT_PATH),
                str(WAREHOUSE_DB_PATH),
            ),
        )
        connection.commit()
    finally:
        connection.close()

    return str(WAREHOUSE_DB_PATH)
