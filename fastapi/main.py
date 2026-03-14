from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
import sqlite3
import csv
import pandas as pd
from datetime import datetime
from io import BytesIO
import json

app = FastAPI()
DB_NAME = "database.db"

# Pretty JSON Response (for creating formatted JSON responses that are human-readable)
class PrettyJSONResponse(JSONResponse):
    def render(self, content: any) -> bytes:
        return (json.dumps(content, indent=4, ensure_ascii=False) + "\n").encode("utf-8")



# Global HTTPException handler
@app.exception_handler(StarletteHTTPException)
async def pretty_http_exception_handler(request: Request, exc: StarletteHTTPException):
    content = {"detail": exc.detail}
    return PrettyJSONResponse(content=content, status_code=exc.status_code)


# Utility functions
def detect_type(value):
    if value is None or value == "":
        return "TEXT"
    try:
        int(value)
        return "INTEGER"
    except:
        pass
    try:
        float(value)
        return "REAL"
    except:
        pass
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return "TIMESTAMP"
    except:
        pass
    try:
        datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return "TIMESTAMP"
    except:
        pass
    return "TEXT"


def infer_column_types(rows):
    column_types = []
    for column in zip(*rows):
        detected = "INTEGER"
        for value in column:
            t = detect_type(value)
            if t == "TEXT":
                detected = "TEXT"
                break
            elif t == "TIMESTAMP" and detected not in ["TEXT"]:
                detected = "TIMESTAMP"
            elif t == "REAL" and detected not in ["TEXT", "TIMESTAMP"]:
                detected = "REAL"
        column_types.append(detected)
    return column_types


def get_next_table_name(cursor):
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'table_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    numbers = []
    for t in tables:
        try:
            numbers.append(int(t.split("_")[1]))
        except:
            pass
    next_number = max(numbers) + 1 if numbers else 1
    return f"table_{next_number}"


def create_table(cursor, table_name, headers, types):
    columns = [f'"{h}" {t}' for h, t in zip(headers, types)]
    query = f'CREATE TABLE "{table_name}" ({", ".join(columns)})'
    cursor.execute(query)


def insert_rows(cursor, table_name, headers, rows):
    placeholders = ",".join(["?"] * len(headers))
    query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
    for row in rows:
        cursor.execute(query, row)


def read_csv_flexible(file_bytes):
    separators = [',', ';', '\t']
    encodings = ['utf-8', 'cp1251', 'windows-1251', 'latin-1']

    for enc in encodings:
        try:
            content_str = file_bytes.decode(enc)
        except Exception:
            continue

        lines = content_str.splitlines()
        if not lines:
            continue

        for sep in separators:
            try:
                reader = csv.reader(lines, delimiter=sep)
                data = list(reader)
                if all(len(row) <= 1 for row in data):
                    continue
                return data
            except Exception:
                continue

    raise ValueError("CSV file cannot be parsed with available encodings and separators")


# Endpoints

@app.get("/")
def home():
    content = {"message": "Welcome to the CSV/XLSX upload service"}
    return PrettyJSONResponse(content=content)


@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    

    if not (file.filename.endswith(".csv") or file.filename.endswith(".xlsx")):
        return PrettyJSONResponse(
            content={"detail": "Only CSV or XLSX files allowed"},
            status_code=400
        )

    file_bytes = await file.read()
    if len(file_bytes) == 0:
        return PrettyJSONResponse(
            content={"detail": "Uploaded file is empty"},
            status_code=400
        )

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    table_name = get_next_table_name(cursor)

    if file.filename.endswith(".csv"):
        try:
            data = read_csv_flexible(file_bytes)
        except ValueError as e:
            return PrettyJSONResponse(
                content={"detail": str(e)},
                status_code=400
            )
    else:
        try:
            excel_file = BytesIO(file_bytes)
            df = pd.read_excel(excel_file)
        except Exception:
            return PrettyJSONResponse(
                content={"detail": "Invalid Excel file"},
                status_code=400
            )
        if df.empty and len(df.columns) == 0:
            return PrettyJSONResponse(
                content={"detail": "Excel file contains no data"},
                status_code=400
            )
        data = [df.columns.tolist()] + df.astype(str).values.tolist()

    headers = data[0]
    rows = data[1:]
    if len(rows) == 0:
        return PrettyJSONResponse(
            content={"detail": "File contains only headers and no data rows"},
            status_code=400
        )

    types = infer_column_types(rows)
    create_table(cursor, table_name, headers, types)
    insert_rows(cursor, table_name, headers, rows)
    conn.commit()
    conn.close()

    content = {
        "message": "File processed successfully",
        "table_name": table_name,
        "columns": [{"name": h, "type": t} for h, t in zip(headers, types)],
        "rows_inserted": len(rows),
    }
    return PrettyJSONResponse(content=content)


@app.get("/tables")
def get_tables():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    tables = [row[0] for row in cursor.fetchall()]
    conn.close()

    content = {"tables": tables}
    return PrettyJSONResponse(content=content)


@app.get("/table/{table_name}")
def get_table(table_name: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    try:
        cursor.execute(f'SELECT * FROM "{table_name}"')
    except sqlite3.OperationalError:
        return PrettyJSONResponse(
            content={"detail": "Table not found"},
            status_code=404
        )

    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    # Types of columns
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    types = {row[1]: row[2] for row in cursor.fetchall()}

    conn.close()

    content = {
        "table": table_name,
        "columns": [{"name": name, "type": types[name]} for name in column_names],
        "rows": rows,
    }
    return PrettyJSONResponse(content=content)