import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from dateutil.parser import parse

# ---------------------------
# GOOGLE SHEETS CONNECTION
# ---------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "service_account.json"  # path to your Google Cloud service account key
SPREADSHEET_NAME = "Your Spreadsheet Name Here"

creds = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(creds)

spreadsheet = client.open(SPREADSHEET_NAME)

# ---------------------------
# DATA TYPE INFERENCE
# ---------------------------
def infer_type(value):
    if value is None or value.strip() == "":
        return StringType()
    try:
        int(value)
        return IntegerType()
    except:
        pass
    try:
        float(value)
        return DoubleType()
    except:
        pass
    try:
        parse(value, fuzzy=False)
        return TimestampType()
    except:
        pass
    return StringType()

def clean_col_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^0-9a-zA-Z]+', '_', name)  # replace non-alphanumeric with _
    return name.strip('_')

# ---------------------------
# PROCESS ALL WORKSHEETS
# ---------------------------
for ws in spreadsheet.worksheets():
    print(f"\nProcessing worksheet: {ws.title}")
    data = ws.get_all_values()

    # Find first non-empty row as header
    headers = None
    for row in data:
        if any(cell.strip() for cell in row):
            headers = row
            break

    if not headers:
        print("No headers found, skipping.")
        continue

    headers = [clean_col_name(h) if h else f"_c{i}" for i, h in enumerate(headers)]
    print(f"Detected headers: {headers}")

    # Sample first data row to infer types
    sample_row = None
    for row in data[1:]:
        if any(cell.strip() for cell in row):
            sample_row = row
            break

    field_types = []
    for i, col in enumerate(headers):
        sample_value = sample_row[i] if sample_row and i < len(sample_row) else ""
        field_types.append(infer_type(sample_value))

    # Build schema
    fields = [StructField(col, dtype, True) for col, dtype in zip(headers, field_types)]
    schema = StructType(fields)

    print(f"Schema for {ws.title}:\n{schema.json()}")

