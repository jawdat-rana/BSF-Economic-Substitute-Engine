import io
import requests
import pandas as pd

WORLD_BANK_XLSX_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/"
    "CMO-Historical-Data-Monthly.xlsx"
)

response = requests.get(WORLD_BANK_XLSX_URL, timeout=60)
response.raise_for_status()

excel_file = pd.ExcelFile(io.BytesIO(response.content), engine="openpyxl")

print("Sheet names:")
for i, name in enumerate(excel_file.sheet_names):
    print(i, name)

for i, name in enumerate(excel_file.sheet_names):
    print(f"\n--- SHEET {i}: {name} ---")
    df = pd.read_excel(excel_file, sheet_name=name, header=None)
    print("shape:", df.shape)
    print(df.head(10).to_string())