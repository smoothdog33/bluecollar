import pandas as pd

# Path to Excel file
file_path = "/Users/ayanbhatt/IdeaProjects/algorithms/Blue_collar_ai_dashboard/report.xlsx  "

# Read all sheets
all_sheets = pd.read_excel(file_path, sheet_name=None)  # None reads all sheets

# 'all_sheets' is a dictionary: {sheet_name: DataFrame}
for sheet_name, df in all_sheets.items():
    print(f"Sheet: {sheet_name}")
    print(df.head())
