import pandas as pd
from sklearn.preprocessing import StandardScaler

# Load datasets
cdr_data = pd.read_csv('cdr_data.csv')
darkweb_data = pd.read_csv('dark_web_transactions_india.csv')

# Normalize column names
cdr_data.rename(columns=lambda x: x.strip().lower(), inplace=True)
darkweb_data.rename(columns=lambda x: x.strip().lower(), inplace=True)

# Check if 'timestamp' column exists
if 'timestamp' not in cdr_data.columns or 'timestamp' not in darkweb_data.columns:
    print("Error: Timestamp column not found in one or both datasets.")
else:
    # Convert timestamps to datetime
    cdr_data['timestamp'] = pd.to_datetime(cdr_data['timestamp'], errors='coerce')
    darkweb_data['timestamp'] = pd.to_datetime(darkweb_data['timestamp'], errors='coerce')

    # Drop rows with invalid timestamps
    cdr_data = cdr_data.dropna(subset=['timestamp'])
    darkweb_data = darkweb_data.dropna(subset=['timestamp'])

    # Sort datasets by timestamp
    cdr_data = cdr_data.sort_values('timestamp')
    darkweb_data = darkweb_data.sort_values('timestamp')

    # Merge datasets using 'nearest' timestamp
    merged_data = pd.merge_asof(cdr_data, darkweb_data, on='timestamp', direction='nearest')

    # Standardize numeric columns
    numeric_cols = merged_data.select_dtypes(include=['number']).columns
    merged_data[numeric_cols] = merged_data[numeric_cols].fillna(0)

    scaler = StandardScaler()
    merged_data[numeric_cols] = scaler.fit_transform(merged_data[numeric_cols])

    print("Merged and standardized data:")
    print(merged_data.head())
