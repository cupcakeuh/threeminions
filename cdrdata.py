

import pandas as pd
import random
import multiprocessing
from tqdm import tqdm
from faker import Faker
from datetime import datetime, timedelta

def generate_cdr_batch(batch_size: int) -> list:
    """
    Generate a batch of CDR records.

    Args:
    batch_size (int): The number of records to generate.

    Returns:
    list: A list of CDR records.
    """
    fake = Faker('en_IN')  # Use Indian locale for Faker
    Faker.seed(42)  # Set random seed for reproducibility

    data = []
    for _ in range(batch_size):
        # Generate Indian phone numbers with 10 digits
        caller = '9' + str(random.randint(100000000, 999999999))  # Ensure phone number starts with 9
        receiver = '9' + str(random.randint(100000000, 999999999))

        # Generate random IMEI and IMSI
        imei = ''.join(str(random.randint(0, 9)) for _ in range(15))
        imsi = ''.join(str(random.randint(0, 9)) for _ in range(15))

        # Generate random IP address
        ip_address = fake.ipv4()

        # Generate random call duration
        duration = random.randint(1, 900)

        # Generate random call start time
        start_time = fake.date_time_this_year()
        end_time = start_time + timedelta(seconds=duration)

        # Generate random location
        location = fake.city()

        # Generate random call type
        call_type = random.choice(["Voice", "SMS", "MMS"])

        # Simulate suspicious activities
        is_suspicious = 0
        if duration < 10 or location in ["Mumbai", "Delhi", "Kolkata"]:
            is_suspicious = 1

        data.append([caller, receiver, imei, imsi, ip_address, start_time, end_time, duration, location, call_type, is_suspicious])

    return data

def generate_large_cdr_dataset(num_records: int, num_processes: int = 4) -> pd.DataFrame:
    """
    Generate a large CDR dataset using multiprocessing.

    Args:
    num_records (int): The total number of records to generate.
    num_processes (int): The number of processes to use. Defaults to 4.

    Returns:
    pd.DataFrame: A DataFrame containing the generated CDR records.
    """
    batch_size = num_records // num_processes

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap(generate_cdr_batch, [batch_size] * num_processes), total=num_processes))

    # Flatten results and create DataFrame
    cdr_data = [record for batch in results for record in batch]
    columns = ["Caller", "Receiver", "IMEI", "IMSI", "IP Address", "Call Start Time", "Call End Time", "Duration (sec)", "Location", "Call Type", "Suspicious"]
    df = pd.DataFrame(cdr_data, columns=columns)

    return df

if __name__ == "__main__":
    num_records = 200000
    num_processes = 8

    cdr_df = generate_large_cdr_dataset(num_records, num_processes)

    # Save to CSV with correct file path
    cdr_df.to_csv(r"C:\Users\Innocent\Documents\cyberthon\cdr_data.csv", index=False)

    print(" CDR Dataset Generated and Saved as 'cdr_data.csv'")
