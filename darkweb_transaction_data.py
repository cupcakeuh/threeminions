from faker import Faker
import pandas as pd
import random
import multiprocessing
from tqdm import tqdm

# Initialize Faker
fake = Faker()
Faker.seed(42)

# List of Indian Dark Web marketplaces (Fake Names)
indian_dark_web_sites = [
    "DMarket India", "Rajdhani Silk Road", "IndianCryptoHub", "BlackBazaar India"
]

# Common illegal activities in the Indian Dark Web
indian_illegal_activities = [
    "Fake Aadhaar Cards", "Stolen UPI Credentials", "Hacked Databases",
    "Drug Trafficking", "Weapons Dealing", "Crypto Laundering", "Illegal SIM Cards"
]

# Generate Indian-style email
def indian_email():
    domains = ["@gmail.com", "@yahoo.in", "@rediffmail.com"]
    return fake.first_name().lower() + str(random.randint(10, 99)) + random.choice(domains)

# Generate Indian IP Address (Fake Ranges)
def indian_ip():
    ip_start = random.choice(["49", "103", "117", "125", "202", "223"])
    return f"{ip_start}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"

# Function to generate a batch of fake transactions
def generate_dark_web_batch(batch_size):
    data = []
    for _ in range(batch_size):
        transaction_id = fake.uuid4()
        buyer = indian_email()
        seller = indian_email()
        amount_inr = round(random.uniform(1000, 500000), 2)  # INR amount
        amount_btc = round(amount_inr / 5000000, 6)  # Approximate BTC conversion
        transaction_time = fake.date_time_this_year()
        dark_web_site = random.choice(indian_dark_web_sites)
        activity = random.choice(indian_illegal_activities)
        ip_address = indian_ip()

        # Suspicious flag (randomly assigned)
        is_suspicious = 1 if amount_inr > 200000 or activity in ["Weapons Dealing", "Crypto Laundering"] else 0

        data.append([
            transaction_id, buyer, seller, amount_inr, amount_btc, 
            transaction_time, dark_web_site, activity, ip_address, is_suspicious
        ])
    
    return data

# Function to generate large dataset with multiprocessing
def generate_large_dark_web_dataset(num_records, num_processes=4):
    batch_size = num_records // num_processes

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap(generate_dark_web_batch, [batch_size] * num_processes), total=num_processes))

    # Flatten results and create DataFrame
    dark_web_data = [record for batch in results for record in batch]
    columns = [
        "Transaction ID", "Buyer", "Seller", "Amount (INR)", "Amount (BTC)", 
        "Timestamp", "Dark Web Site", "Activity", "IP Address", "Suspicious"
    ]
    df = pd.DataFrame(dark_web_data, columns=columns)
    
    return df

if __name__ == "__main__":
    # Generate 200,000 Dark Web transactions for India
    dark_web_df = generate_large_dark_web_dataset(200000, num_processes=8)

    # Save to CSV file (Ensure correct path format)
    file_path = r"C:\Users\Innocent\Documents\cyberthon\dark_web_transactions_india.csv"
    dark_web_df.to_csv(file_path, index=False)

    print(f"✅ Dark Web Transaction Data for India Saved at: {file_path}")
