import pandas as pd
import faker
import random
import uuid
from datetime import datetime, timedelta
import requests

# Initialize Faker
fake = faker.Faker()

# GitHub repo details
GITHUB_USERNAME = 'ae-wizard'
REPO_NAME = 'ae-sample-project'
SESSIONS_PATH = 'models/sessions.csv'
REGISTRATIONS_PATH = 'models/registrations.csv'
SESSIONS_URL = f'https://raw.githubusercontent.com/{GITHUB_USERNAME}/{REPO_NAME}/main/{SESSIONS_PATH}'
REGISTRATIONS_URL = f'https://raw.githubusercontent.com/{GITHUB_USERNAME}/{REPO_NAME}/main/{REGISTRATIONS_PATH}'

# Load sessions and registrations data
sessions_df = pd.read_csv(SESSIONS_URL)
registrations_df = pd.read_csv(REGISTRATIONS_URL)

# Convert timestamps to datetime for easier manipulation
sessions_df['visit_start_time_et'] = pd.to_datetime(sessions_df['visit_start_time_et'])
sessions_df['visit_end_time_et'] = pd.to_datetime(sessions_df['visit_end_time_et'])
registrations_df['registered_at_et'] = pd.to_datetime(registrations_df['registered_at_et'])

# Join sessions and registrations to identify first-time visitors
first_time_visits = pd.merge(sessions_df, registrations_df, on='visit_id', how='inner')

# Select 20-30% of first-time visits to make a purchase
first_time_purchases = first_time_visits.sample(frac=random.uniform(0.2, 0.3))

# Select 10-30% of subsequent visits for users who already registered to make purchases
subsequent_visits = sessions_df[~sessions_df['visit_id'].isin(first_time_visits['visit_id'])]
subsequent_visits_with_registered_users = subsequent_visits[subsequent_visits['visit_id'].isin(registrations_df['visit_id'])]
subsequent_purchases = subsequent_visits_with_registered_users.sample(frac=random.uniform(0.1, 0.3))

# Combine first-time and subsequent purchase records
purchase_visits = pd.concat([first_time_purchases, subsequent_purchases])

# Shipping carriers and methods with corresponding delivery times
shipping_options = [
    {'carrier': 'carrier_1', 'method': 'Ground', 'min_days': 5, 'max_days': 7},
    {'carrier': 'carrier_1', 'method': '2-Day', 'min_days': 2, 'max_days': 3},
    {'carrier': 'carrier_3', 'method': 'Standard', 'min_days': 4, 'max_days': 6},
    {'carrier': 'carrier_2', 'method': 'Priority Mail', 'min_days': 3, 'max_days': 5},
    {'carrier': 'carrier_3', 'method': 'Express', 'min_days': 1, 'max_days': 3}
]

# Define shipping locations
locations = [str(uuid.uuid4()) for _ in range(15)]
priority_locations = random.sample(locations, 4)

def assign_location():
    """Assign shipping locations with priority for 4 locations."""
    if random.random() < 0.5:  # 50% chance for priority locations
        return random.choice(priority_locations)
    else:
        return random.choice(locations)

# Function to calculate estimated delivery date based on the carrier and method
def get_estimated_delivery_date(order_created_at, shipping_option):
    min_days = shipping_option['min_days']
    max_days = shipping_option['max_days']
    delivery_days = random.randint(min_days, max_days)
    estimated_delivery_date = order_created_at + timedelta(days=delivery_days)
    # Randomize delivery time during business hours (9 AM - 6 PM)
    estimated_delivery_date = estimated_delivery_date.replace(
        hour=random.randint(9, 17), minute=random.randint(0, 59), second=random.randint(0, 59)
    )
    return estimated_delivery_date

# Function to calculate the delivery time based on estimated delivery date
def get_delivered_at(estimated_delivery_date):
    # Delivery could happen early or on-time with slight delays
    delivery_variation_days = random.choices([-1, 0, 1, 2], weights=[0.2, 0.6, 0.1, 0.1])[0]
    delivered_at = estimated_delivery_date + timedelta(days=delivery_variation_days)

    # Avoid Sunday deliveries
    while delivered_at.weekday() == 6:
        delivered_at += timedelta(days=1)

    # Randomize delivery time during business hours (9 AM - 6 PM)
    delivered_at = delivered_at.replace(
        hour=random.randint(9, 17), minute=random.randint(0, 59), second=random.randint(0, 59)
    )
    return delivered_at

# Generate random transactions
transactions_data = {
    "order_id": [],
    "visit_id": [],
    "user_id": [],
    "order_created_at_et": [],
    "location_id": [],
    "shipping_carrier": [],
    "shipping_method": [],
    "estimated_delivery_date": [],
    "delivered_at_et": []
}

for _, row in purchase_visits.iterrows():
    visit_id = row['visit_id']
    user_id = row['user_id']  # This is now pulled from the registrations file
    visit_start = row['visit_start_time_et']
    visit_end = row['visit_end_time_et']
    registered_at = row['registered_at_et'] if 'registered_at_et' in row else visit_start

    # Ensure order_created_at is after registered_at and within the visit time window
    order_created_at = fake.date_time_between(start_date=registered_at, end_date=visit_end)
    
    # Assign a shipping location and select a shipping option
    location_id = assign_location()
    shipping_option = random.choice(shipping_options)
    shipping_carrier = shipping_option['carrier']
    shipping_method = shipping_option['method']

    # Calculate estimated delivery date
    estimated_delivery_date = get_estimated_delivery_date(order_created_at, shipping_option)
    delivered_at = get_delivered_at(estimated_delivery_date)

    # Add transaction data
    transactions_data['order_id'].append(str(uuid.uuid4()))
    transactions_data['visit_id'].append(visit_id)
    transactions_data['user_id'].append(user_id)
    transactions_data['order_created_at_et'].append(order_created_at.strftime('%Y-%m-%d %H:%M:%S'))
    transactions_data['location_id'].append(location_id)
    transactions_data['shipping_carrier'].append(shipping_carrier)
    transactions_data['shipping_method'].append(shipping_method)
    transactions_data['estimated_delivery_date'].append(estimated_delivery_date.strftime('%Y-%m-%d %H:%M:%S'))
    transactions_data['delivered_at_et'].append(delivered_at.strftime('%Y-%m-%d %H:%M:%S'))

# Convert to DataFrame
transactions_df = pd.DataFrame(transactions_data)

# Save transactions to CSV
transactions_df.to_csv('updated_transactions.csv', index=False)

print(f"Generated {len(transactions_df)} transactions and saved to 'updated_transactions.csv'.")
