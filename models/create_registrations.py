import pandas as pd
import uuid
import random
from datetime import timedelta
import requests
import os

# Define the GitHub repository and file paths
GITHUB_USERNAME = 'ae-wizard'
REPO_NAME = 'ae-sample-project'
SESSIONS_FILE_PATH = 'models/sessions.csv'
REGISTRATIONS_FILE_PATH = 'models/registrations.csv'
GITHUB_API_URL_SESSIONS = f'https://raw.githubusercontent.com/{GITHUB_USERNAME}/{REPO_NAME}/main/{SESSIONS_FILE_PATH}'

# Function to load sessions.csv from GitHub
def load_sessions_data():
    response = requests.get(GITHUB_API_URL_SESSIONS)
    response.raise_for_status()
    return pd.read_csv(response.content.decode('utf-8'))

# Generate random user_id
def generate_user_id():
    return str(uuid.uuid4())

# Generate registered_at_et between visit_start_time_et and visit_end_time_et
def generate_registered_at(start_time, end_time):
    return start_time + timedelta(
        seconds=random.randint(0, int((end_time - start_time).total_seconds()))
    )

# Load sessions.csv
sessions_df = load_sessions_data()

# Prepare the data for registrations.csv
num_records = 3000
selected_sessions = sessions_df.sample(n=num_records)

registration_records = []

for _, row in selected_sessions.iterrows():
    visit_id = row['visit_id']
    user_id = generate_user_id()
    visit_start_time = pd.to_datetime(row['visit_start_time_et'])
    visit_end_time = pd.to_datetime(row['visit_end_time_et'])
    
    # Generate registered_at_et between visit_start_time_et and visit_end_time_et
    registered_at_et = generate_registered_at(visit_start_time, visit_end_time)

    registration_records.append({
        'visit_id': visit_id,
        'user_id': user_id,
        'registered_at_et': registered_at_et.strftime('%Y-%m-%d %H:%M:%S')
    })

# Create a DataFrame for registrations.csv
registrations_df = pd.DataFrame(registration_records)

# Save the generated data to registrations.csv (locally or upload it to GitHub)
registrations_df.to_csv('registrations.csv', index=False)

print("Generated 3,000 records for registrations.csv successfully.")
