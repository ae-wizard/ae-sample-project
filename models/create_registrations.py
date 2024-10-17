import pandas as pd
import uuid
import random
from datetime import timedelta

# Function to generate user_id (UUID)
def generate_user_id():
    return str(uuid.uuid4())

# Function to generate registered_at_et between visit_start_time_et and visit_end_time_et
def generate_registered_at(start_time, end_time):
    time_diff = (end_time - start_time).total_seconds()
    if time_diff > 0:
        random_seconds = random.randint(0, int(time_diff))
        return start_time + timedelta(seconds=random_seconds)
    else:
        return start_time

# Load your sessions.csv file
sessions_df = pd.read_csv('sessions.csv')

# Ensure that visit_start_time_et and visit_end_time_et are datetime objects
sessions_df['visit_start_time_et'] = pd.to_datetime(sessions_df['visit_start_time_et'])
sessions_df['visit_end_time_et'] = pd.to_datetime(sessions_df['visit_end_time_et'])

# Sample 3000 records from sessions.csv
num_records = 3000
selected_sessions = sessions_df.sample(n=num_records)

registration_records = []

for _, row in selected_sessions.iterrows():
    visit_id = row['visit_id']
    user_id = generate_user_id()
    visit_start_time = row['visit_start_time_et']
    visit_end_time = row['visit_end_time_et']
    
    # Generate registered_at_et between visit_start_time_et and visit_end_time_et
    registered_at_et = generate_registered_at(visit_start_time, visit_end_time)

    registration_records.append({
        'visit_id': visit_id,
        'user_id': user_id,
        'registered_at_et': registered_at_et.strftime('%Y-%m-%d %H:%M:%S')
    })

# Create a DataFrame for registrations.csv
registrations_df = pd.DataFrame(registration_records)

# Save to CSV
registrations_df.to_csv('registrations.csv', index=False)

print("Generated 3,000 records for registrations.csv successfully.")
