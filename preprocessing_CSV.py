import numpy as np
import pandas as pd

print("Reading dataset rows")
ds = pd.read_csv("dataset.csv", na_filter=False)

# Remove all rows where the first cell contains alphabetic characters (names)
ds = ds[~ds.iloc[:, 0].str[0].str.isalpha()]

# Sorting dataset based on user ID (first column)
ds_sorted = ds.sort_values(by=[ds.columns[0]])

# Process user events by grouping users
ds_sorted['datetime'] = pd.to_datetime(ds_sorted.iloc[:, 1])
ds_sorted['lat'] = ds_sorted.iloc[:, 2].apply(lambda x: x[2:-2].split(', ')[0])
ds_sorted['lon'] = ds_sorted.iloc[:, 2].apply(lambda x: x[2:-2].split(', ')[1])

ds_mfu = pd.DataFrame()
ds_nor = pd.DataFrame()

def process_on_users_events(user_df):
    return user_df.sort_values(by='datetime')

print("Formatting user events and sorting by date")
for user_id, user_events in ds_sorted.groupby(ds_sorted.columns[0]):
    print(f"Processing user ID: {user_id}")
    user_events_sorted = process_on_users_events(user_events)
    if len(user_events_sorted) > 30:
        ds_mfu = pd.concat([ds_mfu, user_events_sorted], ignore_index=True)
    else:
        ds_nor = pd.concat([ds_nor, user_events_sorted], ignore_index=True)

# Save to CSV (faster than Excel)
ds_mfu.to_csv("dataset_output_mfu.csv", index=False)
ds_nor.to_csv("dataset_output_nor.csv", index=False)
