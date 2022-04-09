"""
This code simulates a source providing transactional data on a credit card account
for select set of customers, whose ID's range from 1000-1100.
NOTE:
- The output files' contents are always the same (to assist us with checking 
your ETL outputs and dashboards aligns with our expected results). You can disable 
this function if comment out the line: "random.seed(42)"
- You can create more batches of data (to simulate more epochs of transactional
data landing) by increasing the the main for-loop's range.
HINT: there are no transactional UUIDs.
"""
import random
import pandas as pd
from datetime import timedelta
from time import sleep
import numpy as np
random.seed(42)


# Source-data constants
LANDING_ZONE_DIR = "./landing_zone"
_latest_possible_transaction_date = pd.to_datetime("2022-01-14 00:00:01.224489795")
_earliest_source_file_date = pd.to_datetime("2022-01-15")
_batch_length = 150


for batch in range(3):
    print(_latest_possible_transaction_date)
    
    # Generate unique customer ID’s which range from 1000 to 1100
    customer_id = [random.randrange(1000, 1101) for _ in range(0, _batch_length)]
    # Transaction amount change as customers debt or credit their account
    transaction_amounts = [
        np.round(random.randrange(-500, 1000) * np.sin(x/(2*np.pi)))
        for x in range(0, _batch_length)]
    # The date the transaction took place, which can vary from yesturday to a max of 100 days ago
    transaction_date =  [d.strftime('%d-%m-%Y %H:%M:%S.%f') for d in pd.date_range(
            _latest_possible_transaction_date-timedelta(days=31), 
            _latest_possible_transaction_date,
            periods=_batch_length)] #dd-mm-yyyy hh:mm:ss.123456789
    
    # Create the final dataframe and save it into the landing zone
    folder_timestamp = _earliest_source_file_date+timedelta(days=31*batch)
    new_source_data = pd.DataFrame(
        list(zip(customer_id, transaction_amounts, transaction_date)),
        columns =["customer_id", "transaction_amount", "transaction_date"])
    new_source_data.to_csv(f"./transactions_{folder_timestamp}.csv")
    print(f"New source batch has arrived dated: {folder_timestamp}")
    
    # Simulate some time passing before the next batch of source data lands
    _latest_possible_transaction_date += timedelta(days=31)
    sleep(5)
