from ETL.connectors import LookupConnector
from ETL.dbcreds import LOOKUP_CREDS
from data_handlers.usedesk_handlers import UsedeskGetter
from creds.usedesk_creds import TOKEN, TICKETS_URL, SINGLE_TICKET_URL, CHANNEL_VOCAB_URL, GET_AGENTS_URL
from datetime import timedelta, datetime

import time
import pandas as pd
import numpy as np


udGetter = UsedeskGetter(
    token=TOKEN,
    tickets_url=TICKETS_URL,
    single_ticket_url=SINGLE_TICKET_URL,
    channel_vocab_url=CHANNEL_VOCAB_URL,
    get_agents_url=GET_AGENTS_URL
)

lookup = LookupConnector(
    creds=LOOKUP_CREDS
)


tickets_extract_query = '''
select * from dashboards.contact_center_usedesk_tickets
where created_at > '{date_from}' and created_at < '{date_to}';
'''


def batch_insert_data(messages: pd.DataFrame):
    overall_time = 0
    for i in range(0, len(messages), 5000):
        start = datetime.now()
        lookup.insert(schema='dashboards', table='contact_center_usedesk_messages', data=messages[i:(i+5000)])
        time_elapsed = (datetime.now() - start).total_seconds()
        print(f'Time elapsed for batch: {time_elapsed}')
        overall_time += time_elapsed
    print(f'Elapsed in total: {overall_time}s')
    
    
def fetch_usedesk_messages(date_from, date_to):
    created_tickets = udGetter.get_all_tickets(date_from=date_from, date_to=date_to, filter_type='created')

    created_messages, created_changes, created_fields = udGetter.get_tickets_data_by_tickets_batch(tickets_batch=created_tickets)
    
#     created_messages = pd.read_csv(f'last_messages_{DATE_TO}.csv').drop('Unnamed: 0', axis='columns')
    print('tickets extract completed!')

    created_messages = pd.DataFrame(created_messages)
    created_messages.rename(columns={'from':'sender'}, inplace=True)
    created_messages['user_id'] = created_messages['user_id'].fillna(0)
    created_messages['user_id'] = pd.to_numeric(created_messages['user_id']).astype(np.int64)
    created_messages['message_published_at'] = pd.to_datetime(created_messages['message_published_at']) + timedelta(hours=3)
    
    batch_insert_data(created_messages)
    return


def main():
    DATE_FROM = pd.to_datetime(datetime.now().date()) - timedelta(hours=3, days=1)
    DATE_TO = pd.to_datetime(datetime.now().date()) - timedelta(hours=3)
#     DATE_FROM = datetime.strptime("2023-01-23 21:00", "%Y-%m-%d %H:%M")
#     DATE_TO = datetime.strptime("2023-01-24 21:00", "%Y-%m-%d %H:%M")
    
#     fetch_usedesk_messages(DATE_FROM, DATE_TO)
    
    # Example. This part should be uncomment if you want to upload missed data.
    # DATE_FROM, DATE_TO and END_DATE must be specified manually.
#     END_DATE = pd.to_datetime(datetime.now().date()) - timedelta(days=1, hours=3)
#     END_DATE = datetime.strptime("2023-01-08 21:00", "%Y-%m-%d %H:%M")
#     DATE_FROM = datetime.strptime("2023-01-04 21:00", "%Y-%m-%d %H:%M")
#     DATE_TO = datetime.strptime("2023-01-05 21:00", "%Y-%m-%d %H:%M")
    
#     while DATE_TO <= END_DATE:
#         print(f'start extracting messages from {DATE_FROM}')
#         fetch_usedesk_messages(DATE_FROM, DATE_TO)
#         DATE_FROM += timedelta(days=1)
#         DATE_TO += timedelta(days=1)
#         time.sleep(2)


    # For tests
#     bruh = lookup.query("SELECT * from dashboards.contact_center_usedesk_messages limit 10;")
#     for result in bruh['results']:
#         print(result)
    return


if __name__ == "__main__":
    main()
