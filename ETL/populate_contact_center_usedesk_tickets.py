from ETL.connectors import LookupConnector
from ETL.dbcreds import LOOKUP_CREDS
from data_handlers.usedesk_handlers import UsedeskGetter
from creds.usedesk_creds import TOKEN, TICKETS_URL, SINGLE_TICKET_URL, CHANNEL_VOCAB_URL, GET_AGENTS_URL
from datetime import timedelta, datetime
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


def fetch_usedesk_data(date_from, date_to):
    created_tickets = udGetter.get_all_tickets(date_from=date_from, date_to=date_to, filter_type='created')
    created_tickets = pd.DataFrame(created_tickets).iloc[:, :15]
    created_tickets = created_tickets.fillna(0)

    created_tickets.rename(columns={'id': 'ticket_id', 'group': 'group_id'}, inplace=True)

    created_tickets['assignee_id'] = pd.to_numeric(created_tickets['assignee_id']).astype(np.int64)
    created_tickets['group_id'] = pd.to_numeric(created_tickets['group_id']).astype(np.int64)
    created_tickets['created_at'] = pd.to_datetime(created_tickets['created_at']) + timedelta(hours=3)
    created_tickets['last_updated_at'] = pd.to_datetime(created_tickets['last_updated_at']) + timedelta(hours=3)

    batch_insert_data(created_tickets)
#     print(udGetter.get_tickets_batch(date_from=date_from, date_to=date_to, filter_type='created', offset=29))
    return


def batch_insert_data(tickets: pd.DataFrame):
    overall_time = 0
    for i in range(0, len(tickets), 5000):
        start = datetime.now()
        lookup.insert(schema='dashboards', table='contact_center_usedesk_tickets', data=tickets[i:(i+5000)])
        time_elapsed = (datetime.now() - start).total_seconds()
        print(f'Time elapsed for batch: {time_elapsed}')
        overall_time += time_elapsed
    print(f'Elapsed in total: {overall_time}s')


def main():
    # 26 - to
    # as much early as possible
    DATE_FROM = pd.to_datetime(datetime.now().date()) - timedelta(hours=3, days=1)
    DATE_TO = pd.to_datetime(datetime.now().date()) - timedelta(hours=3)
    
#     DATE_FROM = datetime.strptime("2023-01-24 21:00", "%Y-%m-%d %H:%M")
#     DATE_TO = datetime.strptime("2023-01-25 21:00", "%Y-%m-%d %H:%M)
#     fetch_usedesk_data(DATE_FROM, DATE_TO)
    
    # Example. This part should be uncomment if you want to upload missed data.
    # DATE_FROM, DATE_TO and END_DATE must be specified manually.
#     END_DATE = pd.to_datetime(datetime.now().date()) - timedelta(hours=3)
#     DATE_FROM = datetime.strptime("2022-12-30 21:00", "%Y-%m-%d %H:%M")
#     DATE_TO = datetime.strptime("2022-12-31 21:00", "%Y-%m-%d %H:%M")
    
#     while DATE_TO <= END_DATE:
#         print(DATE_FROM)
#         fetch_usedesk_data(DATE_FROM, DATE_TO)
#         DATE_FROM = DATE_FROM + timedelta(days=1)
#         DATE_TO = DATE_TO + timedelta(days=1)
#         time.sleep(2)

    #For tests
    # bruh = lookup.query("SELECT * from dashboards.contact_center_usedesk_tickets limit 10;")
    # for result in bruh['results']:
    #    print(result)
    return


if __name__ == "__main__":
    main()