import logging
from datetime import datetime, timedelta
import json


import azure.functions as func
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()


# TO-DO - Pass the values from environment variables/some secrets store
URL = ""
KEY = ""
COSMO_DATABASE_NAME = ""
COSMO_CONTAINER_NAME = ""
BLOB_CONNECTION_STRING = ""
BLOB_CONTAINER_NAME = ""


#This scheduler runs everyday at 12:00am
@app.schedule(schedule="0 0 0 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    container_client = connect_to_cosmodb_container(database_name=COSMO_DATABASE_NAME,container_name=COSMO_CONTAINER_NAME)
    logging.info("connected to cosmodb client")
    data = fetch_data_from_cosmo_db(container_client=container_client)
    logging.info("fetched the data from cosmdob")
    upload_to_blob(data)


def connect_to_cosmodb_container(database_name, container_name):

    client = CosmosClient(URL, credential=KEY)
    database = client.get_database_client(database_name)
    container_client = database.get_container_client(container_name)
    return container_client


def return_timedelta_iso(time_input, time_delta):
    current_time = time_input
    start_time = datetime(current_time.year, current_time.month, current_time.day) - timedelta(days=time_delta)
    end_time = start_time + timedelta(days=time_delta, seconds=-1)
    start_time_str = start_time.isoformat()
    end_time_str = end_time.isoformat()
    return start_time_str, end_time_str



def fetch_data_from_cosmo_db(container_client):

    day = datetime.utcnow()
    #message time window should be 1 day for messages created on one day and remained on same day
    message_start_time, message_end_time = return_timedelta_iso(day, time_delta=1)
    #conversation time window should be 2 days for messages created on one day and overflowed to next day
    conversation_start_time, conversation_end_time = return_timedelta_iso(day, time_delta=2) 
   
    #query for conversations that started a day before but may have messages from the next day.
    message_overflow_query = f"""
    SELECT 
        c.heading, 
        c.created_at, 
        c.updated_at,
        c.id, 
        c.user_id, 
        ARRAY(
            SELECT m.message_id, m.created_at, m.updated_at, m.token_count, m.current_message_token_count, m.correlation_id, m.type, m.data, m.feedback
            FROM m IN c.messages 
            WHERE m.created_at >= '{message_start_time}' AND m.created_at <= '{message_end_time}'
        ) AS messages 
    FROM c 
    WHERE c.created_at >= '{conversation_start_time}' AND c.created_at <= '{conversation_end_time}'
    """


    message_overflow_items =  list(container_client.query_items(query=message_overflow_query, enable_cross_partition_query=True))
    remove_empty_items = [item for item in message_overflow_items if item["messages"] ]
    items_data = json.dumps(remove_empty_items, indent=4)

    return items_data


def upload_to_blob(json_data): 
    connection_string = BLOB_CONNECTION_STRING
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    # Upload the JSON String to Azure Blob Storage
    blob_name = f"{datetime.utcnow()}.json"
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the Created Blob
    blob_client.upload_blob(json_data, overwrite=True)
    print(f"Data successfully uploaded to {blob_name}")


