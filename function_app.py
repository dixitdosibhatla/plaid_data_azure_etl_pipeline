import azure.functions as func
import os
import json
import logging
from io import StringIO

import time
import datetime
from datetime import datetime,date, timedelta

from plaid.api import plaid_api
from plaid.configuration import Configuration
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.products import Products
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.transactions_get_request import TransactionsGetRequest

from azure.storage.blob import BlobServiceClient

import pandas as pd

app = func.FunctionApp()

# Load environment variables
client_id = os.environ.get("PLAID_CLIENT_ID")
secret = os.environ.get("PLAID_SECRET")
azure_conn_str = os.environ.get("AZURE_CONN_STR")
extract_container_name = os.environ.get("EXTRACT_CONTAINER_NAME", "raw-data")
output_container_name = os.environ.get("OUTPUT_CONTAINER_NAME", "transformed-data")
blob_service = BlobServiceClient.from_connection_string(conn_str=azure_conn_str)

if not all([client_id, secret, azure_conn_str]):
    raise ValueError("Missing one or more required environment variables.")

#calculate start and end date
first_day_of_this_month = date.today().replace(day=1)
end_date = first_day_of_this_month - timedelta(days=1)
start_date = end_date.replace(day=1)

@app.function_name(name="extract_plaid_data")
@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="myTimer", run_on_startup=True, use_monitor=True)
def extract_plaid_data(myTimer: func.TimerRequest)-> None:

    if myTimer.past_due:
        logging.info("Time is past due")
    
    logging.info("Starting plaid data extraction")

    configuration = Configuration(
        host= "https://sandbox.plaid.com",
        api_key={
            'clientId': client_id,
            'secret': secret
        }
    )

    api_client = plaid_api.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    sandbox_request = SandboxPublicTokenCreateRequest(
            institution_id="ins_109508",
            initial_products=[Products("transactions")]
    )

    response = client.sandbox_public_token_create(sandbox_request)
    public_token = response['public_token']
    logging.info("public token generated")

    exchange_request = ItemPublicTokenExchangeRequest(public_token=public_token)
    exchange_response = client.item_public_token_exchange(exchange_request)
    access_token = exchange_response['access_token']
    logging.info("access token obtained")

    #waiting for data to be ready
    time.sleep(5)
    logging.info("waiting for the data to be ready")


    transaction_request = TransactionsGetRequest(
        access_token = access_token,
        start_date = start_date,
        end_date = end_date
    )

    transaction_response = client.transactions_get(transaction_request)
    plaid_data = transaction_response.to_dict()
    logging.info(f"Retrieved len{plaid_data.get('transactions',[])} transactions")
    
    extract_container_client = blob_service.get_container_client(extract_container_name)

    try:
        extract_container_client.create_container()
    except Exception:
        pass  # Ignore if container already exists

    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    filename = f"to_process/plaid_raw_{start_str}_{end_str}.json"

    blob_client = extract_container_client.get_blob_client(filename)
    blob_client.upload_blob(json.dumps(plaid_data, indent=2, default=str), overwrite=True)
    logging.info("Uploaded to blob")

@app.function_name(name="transform_load_plaid_data")
@app.blob_trigger(arg_name="triggeredBlob", path= "raw-data/to_process/{name}.json", connection="AZURE_CONN_STR")
def transform_load_plaid_data(triggeredBlob: func.InputStream) -> None:

    folder_prefix = "to_process/"
    extract_container_client = blob_service.get_container_client(extract_container_name)
    transformed_container_client = blob_service.get_container_client(output_container_name)

    def transaction_data(plaid_data):
        transaction_list = []
        for txn in plaid_data.get('transactions', []):
            transaction_elements = {
                'transaction_id': txn.get('transaction_id'),
                'account_id': txn.get('account_id'),
                'amount': txn.get('amount'),
                'iso_currency_code': txn.get('iso_currency_code'),
                'date': txn.get('date'),
                'transaction_type': txn.get('transaction_type'),
                'confidence_level': txn.get('personal_finance_category', {}).get('confidence_level'),
                'pending': txn.get('pending', False),
                'category': txn.get('personal_finance_category', {}).get('primary'),
                'merchant_name': txn.get('name'),
                'payment_channel': txn.get('payment_channel'),
                'website': txn.get('website', None)
            }
            transaction_list.append(transaction_elements)
        return transaction_list
    
    def account_data(plaid_data):
        account_list = []
        for acc in plaid_data.get('accounts', []):
            account_elements = {
                'account_id': acc.get('account_id'),
                'name': acc.get('name'),
                'official_name': acc.get('official_name'),
                'type': acc.get('type'),
                'subtype': acc.get('subtype'),
                'holder_category': acc.get('holder_category', None),
                'current_balance': acc.get('balances', {}).get('current'),
                'available_balance': acc.get('balances', {}).get('available'),
                'iso_currency_code': acc.get('balances', {}).get('iso_currency_code'),
            }
            account_list.append(account_elements)
        return account_list

    plaid_data_list = []
    blob_names = []

    for blob in extract_container_client.list_blobs(name_starts_with=folder_prefix):
        if blob.name.endswith(".json"):
            blob_client = extract_container_client.get_blob_client(blob.name)
            blob_content = blob_client.download_blob().readall()
            json_obj = json.loads(blob_content)
            plaid_data_list.append(json_obj)
            blob_names.append(blob.name)

    for data in plaid_data_list:

        transactions_list = transaction_data(data)

        accounts_list = account_data(data)
        
        transactions_df = pd.DataFrame.from_dict(transactions_list)

        accounts_df = pd.DataFrame.from_dict(accounts_list)

        transactions_df['date'] = pd.to_datetime(transactions_df['date'], errors='coerce')
        transactions_df['category'] = transactions_df['category'].fillna('Uncategorized').str.title()
        transactions_df['merchant_name'] = transactions_df['merchant_name'].fillna('Unknown').str.strip()
        transactions_df['payment_channel'] = transactions_df['payment_channel'].fillna('Unknown').str.title()
        transactions_df['transaction_type'] = transactions_df['transaction_type'].fillna('Unknown').str.title()
        transactions_df['iso_currency_code'] = transactions_df['iso_currency_code'].fillna('UNKNOWN').str.upper()
        transactions_df['pending'] = transactions_df['pending'].astype(bool)
        transactions_df = transactions_df.drop_duplicates(subset='transaction_id')


        accounts_df['name'] = accounts_df['name'].fillna('Unknown Account').str.strip()
        accounts_df['official_name'] = accounts_df['official_name'].fillna('Unknown Account').str.strip()
        accounts_df['type'] = accounts_df['type'].fillna('Unknown').str.title()
        accounts_df['subtype'] = accounts_df['subtype'].fillna('Unknown').str.title()
        accounts_df['holder_category'] = accounts_df['holder_category'].fillna('Unknown').str.title()
        accounts_df['current_balance'] = accounts_df['current_balance'].fillna(0).astype(float)
        accounts_df['available_balance'] = accounts_df['available_balance'].fillna(0).astype(float)
        accounts_df['iso_currency_code'] = accounts_df['iso_currency_code'].fillna('UNKNOWN').str.upper()
        accounts_df = accounts_df.drop_duplicates(subset='account_id')

        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')

        accounts_buffer = StringIO()
        accounts_df.to_csv(accounts_buffer, index=False)
        accounts_content = accounts_buffer.getvalue()
        accounts_blob_name = f"accounts_data/accounts_transformed_{start_str}_{end_str}.csv"

        # Prepare buffer for transactions data and upload
        transactions_buffer = StringIO()
        transactions_df.to_csv(transactions_buffer, index=False)
        transactions_content = transactions_buffer.getvalue()
        transactions_blob_name = f"transactions_data/transactions_transformed_{start_str}_{end_str}.csv"

        try:
            transformed_container_client.create_container()
        except Exception:
            pass  # Ignore if container already exists

        # Upload blobs with overwrite=True to replace if exists (usually timestamp ensures unique name)
        transformed_container_client.upload_blob(name=accounts_blob_name, data=accounts_content, overwrite=True)
        transformed_container_client.upload_blob(name=transactions_blob_name, data=transactions_content, overwrite=True)

        logging.info(f"Uploaded transactions data to the {output_container_name}/{transactions_blob_name}")
        logging.info(f"Uploaded accounts data to the {output_container_name}/{accounts_blob_name}")
    
    for name in blob_names:
        source_blob = extract_container_client.get_blob_client(name)
        dest_name = name.replace("to_process/","processed/")
        dest_blob = extract_container_client.get_blob_client(dest_name)

        copy_source_url = source_blob.url
        dest_blob.start_copy_from_url(copy_source_url)

        logging.info("COPYING...")
        time.sleep(1)
        logging.info("COPYING DONE.")
        
        source_blob.delete_blob()

        logging.info(f"Moved blob from {name} to {dest_name}")