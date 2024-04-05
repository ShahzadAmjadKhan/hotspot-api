import concurrent.futures
import csv
import os
import pandas as pd
import logging
import threading
import multiprocessing
from retry_requests import retry

HOTSPOT_CSV_FILENAME = "hotspot_data.csv"
HOTSPOT_INFO_CSV_FILENAME = "hotspot_info_data.csv"
ORG_OUI_CSV_FILENAME = "org_oui_data.csv"
CACHE_SIZE = 100
WORKER_THREAD_COUNT = multiprocessing.cpu_count()


def call_api(url, session, log=False):
    try:
        if log:
            logging.info("fetching data from url:{}".format(url))

        response = session.get(url)
        if response.status_code == 200:
            if log:
                logging.info("data fetched from url:{}".format(url))
            return response  # Assuming API returns JSON response
        else:
            logging.warning("Response failed for url {} with threadId: {}".format(url, threading.get_ident()))
            return 'API call failed'
    except Exception as e:
        logging.warning("Exception for url:".format(url), e)
        return 'API call failed'


def write_to_csv(data, file_name):

    logging.info("Writing {} items in CSV file :{}".format(len(data), file_name))
    df = pd.DataFrame()
    if type(data) is list:
        df = pd.concat(data)
    else:
        df = data

    df.to_csv(file_name,
              mode='a',
              encoding='utf-8',
              index=False,
              quotechar='"',
              quoting=csv.QUOTE_ALL,
              header=not os.path.exists(file_name))

    logging.info("Written {} items in CSV file :{}".format(len(data), file_name))


def process_org_oui_data():

    with retry() as req_session:
        url = "https://entities.nft.helium.io/v2/oui/all"
        logging.info("Processing for Org oui data started")
        response = call_api(url, req_session, True)
        if response != 'API call failed':
            json = response.json()
            write_to_csv(pd.json_normalize(json['orgs']), ORG_OUI_CSV_FILENAME)

        logging.info("Processing for Org oui data completed")


def process_hotspots_data():

    with retry() as req_session:
        logging.info("Processing for hotspots data started")
        sub_networks = ['mobile', 'iot']
        start_url = "https://entities.nft.helium.io/v2/hotspots?subnetwork={}"

        for sub_network in sub_networks:
            logging.info("Processing for hotspots subnetwork {} data started".format(sub_network))
            url = start_url.format(sub_network)
            json = call_api(url, req_session, True).json()
            write_to_csv(pd.json_normalize(json['items']), HOTSPOT_CSV_FILENAME)
            start_url_with_cursor = url + "&cursor={}"
            cursor = json['cursor']
            while cursor:
                url = start_url_with_cursor.format(cursor)
                response = call_api(url, req_session, True)
                json = response.json()
                cursor = json['cursor']
                write_to_csv(pd.json_normalize(json['items']), HOTSPOT_CSV_FILENAME)

            logging.info("Processing for hotspots subnetwork {} data ended".format(sub_network))


def process_hotspot_info_records(records):
    cache = []

    processed_records_csv = 0
    fetch_records = 0
    base_url = "https://entities.nft.helium.io/v2/hotspot/{}"
    logging.info("Starting processing of {} records".format(len(records)))
    with retry() as session:
        for key in records:
            url = base_url.format(key)
            response = call_api(url, session)
            fetch_records += 1
            if fetch_records % 10 == 0:
                logging.info("{} items fetched".format(fetch_records))

            if response != 'API call failed':
                cache.append(pd.json_normalize(response.json()))

            if len(cache) == CACHE_SIZE:
                write_to_csv(cache, HOTSPOT_INFO_CSV_FILENAME)
                processed_records_csv += CACHE_SIZE
                logging.info("{} items added in CSV".format(fetch_records))
                cache = []

    # Write remaining items in cache to CSV
    if cache:
        write_to_csv(cache, HOTSPOT_INFO_CSV_FILENAME)
        logging.info("Finished processing of {} records".format(len(records)))


def process_hotspot_info_data():

    logging.info("Processing for hotspot info started with thread count {}".format(WORKER_THREAD_COUNT))
    with open(HOTSPOT_CSV_FILENAME, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header if present

        records = [row[0] for row in csv_reader]  # Extracting keys from CSV

    # Split records into chunks for threading
    chunk_size = len(records) // 1000  # Dividing records into 1000 chunks
    record_chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKER_THREAD_COUNT) as executor:
        executor.map(process_hotspot_info_records, record_chunks)

    logging.info("Processing for hotspot info ended  with thread count {}".format(WORKER_THREAD_COUNT))


def delete_old_csv_files():
    if os.path.exists(HOTSPOT_INFO_CSV_FILENAME):
        os.remove(HOTSPOT_INFO_CSV_FILENAME)

    if os.path.exists(HOTSPOT_CSV_FILENAME):
        os.remove(HOTSPOT_CSV_FILENAME)

    if os.path.exists(ORG_OUI_CSV_FILENAME):
        os.remove(ORG_OUI_CSV_FILENAME)


def process_helium_data():
    process_hotspots_data()
    process_org_oui_data()
    process_hotspot_info_data()


def init():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(thread)d [%(threadName)s] %(message)s', level=logging.INFO)


if __name__ == '__main__':

    init()
    logging.info("Helium Hotspot data extraction started")
    delete_old_csv_files()
    process_helium_data()
    logging.info("Helium Hotspot data extraction started")
