import requests
import csv
import datetime
import os

HOTSPOT_CSV_FILENAME = "hotspot_data.csv"
ORG_OUI_CSV_FILENAME = "org_oui_data.csv"


def org_oui_data(req_session):
    url = "https://entities.nft.helium.io/v2/oui/all"
    print("{}: Processing Org Oui".format(datetime.datetime.now()))
    response = req_session.get(url)
    json = response.json()

    print("{}: Preparing Org Oui CSV".format(datetime.datetime.now()))
    create_csv(json, ORG_OUI_CSV_FILENAME, json['orgs'][0].keys(), 'orgs')
    print("{}: Completed Org Oui CSV ".format(datetime.datetime.now()))


def create_csv(json_data, file_name, header, items_key):
    with open(file_name, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        if header:
            csv_writer.writerow(header)

        for item in json_data[items_key]:
            csv_writer.writerow(item.values())


def hotspots_data(req_session):
    sub_networks = ['mobile', 'iot']
    start_url = "https://entities.nft.helium.io/v2/hotspots?subnetwork={}"

    for index, sub_network in enumerate(sub_networks):
        print("{}: Processing Hotspots for {}".format(datetime.datetime.now(), sub_network))
        url = start_url.format(sub_network)
        response = req_session.get(url)
        json = response.json()
        cursor = json['cursor']

        if index == 0:
            headers = json['items'][0].keys()
        else:
            headers = None

        print("{}: Preparing CSV Hotspots for {}".format(datetime.datetime.now(), sub_network))

        create_csv(json, HOTSPOT_CSV_FILENAME, headers, 'items')
        start_url_with_cursor = url + "&cursor={}"
        while cursor:
            url = start_url_with_cursor.format(cursor)
            response = req_session.get(url)
            json = response.json()
            cursor = json['cursor']
            create_csv(json, HOTSPOT_CSV_FILENAME, None, 'items')

        print("{}: Completed CSV Hotspots for {}".format(datetime.datetime.now(), sub_network))


def delete_files():
    if os.path.exists(HOTSPOT_CSV_FILENAME):
        os.remove(HOTSPOT_CSV_FILENAME)

    if os.path.exists(ORG_OUI_CSV_FILENAME):
        os.remove(ORG_OUI_CSV_FILENAME)


if __name__ == '__main__':
    print("{}: Data extraction started".format(datetime.datetime.now()))

    delete_files()
    session = requests.session()
    hotspots_data(session)
    org_oui_data(session)

    print("{}: Data extraction ended".format(datetime.datetime.now()))