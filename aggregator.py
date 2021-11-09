#!/usr/bin/env python3

import argparse
import requests
import json
import pickle
import os

def cli():
    parser = argparse.ArgumentParser(description="Aggregates poll data from kobo toolbox via API")
    parser.add_argument('api_token', type=str,
                        help="Authenticate via api token. You can find it on your profile page.")
    parser.add_argument('--kpi-url', default="kf.kobotoolbox.org", type=str,
                        help="Kpi url used to retrieve data from.")
    args = parser.parse_args()

    # TODO: rm pickle stuff, only temporary, for quicker dev
    json_response = None
    pickle_path = "json_response.pkl"
    if os.path.isfile(pickle_path):
        with open(pickle_path, "rb") as pickle_file:
            json_response = pickle.load(pickle_file)
    else:
        json_response = download_assets(args.kpi_url, args.api_token)
        with open(pickle_path, "wb") as pickle_file:
            pickle.dump(json_response, pickle_file)

    print("Got {} assets in total".format(len(json_response["results"])))
    print(list(filter_assets(json_response, "Erntemengen - KW"))[0])

def download_assets(kpi_url: str, api_token: str):
    get_assets_url = "https://{}/api/v2/assets.json".format(kpi_url)
    print("Requesting assets from {}".format(get_assets_url))
    response = requests.get(get_assets_url, headers={'Authorization': "Token " + api_token})
    response.raise_for_status()
    json_response = response.json()
    return json_response

def filter_assets(json_response, asset_name_infix):
    for asset in json_response['results']:
        if not asset['has_deployment']: # only parse data from polls that are complete
            print("Filtered asset {} because it never was deployed.".format(asset['name']))
            continue
        if asset_name_infix not in asset['name']:
            print("Filtered asset {} due to name mismatch (does not contain '{}').".format(asset['name'], asset_name_infix))
            continue
        yield asset

if __name__ == "__main__":
    cli()
