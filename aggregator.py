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
    parser.add_argument('--kpi-url', default="https://kf.kobotoolbox.org/", type=str,
                        help="Kpi url used to retrieve data from.")
    args = parser.parse_args()

    json_response = None
    pickle_path = "json_response.pkl"
    if os.path.isfile(pickle_path):
        with open(pickle_path, "rb") as pickle_file:
            json_response = pickle.load(pickle_file)
    else:
        json_response = download_assets(args.kpi_url, args.api_token)
        with open(pickle_path, "wb") as pickle_file:
            pickle.dump(pickle_file, json_response)
    print(len(json_response["results"]))
    #print(polls_received)
    #if len(polls_received) == 0:
    #    print("Error: no assets received.")
    #    return
    #print("Got {} assets".len(assets_received))

def download_assets(kpi_url: str, api_token: str):
    response = requests.get("https://{}/api/v2/assets.json".format(kpi_url), headers={'Authorization': "Token " + api_token})
    response.raise_for_status()
    json_response = response.json()
    return json_response

if __name__ == "__main__":
    cli()
