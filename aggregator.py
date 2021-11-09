#!/usr/bin/env python3

import argparse
import requests
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
    parsed_poll_data = dict()
    for asset in filter_assets(json_response, "Erntemengen - KW"):
        week = parse_week_from(asset)
        if week not in parsed_poll_data:
            parsed_poll_data[week] = dict()
        week_dict = parsed_poll_data[week]

        location = parse_location_from(asset)
        if location in week_dict:
            raise RuntimeError("Got the same location ({}) twice for the same week ({}):\n{}".format(location, week, week_dict))
        submissions = download_submissions_from(asset, args.api_token)
        week_dict[location] = parse_submissions(submissions)
        print(week_dict)

def download_submissions_from(asset, api_token):
    get_submissions_url = asset['data']
    print("\tRequesting submission data {}".format(get_submissions_url))
    response = requests.get(get_submissions_url, headers={'Authorization': "Token " + api_token})
    response.raise_for_status()
    json_response = response.json()
    return json_response

def parse_submissions(assets_submissions):
    data = dict()
    data['submission_count'] = assets_submissions['count']
    data['vegetable_amounts'] = dict() # key: veggie type, value: list of submitted amount-wishes (as a number from -2 to 2). E.g. 'Zucchini': ['too_much_half','okay'] (two submissions)
    for submission in assets_submissions['results']:
        for key, value in submission.items():
            if 'group_rr' in key: # ignore data like "submission time", etc., only look at radio button results
                veggie_type = parse_veggie_type(key)
                if veggie_type not in data['vegetable_amounts']:
                    data['vegetable_amounts'][veggie_type] = []
                data['vegetable_amounts'][veggie_type].append(value)
    return data

def parse_veggie_type(radio_button_field_name):
    veggie_type_raw = key.split('/')[1]
    if veggie_type_raw == "M_hren":
        return "Möhren"
    else:
        return veggie_type_raw

def parse_location_from(asset):
    return asset['name'].split("Erntemengen - KW")[1].split(", ")[1]

def parse_week_from(asset):
    return asset['name'].split("Erntemengen - ")[1].split(",")[0]

def download_assets(kpi_url: str, api_token: str):
    get_assets_url = "https://{}/api/v2/assets.json".format(kpi_url)
    print("Requesting assets from {}".format(get_assets_url))
    response = requests.get(get_assets_url, headers={'Authorization': "Token " + api_token})
    response.raise_for_status()
    json_response = response.json()
    return json_response

def filter_assets(json_response, asset_name_infix):
    for asset in json_response['results']:
        if not asset['has_deployment']:
            print("x Skip - was never deployed '{}'".format(asset['name']))
            continue
        if asset_name_infix not in asset['name']:
            print("x Skip - mismatch           '{}' (does not contain '{}')".format(asset['name'], asset_name_infix))
            continue
        if "BETA" in asset['name']:
            print("x Skip - beta poll          '{}'".format(asset['name']))
            continue
        print("✓ Got asset                 '{}'".format(asset['name']))
        yield asset

if __name__ == "__main__":
    cli()
