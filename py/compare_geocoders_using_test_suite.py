from __future__ import print_function
import geocoder
import requests
import json
import time
from unidecode import unidecode
from math import radians, cos, sin, asin, sqrt
import csv
import pickle
from ott.geocoder.geosolr import GeoSolr
import datetime
import shutil
import httplib2
import os
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

pdxlatlng = [45.5231, -122.6765]
# dict formatted text file of geocoders with API keys
geocoder_file = 'P:/geocoder_dict.txt'
spreadsheet = "1b0zxcb_5w0M6ydStkVlL9ceIAs5P_gJhdQNLfhd0pyA"
spreadsheet_range = 'Locations!A1:S'
results_dir = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\results"
current = "%02d" % datetime.datetime.now().month + "%02d" % datetime.datetime.now().day + \
          str(datetime.datetime.now().year)[2:]


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 3956 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 5280


def get_address_lat_lng(geocoder_response):
    """
    This function works for all geocoders except trimet and mapzen
    """
    try:
        if geocoder_response.status not in ["ERROR", "ZERO_RESULTS"]:
            address = unidecode(geocoder_response.address.replace("'", "\''"))
            latitude = geocoder_response.lat
            longitude = geocoder_response.lng
        else:
            address = ''
            latitude = 0
            longitude = 0
    except AttributeError:
        address = ''
        latitude = 0
        longitude = 0
    return float(latitude), float(longitude), address


def parse_mapzen_response(txt):
    """
    `txt` is a string containing JSON-formatted text from Mapzen's API

    returns a dictionary containing the useful key/values from the most
       relevant result.
    """
    gdict = {}  # just initialize a dict for now, with status of None
    try:
        data = json.loads(txt)
        if data['features']:  # it has at least one feature...
            gdict['status'] = 'OK'
            feature = data['features'][0]  # pick out the first one
            props = feature['properties']  # just for easier reference
            gdict['confidence'] = props['confidence']
            gdict['label'] = unidecode(props['label']).replace("'", "\''")

            # now get the coordinates
            coords = feature['geometry']['coordinates']
            gdict['longitude'] = coords[0]
            gdict['latitude'] = coords[1]
        else:
            gdict['label'] = ''
            gdict['longitude'] = 0
            gdict['latitude'] = 0
    except ValueError:
        gdict['label'] = ''
        gdict['longitude'] = 0
        gdict['latitude'] = 0
    return gdict


def rlis_geocode(addr_str, token):
    """Take an input address string, send it to the rlis api and return
    a dictionary that are the state plane coordinated for that address,
    handle errors in the request fails in one way or another. Copied
    from Grant Humphries's 'geocode_employer_records.py' (on github)"""

    url = 'http://gis.oregonmetro.gov/rlisapi2/locate/'
    params = {
        'token': token,
        'input': addr_str,
        'form': 'json'
    }
    rsp = requests.get(url, params=params)

    if rsp.status_code != 200:
        return -1, -1, -1
    else:
        json_rsp = rsp.json()
        if json_rsp['error']:
            return -1, -1, -1
        else:
            return json_rsp['data'][0]['lat'], json_rsp['data'][0]['lng'], json_rsp['data'][0]['fullAddress']


def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def get_test_suite(spreadsheet_id, range_name):
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discovery_url)

    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
    values = result.get('values', [])

    data_legend = []
    data_list = []
    for row in values:
        if row[0] == "Location_ID":
            data_legend = [str(r) for r in row]
        else:
            current_row = [str(r) for r in row]
            data_list.append(current_row)
    return data_legend, data_list


def get_geocoder_input(test_suite_data_row, test_suite_data_legend):
    test_suite_columns = {("Top User Submissions", "Log Files"): "Name",
                          ("Intersections", "Log Files"): "Name",
                          ("Commonly Misspelled Locations", "Log Files"): "Name",
                          ("Multifamily Residential", "MAF Multifamily"): "Address",
                          ("Transit POI", "TriMet Geodata"): "Name",
                          ("Theoretical Addresses", "MAF Interpolation"): "Address",
                          ("eFare Outlets", "Validated Landmarks"): "Name",
                          ("POIs", "*"): "Address",
                          ("Leading Zero Addresses", "MAF"): "Address",
                          ("Transit POI", "Log files - Stop ID"): "Name",
                          ("Vancouver, WA Addresses", "User Selected"): "Address",
                          ("Locations with Aliases", "User Selected"): "Address"}

    test_suite_name = ", ".join(k for k in [test_suite_data_row[test_suite_data_legend.index("Name")],
                                            test_suite_data_row[test_suite_data_legend.index("City")],
                                            test_suite_data_row[test_suite_data_legend.index("State")]])
    street_address = ((test_suite_data_row[test_suite_data_legend.index("House_Number")])
                      if test_suite_data_row[test_suite_data_legend.index("House_Number")] else "") + \
                     ((" " + test_suite_data_row[test_suite_data_legend.index("Prefix")])
                      if test_suite_data_row[test_suite_data_legend.index("Prefix")] else "") + \
                     ((" " + test_suite_data_row[test_suite_data_legend.index("Street_Name")])
                      if test_suite_data_row[test_suite_data_legend.index("Street_Name")] else "") + \
                     ((" " + test_suite_data_row[test_suite_data_legend.index("Street_Type")])
                      if test_suite_data_row[test_suite_data_legend.index("Street_Type")] else "") + \
                     ((", " + test_suite_data_row[test_suite_data_legend.index("Unit_Type")])
                      if test_suite_data_row[test_suite_data_legend.index("Unit_Type")] else "") + \
                     ((" " + test_suite_data_row[test_suite_data_legend.index("Unit_Number")])
                      if test_suite_data_row[test_suite_data_legend.index("Unit_Number")] else "") + ", " + \
                     ", ".join(j for j in [test_suite_data_row[test_suite_data_legend.index("City")],
                                           test_suite_data_row[test_suite_data_legend.index("State")]])

    key = (test_suite_data_row[test_suite_data_legend.index("Type")],
           test_suite_data_row[test_suite_data_legend.index("Source")])

    if key[0] == "POIs":
        return street_address
    else:
        if test_suite_columns[key] == "Name":
            return test_suite_name
        elif test_suite_columns[key] == "Address":
            return street_address


def get_geocoder_result(address, geocoder_name, geocoder_dict):
    trimet_geocoder_url = 'http://maps.trimet.org/solr'
    trimet_geocoder = GeoSolr(trimet_geocoder_url)
    # For Mapzen bbox parameter
    bbox = "&boundary.rect.min_lon=-123.785578" \
           + "&boundary.rect.min_lat=44.683870" \
           + "&boundary.rect.max_lon=-121.651006" \
           + "&boundary.rect.max_lat=46.059685"

    try:
        if geocoder_name == "trimet":
            (result_lat, result_long, address) = [trimet_geocoder.query(address, rows=1, start=0).results[0]['lat'],
                                                  trimet_geocoder.query(address, rows=1, start=0).results[0]['lon'],
                                                  (trimet_geocoder.query(address, rows=1, start=0).results[0]['address'] if
                                                   'address' in trimet_geocoder.query(address, rows=1, start=0).results[0]
                                                   else trimet_geocoder.query(address, rows=1, start=0).results[0]['name'])]
        elif geocoder_name == "mapzen":
            url = "https://search.mapzen.com/v1/search?api_key=" + geocoder_dict[geocoder_name] + "&text=" + address + bbox
            response = requests.get(url).text
            result = parse_mapzen_response(response)
            (result_lat, result_long, address) = [result['latitude'], result['longitude'], result['label']]
        elif geocoder_name == "rlis":
            (result_lat, result_long, address) = rlis_geocode(address, geocoder_dict[geocoder_name])
        else:
            this_geocoder = getattr(geocoder, geocoder_name)
            if geocoder_dict[geocoder_name] == '':
                result = this_geocoder(address)
                while result.status == 'OVER_QUERY_LIMIT':
                    print ("over", geocoder_name,
                           "query limit - will try again in 30 minutes")
                    time.sleep(1800)
                    result = this_geocoder(address)
            else:
                result = this_geocoder(address, key=geocoder_dict[geocoder_name])
            (result_lat, result_long, address) = get_address_lat_lng(result)
    except TypeError:
        (result_lat, result_long, address) = [-1, -1, -1]
    except KeyError:
        (result_lat, result_long, address) = [-1, -1, -1]
    except IndexError:
        (result_lat, result_long, address) = [-1, -1, -1]
    return result_lat, result_long, address


def evaluate_test_suite():
    test_suite_legend, test_suite_data_list = get_test_suite(spreadsheet, spreadsheet_range)
    with open(geocoder_file, 'r') as inf:
        geocoder_api_dict = eval(inf.read())

    results_dict = {}
    type_source_total = {}
    geocoder_responses = {}
    for t in test_suite_data_list:
        test_suite_type = t[test_suite_legend.index("Type")]
        source = t[test_suite_legend.index("Source")]
        name = t[test_suite_legend.index("Name")]
        prefix = t[test_suite_legend.index("Prefix")]
        house_number = t[test_suite_legend.index("House_Number")]
        unit_type = test_suite_legend.index("Unit_Type")
        unit_number = t[test_suite_legend.index("Unit_Number")]
        street_name = t[test_suite_legend.index("Street_Name")]
        street_type = t[test_suite_legend.index("Street_Type")]
        city = t[test_suite_legend.index("City")]
        state = t[test_suite_legend.index("State")]
        test_suite_zip = t[test_suite_legend.index("Zip")]
        stop_id = t[test_suite_legend.index("Stop_ID")]
        x_coord = t[test_suite_legend.index("X_Coord")]
        y_coord = t[test_suite_legend.index("Y_Coord")]
        test_suite_lat = float(t[test_suite_legend.index("Lat")])
        test_suite_lon = float(t[test_suite_legend.index("Lon")])

        geocoder_input = get_geocoder_input(t, test_suite_legend)
        geocoder_responses[geocoder_input] = {}

        for g in geocoder_api_dict.keys():
            # if g != "google":
            if g not in results_dict:
                results_dict[g] = {}
            if (test_suite_type, source) not in results_dict[g]:
                results_dict[g][(test_suite_type, source)] = 0
            if (test_suite_type, source) not in type_source_total:
                type_source_total[(test_suite_type, source)] = 0
            # if g != "google":
            geocoder_lat, geocoder_long, geocoder_address = get_geocoder_result(geocoder_input, g, geocoder_api_dict)
            if haversine(test_suite_lon, test_suite_lat, geocoder_long, geocoder_lat) <= 50:
                results_dict[g][(test_suite_type, source)] += 1
            if g == "trimet":
                if test_suite_type != "POIs":
                    type_source_total[(test_suite_type, source)] += 1
                else:
                    if ("POIs", "Landmarks") not in type_source_total:
                        type_source_total[("POIs", "Landmarks")] = 0
                    type_source_total[("POIs", "Landmarks")] += 1
            geocoder_responses[geocoder_input][g] = [geocoder_lat, geocoder_long, geocoder_address, test_suite_lat,
                                                     test_suite_lon]

    pickle.dump(geocoder_responses, open(os.path.join(out_dir, "responses.p"), "wb"))

    poi_dict = {}
    results_file = os.path.join(out_dir, "results.txt")
    with open(results_file, "wb") as text_file:
        writer = csv.writer(text_file, delimiter="\t")
        legend = ["Geocoder", "Type", "Source", "Correctly Geocoded", "Total Addresses", "PERCENTAGE CORRECT"]
        writer.writerow(legend)
        for r in results_dict:
            poi_dict[r] = {}
            for (t, s) in results_dict[r]:
                if t != "POIs":
                    row = [r, t, s, results_dict[r][(t, s)], type_source_total[(t, s)],
                           float(results_dict[r][(t, s)]) / float(type_source_total[(t, s)])]
                    writer.writerow(row)
                else:
                    if ("POIs", "Landmarks") not in poi_dict[r]:
                        poi_dict[r][("POIs", "Landmarks")] = 0
                    poi_dict[r][("POIs", "Landmarks")] += results_dict[r][(t, s)]
        for p in poi_dict:
            row = [p, "POIs", "Landmarks", poi_dict[p][("POIs", "Landmarks")], type_source_total[("POIs", "Landmarks")]]
            writer.writerow(row)
    del text_file

    responses_file = os.path.join(out_dir, "responses.txt")
    with open(responses_file, "wb") as text_file:
        writer = csv.writer(text_file, delimiter="\t")
        legend = ["Geocoder", "Input", "Geocoded Latitude", "Geocoded Longitude", "Geocoded Address",
                  "Correct Latitude", "Correct Longitude"]
        writer.writerow(legend)
        for geocoder_input in geocoder_responses:
            for geocoder in geocoder_responses[geocoder_input]:
                row = [geocoder, geocoder_input, geocoder_responses[geocoder_input][geocoder][0],
                       geocoder_responses[geocoder_input][geocoder][1], geocoder_responses[geocoder_input][geocoder][2],
                       geocoder_responses[geocoder_input][geocoder][3], geocoder_responses[geocoder_input][geocoder][4]]
                writer.writerow(row)
    del text_file
    return


if __name__ == '__main__':
    out_dir = os.path.join(results_dir, current)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.mkdir(out_dir)
    evaluate_test_suite()
