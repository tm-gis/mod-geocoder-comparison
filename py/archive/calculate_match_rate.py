import datetime
import random
import time

import geocoder
import py.pickle
import requests
from ott.geocoder.geosolr import GeoSolr

from py.archive.geocoder_comp_2017 import parse_mapzen_response, rlis_geocode, getAddrAndLatLng

sample_addresses = py.pickle.load(open(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\py\pickle\address_samples.p", "rb"))

tm_geocoder_url = 'http://maps.trimet.org/solr'
tm_geocoder = GeoSolr(tm_geocoder_url)

# dict formatted text file of geocoders with API keys
geocoder_file = 'P:/geocoder_dict.txt'

# For Mapzen bbox parameter
bbox = "&boundary.rect.min_lon=-123.785578" \
        + "&boundary.rect.min_lat=44.683870" \
        + "&boundary.rect.max_lon=-121.651006" \
        + "&boundary.rect.max_lat=46.059685"

pdxlatlng = [45.5231, -122.6765]

with open(geocoder_file, 'r') as inf:
    geocoder_dict = eval(inf.read())


def create_test_suite(in_address):
    # in_address = [street_number,  0
    #               prefix,         1
    #               street_name,    2
    #               suffix,         3
    #               city,           4
    #               state,          5
    #               zip_code]       6
    def create_base_address():
        current = list(in_address)
        return " ".join(c for c in current[:4]) + ", " + current[4] + ", " + current[5]

    def no_directional_prefix():
        current = list(in_address)
        current.pop(1)
        return " ".join(c for c in current[:3]) + ", " + current[4]

    def no_state():
        current = list(in_address)
        return " ".join(c for c in current[:4]) + ", " + current[4]

    def wrong_suffix():
        current = list(in_address)
        suffix_list = ['AVE', 'ST', 'RD', 'DR', 'CT', 'BLVD', 'PL', 'BROADWAY', 'PKWY', 'TER', '', 'LN', 'HWY', 'WAY',
                       'LOOP', 'CIR']
        current_suffix_index = suffix_list.index(current[3])
        while current_suffix_index == suffix_list.index(current[3]):
            random.seed(datetime.datetime.now())
            current_suffix_index = random.randint(0, len(suffix_list) - 1)
        return " ".join(c for c in current[:3]) + " " + suffix_list[current_suffix_index] + ", " + current[5]

    def no_commas():
        current = list(in_address)
        return " ".join(c for c in current)

    def no_city():
        current = list(in_address)
        return " ".join(c for c in current[:4]) + ", " + current[5]

    def misspellings():
        current = list(in_address)
        vowels = ['A', 'E', 'I', 'O', 'U']
        random.seed(datetime.datetime.now())
        random_vowel = vowels[random.randint(0, len(vowels) - 1)]
        vowel_bool = False
        for v in vowels:
            if v in current[2]:
                current_vowel = current[2].index(v)
                vowel_bool = True
        if vowel_bool is False:
            current_vowel = current[2][random.randint(0, len(current[2]) - 1)]
        while current_vowel == random_vowel:
            random_vowel = vowels[random.randint(0, len(vowels) - 1)]
        return " ".join(c for c in current[:2]) + " " + current[2].replace(current[2], random_vowel) + \
               " " + current[3] + ", " + current[4] + ", " + current[5]

    return [create_base_address(), no_directional_prefix(), no_state(), wrong_suffix(), no_commas(), no_city()]

counter = 0
result_dict = {}
for address in sample_addresses:
    counter += 1
    if counter == 100:
        break
    test_suite_address_list = create_test_suite(address)
    for test_address in test_suite_address_list:
        base_address = " ".join(a for a in address[:4]) + ", " + address[4] + ", " + address[5]
        anAdd = test_address
        # print base_address, "||", anAdd
        for aGeocoder in geocoder_dict.keys():
            if aGeocoder not in result_dict:
                result_dict[aGeocoder] = {}
            if base_address not in result_dict[aGeocoder]:
                result_dict[aGeocoder][base_address] = []
            if aGeocoder == 'trimet':
                gc = tm_geocoder.query(anAdd, rows=1, start=0)
                try:
                    results = gc.results[0]
                    addr = (str.decode('{0}, {1}, OR'.format(results['name'].replace("'", "\''"), results['city'])))
                except:
                    addr = 'Null'
                    lat = 'Null'
                    lng = 'Null'
               # I have to call mapzen outside of the geocoder wrapper because
                # without the bbox parameters (which I can't pass through geocoder)
                # it was returning some very wacky results - Kansas, Iowa, FL -
                # and just in the first 10 records.
            elif aGeocoder == 'mapzen':
                request = "https://search.mapzen.com/v1/search?api_key=" \
                        + geocoder_dict[aGeocoder] + "&text=" + anAdd + bbox
                resp = requests.get(request, verify=False).text
                result = parse_mapzen_response(resp)
                addr = result['label']
                lat = result['latitude']
                lng = result['longitude']
            elif aGeocoder == 'rlis':
                result = rlis_geocode(anAdd, geocoder_dict[aGeocoder])
                if result:
                    addr = result['fullAddress']
                    lat = result['lat']
                    lng = result['lng']
                else:
                    addr = 'Null'
                    lat = 'Null'
                    lng = 'Null'
            else:
                this_geocoder = getattr(geocoder, aGeocoder)
                if geocoder_dict[aGeocoder] == '':
                    g = this_geocoder(anAdd)
                    while g.status == 'OVER_QUERY_LIMIT':
                        print ("over", aGeocoder,
                               "query limit - will try again in 30 minutes")
                        time.sleep(1800)
                        g = this_geocoder(anAdd)
                else:
                    # another possible parameter to pass for mapbox is
                    # proximity=pdxlatlng (makes it favor results near that
                    # point) but it didn't improve things for a small sample
                    g = this_geocoder(anAdd, key=geocoder_dict[aGeocoder])
                addr, lat, lng = getAddrAndLatLng(g)
            result_dict[aGeocoder][base_address].append(addr)
py.pickle.dump(result_dict, open(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\py\pickle\full_test_suite_results.p", "wb"))
