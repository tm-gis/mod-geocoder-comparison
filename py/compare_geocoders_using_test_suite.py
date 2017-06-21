from __future__ import print_function
import geocoder
import requests
import json
import time
from unidecode import unidecode
from math import radians, cos, sin, asin, sqrt
from ott.geocoder.geosolr import GeoSolr
import datetime
import os
import psycopg2
import subprocess

pdxlatlng = [45.5231, -122.6765]
# dict formatted text file of geocoders with API keys
geocoder_file = 'P:/geocoder_dict.txt'
results_dir = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\results"
current = "%02d" % datetime.datetime.now().month + "%02d" % datetime.datetime.now().day + \
          str(datetime.datetime.now().year)[2:]
database_name = "geocoder_test_suite"
user = "postgres"
password = ""
host = "127.0.0.1"
port = "5432"
table = "public.location_polygons"


def start_server():
    try:
        subprocess.call(["pg_ctl", "-D", "X:\geocoder\db", "start"])
    except:
        pass
    return


def get_test_suite_from_postgres(db_name, db_user, db_password, db_host, db_port, table_name):
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" % (db_name, db_user, db_host, db_password,
                                                                               db_port))
    cur = conn.cursor()
    select_query = "SELECT * from " + table_name
    cur.execute(select_query)
    rows = cur.fetchall()
    test_suite_data = []
    for row in rows:
        test_suite_data.append(row)

    legend_query = "SELECT column_name FROM information_schema.columns WHERE table_schema='%s' AND table_name='%s'" % \
                   (table_name[:table_name.index(".")], table_name[table_name.index(".") + 1:])
    cur.execute(legend_query)
    rows = cur.fetchall()
    test_suite_legend = []
    for row in rows:
        test_suite_legend.append(row[0])
    cur.close()
    conn.close()
    return test_suite_legend, test_suite_data


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
            latitude = -1
            longitude = -1
    except AttributeError:
        address = ''
        latitude = -1
        longitude = -1
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
            gdict['longitude'] = -1
            gdict['latitude'] = -1
    except ValueError:
        gdict['label'] = ''
        gdict['longitude'] = -1
        gdict['latitude'] = -1
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


def get_geocoder_input(test_suite_data_row, test_suite_data_legend):
    test_suite_columns = {("Top User Submissions", "Log Files"): "Name",
                          ("Intersections", "Log Files"): "Name",
                          ("Commonly Misspelled Locations", "Log Files"): "Name",
                          ("Multifamily Residential", "MAF Multifamily"): "Address",
                          ("Transit POI", "TriMet Geodata"): "Name",
                          ("Theoretical Addresses", "MAF Interpolation"): "Address",
                          ("eFare Outlets", "Validated Landmarks"): "Name",
                          ("Landmarks", "*"): "Name",
                          ("Leading Zero Addresses", "MAF"): "Address",
                          ("Transit POI", "Log files - Stop ID"): "Name",
                          ("Vancouver, WA Addresses", "User Selected"): "Address",
                          ("Locations with Aliases", "User Selected"): "Address",
                          ("Venues", "FourSquare Check-ins"): "Name",
                          ("Proportional to Population", "Open Addresses"): "Address",
                          ("Misspelled Street", "Open Addresses"): "Address",
                          ("Misspelled City", "Open Addresses"): "Address",
                          ("Wrong Suffix", "Open Addresses"): "Address",
                          ("Transposed Street", "Open Addresses"): "Address",
                          ("Renamed Streets", "Hillsboro"): "Address",
                          ("Bus Stop IDs", "Log files - Stop ID"): "Name",
                          ("Vancouver Addresses", "Open Addresses"): "Address"}

    test_suite_name = ", ".join(i if i else "" for i in [test_suite_data_row[test_suite_data_legend.index("name")],
                                                         test_suite_data_row[test_suite_data_legend.index("city")],
                                                         test_suite_data_row[test_suite_data_legend.index("state")]])

    street_address = " ".join(j if j else "" for j in [test_suite_data_row[test_suite_data_legend.index("house_numb")],
                                                       test_suite_data_row[test_suite_data_legend.index("prefix")],
                                                       test_suite_data_row[test_suite_data_legend.index("street_nam")],
                                                       test_suite_data_row[test_suite_data_legend.index("street_typ")],
                                                       test_suite_data_row[test_suite_data_legend.index("unit_type")],
                                                       test_suite_data_row[test_suite_data_legend.index("unit_numbe")]]) + ", " +\
                     ", ".join(k if k else "" for k in [test_suite_data_row[test_suite_data_legend.index("city")],
                                                        test_suite_data_row[test_suite_data_legend.index("state")]])

    key = (test_suite_data_row[test_suite_data_legend.index("type")],
           test_suite_data_row[test_suite_data_legend.index("source")])

    if key[0] == "Landmarks":
        return street_address, "Address"
    else:
        if test_suite_columns[key] == "Name":
            return test_suite_name, "Name"
        elif test_suite_columns[key] == "Address":
            return street_address, "Address"


def get_geocoder_result(address, geocoder_name, geocoder_dict):
    trimet_geocoder_url = 'http://maps.trimet.org/solr'
    trimet_geocoder = GeoSolr(trimet_geocoder_url)
    # For Mapzen bbox parameter
    bbox = "&boundary.rect.min_lon=-123.785578" \
           + "&boundary.rect.min_lat=44.683870" \
           + "&boundary.rect.max_lon=-121.651006" \
           + "&boundary.rect.max_lat=46.059685"

    try:
        if geocoder_name == "trimet_solr":
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
        elif geocoder_name == "trimet_pelias":
            url = "http://cs-dv-mapgeo02/ws/pelias/v1/search?text=" + address
            response = requests.get(url).json()
            # result = parse_mapzen_response(response)
            # (result_lat, result_long, address) = [result['latitude'], result['longitude'], result['label']]
            (result_lat, result_long, address) = [response['features'][0]["geometry"]["coordinates"][1],
                                                  response['features'][0]["geometry"]["coordinates"][0],
                                                  response['features'][0]["properties"]["label"]]
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
    test_suite_legend, test_suite_data_list = get_test_suite_from_postgres(database_name, user, password, host, port,
                                                                           table)
    with open(geocoder_file, 'r') as inf:
        geocoder_api_dict = eval(inf.read())

    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" % (database_name, user, host, password,
                                                                               port))
    field_type_dict = {"Location_ID": "integer",
                       "Geocoder": "varchar(25)",
                       "Address_Submitted": "varchar(255)",
                       "Geocoder_Response": "varchar(255)",
                       "Geocoder_Lat": "double precision",
                       "Geocoder_Lon": "double precision"}

    fields_list = ["Location_ID", "Geocoder", "Address_Submitted", "Geocoder_Response", "Geocoder_Lat", "Geocoder_Lon"]

    create_table_sql = "CREATE TABLE IF NOT EXISTS geocoder_responses ("
    for f in fields_list:
        if f != fields_list[len(fields_list) - 1]:
            create_table_sql += (f + " " + field_type_dict[f] + ", ")
        else:
            create_table_sql += (f + " " + field_type_dict[f])
    create_table_sql += ");"
    cur = conn.cursor()
    cur.execute(create_table_sql)

    geocoder_responses = {}

    for t in test_suite_data_list:
        loc_id = int(t[test_suite_legend.index("location_i")])

        geocoder_input, input_type = get_geocoder_input(t, test_suite_legend)
        geocoder_responses[geocoder_input] = {}

        for g in geocoder_api_dict.keys():
            # if g != "google":
            geocoder_lat, geocoder_long, geocoder_address = get_geocoder_result(geocoder_input, g, geocoder_api_dict)
            insert_sql = "INSERT INTO geocoder_responses (" + ", ".join(f for f in fields_list) + \
                         ") VALUES (" + "%s, " * (len(fields_list) - 1) + "%s);"
            insert_row = [loc_id, g, geocoder_input, geocoder_address, geocoder_lat, geocoder_long]
            try:
                cur.execute(insert_sql, insert_row)
            except:
                print (insert_sql, insert_row)
    add_geometry_sql = "SELECT AddGeometryColumn('geocoder_responses', 'geocoder_response_geom', '4326', 'POINT', 2);"
    cur.execute(add_geometry_sql)

    update_geometry_sql = """UPDATE geocoder_responses SET geocoder_response_geom =
                             ST_SetSRID(ST_MakePoint(geocoder_lon, geocoder_lat), 4326)
                             WHERE geocoder_lon < -1 AND geocoder_lat > 0;"""
    cur.execute(update_geometry_sql)
    conn.commit()
    cur.close()
    conn.close()
    return


def get_results():
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s port=%s" % (database_name, user, host, password,
                                                                               port))
    cur = conn.cursor()
    create_table_sql = "CREATE TABLE IF NOT EXISTS results (" + \
                       """location_id integer,
                          geocoder varchar(25),
                          distance_feet double precision);"""
    cur.execute(create_table_sql)
    for i in range(1, 2001):
        select_query = """SELECT geocoder, geocoder_response_geom, geom, ST_DISTANCE(
                          ST_TRANSFORM(geom, 2913),
                          ST_TRANSFORM(geocoder_response_geom, 2913))
                          FROM public.geocoder_responses, public.location_polygons
                          WHERE location_i = location_id and location_i = """ + str(i) + ";"
        cur.execute(select_query)
        rows = cur.fetchall()
        for r in rows:
            insert_sql = "INSERT INTO results (location_id, geocoder, distance_feet) VALUES (%s, %s, %s);"
            insert_data = [i, r[0], r[3]]
            cur.execute(insert_sql, insert_data)
    conn.commit()
    cur.close()
    conn.close()
    return


def create_shapefile():
    command = """pgsql2shp -f %s -h %s -u %s %s %s %s %s""" % \
              (r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\geocoder_responses.shp",
               host, user, password, "geocoder_test_suite", "public.geocoder_responses",
               "SELECT * FROM public.geocoder_responses")
    os.system(command)
    return

if __name__ == '__main__':
    start_server()
    evaluate_test_suite()
    get_results()
    # create_shapefile()
