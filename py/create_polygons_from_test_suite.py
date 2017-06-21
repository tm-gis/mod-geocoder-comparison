import os
from subprocess import call
import arcpy
import math
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage
import httplib2

SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Google Sheets API Python Quickstart'

spreadsheet = "1b0zxcb_5w0M6ydStkVlL9ceIAs5P_gJhdQNLfhd0pyA"
spreadsheet_range = "Locations!A1:T"
gdb = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\data.gdb"
taxlots_shp = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\taxlots.shp"
shp_dir = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp"


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


def create_polygons():
    wgs84 = arcpy.SpatialReference(u'Geographic Coordinate Systems/World/WGS 1984')
    locations_fc = os.path.join(gdb, "locations")
    if not arcpy.Exists(locations_fc):
        arcpy.CreateFeatureclass_management(gdb, "locations", "POINT", "", "", "", wgs84)

    field_type_dict = {"Location_ID": "LONG",
                       "Category": "TEXT",
                       "Type": "TEXT",
                       "Source": "TEXT",
                       "Name": "TEXT",
                       "Prefix": "TEXT",
                       "House_Number": "TEXT",
                       "Unit_Type": "TEXT",
                       "Unit_Number": "TEXT",
                       "Street_Name": "TEXT",
                       "Street_Type": "TEXT",
                       "City": "TEXT",
                       "State": "TEXT",
                       "Zip": "TEXT",
                       "Stop_ID": "TEXT",
                       "X_Coord": "TEXT",
                       "Y_Coord": "TEXT",
                       "Lat": "DOUBLE",
                       "Lon": "DOUBLE",
                       "Request": "TEXT"}
    for t in test_suite_legend:
        arcpy.AddField_management(locations_fc, t, field_type_dict[t])
    insert_cursor = arcpy.da.InsertCursor(locations_fc, test_suite_legend + ["SHAPE@X", "SHAPE@Y"])
    for t in test_suite_data_list:
        row = t + [float(t[test_suite_legend.index("Lon")]), float(t[test_suite_legend.index("Lat")])]
        insert_cursor.insertRow((tuple(row)))
    del insert_cursor

    taxlots_lyr = arcpy.MakeFeatureLayer_management(taxlots_shp, "taxlots_lyr", "\"TLID\" NOT LIKE '%-STR%'")
    projected_locations_fc = os.path.join(gdb, "projected_locations")
    state_plane = arcpy.Describe(taxlots_shp).spatialReference
    arcpy.Project_management(locations_fc, projected_locations_fc, state_plane)
    locations_lyr = arcpy.MakeFeatureLayer_management(projected_locations_fc)

    join_fc = os.path.join(gdb, "locations_join")
    field_mappings = arcpy.FieldMappings()
    field_mappings.addTable(locations_lyr)
    field_mappings.addTable(taxlots_lyr)

    arcpy.SpatialJoin_analysis(locations_lyr, taxlots_lyr, join_fc, "JOIN_ONE_TO_ONE", "KEEP_ALL", field_mappings,
                               "CLOSEST")

    location_to_tlid = {}  # location_to_tlid[location_id] = [type, source, tlid, lat, lon]

    for row in arcpy.da.SearchCursor(join_fc, ["Location_ID", "Type", "Source", "TLID", "Lat", "Lon"]):
        location_to_tlid[row[0]] = [row[1], row[2], row[3], row[4], row[5]]

    tlid_list = [l[2] for l in location_to_tlid.values()]
    tlid_shape_dict = {}
    arcpy.MakeFeatureLayer_management(taxlots_shp, "selected_taxlots", "\"TLID\" IN ('" +
                                      "', '".join(t for t in tlid_list) + "')")

    projected_taxlots = os.path.join(gdb, "projected_taxlots")
    arcpy.Project_management("selected_taxlots", projected_taxlots, wgs84)
    for row in arcpy.da.SearchCursor(projected_taxlots, ["TLID", "SHAPE@"]):
        tlid_shape_dict[row[0]] = row[1]

    out_name = "location_polygons.shp"
    out_polygons = os.path.join(shp_dir, out_name)
    arcpy.CreateFeatureclass_management(shp_dir, out_name, "POLYGON", "", "", "", wgs84)
    for t in test_suite_legend:
        arcpy.AddField_management(out_polygons, t[:10], field_type_dict[t])
    insert_legend = [l[:10] for l in test_suite_legend]
    insert_cursor = arcpy.da.InsertCursor(out_polygons, insert_legend + ["SHAPE@"])
    for t in test_suite_data_list:
        loc_id = int(t[test_suite_legend.index("Location_ID")])
        if t[test_suite_legend.index("Type")] in ["Intersections", "Bus Stop IDs", "Theoretical Addresses"] or \
                        t[test_suite_legend.index("Category")] in ["Transit POI"]:
            lat, lon = float(location_to_tlid[loc_id][3]), float(location_to_tlid[loc_id][4])
            circle = []

            if t[test_suite_legend.index("Category")] in ["Transit POI"] or t[test_suite_legend.index("Type")] in \
                    ["Bus Stop IDs"]:
                buffer_distance = 0.00003
            else:
                buffer_distance = 0.0001
            for i in range(1, 361):
                r = math.radians(i)
                x = lon + buffer_distance * 1.5 * math.cos(r)
                y = lat + buffer_distance * math.sin(r)
                circle.append(arcpy.Point(x, y))

            circle_array = arcpy.Array(circle)
            shape = arcpy.Polygon(circle_array, wgs84)
        else:
            shape = tlid_shape_dict[location_to_tlid[loc_id][2]]
        row = t + [shape]
        insert_cursor.insertRow((tuple(row)))
    del insert_cursor
    arcpy.Delete_management(taxlots_lyr)
    arcpy.Delete_management(locations_lyr)
    arcpy.Delete_management(locations_fc)
    arcpy.Delete_management(projected_locations_fc)
    arcpy.Delete_management(join_fc)
    arcpy.Delete_management(projected_taxlots)
    return out_polygons


def import_test_suite(in_polygons, db_name):
    # start up the server using pg_ctl
    def start_server():
        try:
            call(["pg_ctl", "-D", "X:\geocoder\db", "start"])
        except:
            pass
        return
    start_server()
    command_line = "shp2pgsql -I -s 4326 %s | psql -U lint -d %s" % (in_polygons.lower(), db_name)
    os.system(command_line)
    return


if __name__ == '__main__':
    test_suite_legend, test_suite_data_list = get_test_suite(spreadsheet, spreadsheet_range)
    location_polygons = create_polygons()
    location_polygons = os.path.join(shp_dir, "location_polygons.shp")
    import_test_suite(location_polygons, "geocoder_test_suite")

# ---------------only used for the mapbox shapefile to json--------------------------
# # ogr2ogr -f GeoJSON locations.json location_polygons_mapbox.shp -lco RFC7946=YES
# arcpy.AddField_management("location_polygons_mapbox", "fill", "TEXT")
# arcpy.AddField_management("location_polygons_mapbox", "fill_opaci", "FLOAT")
# arcpy.AddField_management("location_polygons_mapbox", "title", "TEXT")
# arcpy.AddField_management("location_polygons_mapbox", "descriptio", "TEXT")
# # #2ca02c
# marker_color = {"POI": '#1f77b4', "Anomaly": '#ff7f0e', "Residential": '#ffe605', "Transit POI": '#d62728', "Business": '#9467bd'}
# fields = ["Category", "fill", "Category", "Type", "Source", "Name", "House_Numb", "Prefix", "Street_Nam", "Street_Typ",
#           "City", "title", "descriptio", "fill_opaci"]
# with arcpy.da.UpdateCursor("location_polygons_mapbox", fields) as cursor:
#     for row in cursor:
#         row[fields.index("fill")] = marker_color[row[fields.index("Category")]]
#         row[fields.index("fill_opaci")] = 0.3
#         row[fields.index("title")] = ", ".join(i for i in [row[fields.index("Category")], row[fields.index("Type")],
#                                                            row[fields.index("Source")]])
#         row[fields.index("descriptio")] = row[fields.index("Name")] + " " + " ".join(i for i in [
#             row[fields.index("House_Numb")], row[fields.index("Prefix")], row[fields.index("Street_Nam")],
#             row[fields.index("Street_Typ")]]) + ", " + row[fields.index("City")]
#         cursor.updateRow(row)
