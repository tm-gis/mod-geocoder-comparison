import sys
import os
import psycopg2
sys.path.append(r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\py")
from compare_geocoders_using_test_suite import get_test_suite
from subprocess import call
import arcpy

spreadsheet = "1b0zxcb_5w0M6ydStkVlL9ceIAs5P_gJhdQNLfhd0pyA"
spreadsheet_range = "Locations!A1:S"
db_name = "geocoder_test_suite"
user = "postgres"
password = ""
host = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\db"
port = "5432"
gdb = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\data.gdb"
taxlots_shp = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp\taxlots.shp"
shp_dir = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\shp"


# start up the server using pg_ctl
def start_server():
    try:
        call(["pg_ctl", "-D", "G:\PUBLIC\GIS\Geocoding\geocoder_comparison\db", "start"])
    except:
        pass
    return

test_suite_legend, test_suite_data_list = get_test_suite(spreadsheet, spreadsheet_range)
connection_string = "dbname=%s user=%s password=%s host=%s port=%s" % (db_name, user, password, host, port)


def import_test_suite():
    start_server()
    conn = psycopg2.connect(connection_string)

    field_type_dict = {"Location_ID": "integer primary key",
                       "Category": "varchar(255)",
                       "Type": "varchar(255)",
                       "Source": "varchar(255)",
                       "Name": "varchar(255)",
                       "Prefix": "varchar(25)",
                       "House_Number": "varchar(25)",
                       "Unit_Type": "varchar(25)",
                       "Unit_Number": "varchar(25)",
                       "Street_Name": "varchar(255)",
                       "Street_Type": "varchar(25)",
                       "City": "varchar(25)",
                       "State": "varchar(25)",
                       "Zip": "varchar(5)",
                       "Stop_ID": "varchar(25)",
                       "X_Coord": "varchar(25)",
                       "Y_Coord": "varchar(25)",
                       "Lat": "double precision",
                       "Lon": "double precision"}

    create_table_sql = "CREATE TABLE locations ("
    for l in test_suite_legend:
        if l != test_suite_legend[len(test_suite_legend) - 1]:
            create_table_sql += (l + " " + field_type_dict[l] + ", ")
        else:
            create_table_sql += (l + " " + field_type_dict[l])
    create_table_sql += ");"

    cur = conn.cursor()
    cur.execute(create_table_sql)

    add_geometry_sql = "SELECT AddGeometryColumn('locations', 'geometry', '4326', 'POINT', 2);"
    cur.execute(add_geometry_sql)

    for data_row in test_suite_data_list:
        insert_sql = "INSERT INTO locations (" + ", ".join(t for t in test_suite_legend) + \
                     ") VALUES (" + "%s, " * (len(test_suite_legend) - 1) + "%s);"
        insert_row = data_row[:]
        insert_row[test_suite_legend.index("Location_ID")] = int(insert_row[test_suite_legend.index("Location_ID")])
        insert_row[test_suite_legend.index("Lat")] = float(insert_row[test_suite_legend.index("Lat")])
        insert_row[test_suite_legend.index("Lon")] = float(insert_row[test_suite_legend.index("Lon")])
        cur.execute(insert_sql, insert_row)

    update_geometry_sql = "UPDATE locations SET geometry = ST_SetSRID(ST_MakePoint(Lon, Lat), 4326);"
    cur.execute(update_geometry_sql)
    conn.commit()
    cur.close()
    conn.close()
    return


def select_taxlots():
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
                       "Lon": "DOUBLE"}
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
        if t[test_suite_legend.index("Type")] in ["Intersections", "Bus Stop IDs"]:
            lat, lon = float(location_to_tlid[loc_id][3]), float(location_to_tlid[loc_id][4])
            west = lon - 0.00005
            east = lon + 0.00005
            north = lat + 0.00005
            south = lat - 0.00005
            vertices = [arcpy.Point(west, north), arcpy.Point(east, north), arcpy.Point(east, south),
                        arcpy.Point(west, south)]
            array = arcpy.Array(vertices)
            shape = arcpy.Polygon(array, wgs84)
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
    return

if __name__ == '__main__':
    # import_test_suite()
    select_taxlots()
