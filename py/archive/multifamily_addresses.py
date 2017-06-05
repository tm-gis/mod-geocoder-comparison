import arcpy
import csv
import random
import datetime

maf_shp = r"G:\PUBLIC\Mailing_lists\GIS\Mailing_requests\template_data\merged_maf_2016_11.shp"
out_file = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\random_residential.csv"

address_list = []
for row in arcpy.da.SearchCursor(maf_shp, ["HOUSE", "UNIT_TYPE", "UNIT_NO", "FDPRE", "FNAME", "FTYPE", "MAIL_CITY",
                                           "STATE", "ZIP", "X_COORD", "Y_COORD"]):
    house = row[0]
    unit_type = row[1]
    unit_no = row[2]
    fdpre = row[3]
    fname = row[4]
    ftype = row[5]
    juris_city = row[6]
    state = row[7]
    zip_code = row[8]
    x_coord = row[9]
    y_coord = row[10]

    address_list.append((house, unit_type, unit_no, fdpre, fname, ftype, juris_city, state, zip_code, x_coord, y_coord))

sample_list = []
while len(sample_list) < 101:
    random.seed(datetime.datetime.now())
    random_address = address_list[random.randint(0, len(address_list) - 1)]
    # if random_address[1] != " " and random_address[2] != " " and random_address[6] != "UNINCORPORATED":
    if random_address[1] == " " and random_address[2] == " ":
        if random_address not in sample_list:
            sample_list.append(random_address)

with open(out_file, "wb") as csv_file:
    writer = csv.writer(csv_file, delimiter=",")
    for s in sample_list:
        house = s[0]
        unit_type = s[1]
        unit_no = s[2]
        fdpre = s[3]
        fname = s[4]
        ftype = s[5]
        juris_city = s[6]
        state = s[7]
        zip_code = s[8]
        x_coord = s[9]
        y_coord = s[10]

        row = [fdpre, house, unit_type, unit_no, fname, ftype, juris_city, state, zip_code, x_coord, y_coord]
        writer.writerow(row)
del csv_file
