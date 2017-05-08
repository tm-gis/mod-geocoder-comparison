import arcpy
import random
import datetime
import csv

maf_shp = r"G:\PUBLIC\Mailing_lists\GIS\Mailing_requests\template_data\merged_maf_2016_11.shp"
out_file = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\theoretical_addresses.csv"

address_dict = {}
address_list = []
coord_dict = {}
for row in arcpy.da.SearchCursor(maf_shp, ["HOUSE", "UNIT_TYPE", "UNIT_NO", "FDPRE", "FNAME", "FTYPE", "JURIS_CITY",
                                           "STATE", "ZIP"]):
    house = row[0]
    unit_type = row[1]
    unit_no = row[2]
    fdpre = row[3]
    fname = row[4]
    ftype = row[5]
    juris_city = row[6]
    state = row[7]
    zip_code = row[8]

    if (fdpre, fname, ftype, juris_city, zip_code) not in address_dict:
        address_dict[(fdpre, fname, ftype, juris_city, zip_code)] = []
        address_list.append((fdpre, fname, ftype, juris_city, zip_code))
    address_dict[(fdpre, fname, ftype, juris_city, zip_code)].append(house)

sample_list = []
streets_list = []
while len(sample_list) < 100:
    print len(sample_list)
    random.seed(datetime.datetime.now())
    random_street = ("SW", "TEST", "STREET", "UNINCORPORATED", "OR", "97038")

    while random_street[3] == "UNINCORPORATED" or random_street in streets_list:
        random_street = address_list[random.randint(0, len(address_list) - 1)]

    streets_list.append(random_street)
    print random_street
    number_list = []
    for i in address_dict[random_street]:
        try:
            number_list.append(int(i))
        except:
            pass

    min_number = min(number_list)
    max_number = max(number_list)
    print (min_number, max_number)

    random.seed(datetime.datetime.now())
    random_house = str(min_number)
    if min_number != max_number:
        while random_house in address_dict[random_street]:
            random_house = str(random.randint(int(min_number), int(max_number)))
    elif (max_number - min_number) == 1:
        random_house = str(max_number + 1)
    else:
        random_house = str(int(random_house) + 1)

    current = [random_house]
    for rs in random_street:
        current.append(rs)

    if current not in sample_list:
        sample_list.append(current)

with open(out_file, "wb") as csv_file:
    writer = csv.writer(csv_file, delimiter=",")
    for s in sample_list:
        house = s[0]
        fdpre = s[1]
        fname = s[2]
        ftype = s[3]
        juris_city = s[4]
        zip_code = s[5]

        row = [fdpre, house, fname, ftype, juris_city, "OR", zip_code]
        writer.writerow(row)
del csv_file
