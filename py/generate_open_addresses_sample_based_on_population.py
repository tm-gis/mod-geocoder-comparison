import arcpy
import os
import pickle
import csv
import random
import datetime
from ott.geocoder.geosolr import GeoSolr

county_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\rlis_co"
oregon_bg_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\bg_OR"
washington_bg_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\bg_WA"
gdb = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb"
open_addresses_dir = r"G:\PUBLIC\GIS\Geocoding\OpenAddresses\openaddresses_us_west_03-16-17\us"
county_pop_pickle_file = r"G:\PUBLIC\GIS\Geocoding\Log_Files\python\pickle\county_pop_dict.p"
address_sample_pickle_file = r"G:\PUBLIC\GIS\Geocoding\Log_Files\python\pickle\address_samples.p"
addresses_text = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\random_open_addresses.txt"

arcpy.env.overwriteOutput = True


def create_base_layers(in_county, in_oregon_bg, in_washington_bg):
    # make layer of seven counties of interest
    seven_counties_list = ["Multnomah", "Clackamas", "Washington", "Clark", "Yamhill", "Polk", "Marion"]
    seven_counties_lyr = arcpy.MakeFeatureLayer_management(in_county, "seven_counties", "\"COUNTY\" IN ('" +
                                                           "', '".join(c for c in seven_counties_list) + "')")

    # make layer of block groups that intersect the cities in the seven counties
    merged_bg_fc = os.path.join(gdb, "merged_bg")
    arcpy.Merge_management([in_oregon_bg, in_washington_bg], merged_bg_fc)
    return seven_counties_lyr, merged_bg_fc


def calculate_population_by_county(in_county, in_bg):
    # iterate through cities, clip block groups to city boundary and proportionally assign population
    # seven_counties_list = ["Multnomah", "Clackamas", "Washington", "Clark", "Yamhill", "Polk", "Marion"]
    seven_counties_list = ["Multnomah", "Clackamas", "Washington"]
    county_pop_dict = {}

    for county in seven_counties_list:
        print county
        if county not in county_pop_dict:
            county_pop_dict[county] = 0

        current_county_lyr = arcpy.MakeFeatureLayer_management(in_county, "current_county", "\"COUNTY\" = '" + county + "'")
        clipped_bg_lyr = arcpy.Clip_analysis(in_bg, current_county_lyr, os.path.join(gdb, "clipped_bg"))
        arcpy.AddField_management(clipped_bg_lyr, "Clipped_Area", "DOUBLE")
        arcpy.CalculateField_management(clipped_bg_lyr, "Clipped_Area", "!SHAPE.AREA!", "Python_9.3")

        for bg in arcpy.da.SearchCursor(clipped_bg_lyr, ["ALAND", "AWATER", "B01001_001", "Clipped_Area"]):
            county_pop_dict[county] += ((int(bg[2]) if bg[2] else 0) * (bg[3] / (bg[0] + bg[1])))

        arcpy.Delete_management(current_county_lyr)
        arcpy.Delete_management(clipped_bg_lyr)

    return county_pop_dict


def sample_open_addresses(county_pop_dict, sample_size):
    # pull number of openadresses addresses proportional to population
    total_population = sum(county_pop_dict.values())
    address_samples_list = []

    for county in county_pop:
        if county not in ["Yamhill", "Polk", "Marion"]:  # Counties not in OpenAddresses
            county_proportion = int(round((county_pop_dict[county] / total_population) * sample_size))
            county_addresses = []
            current_county_address_list = []
            if county == "Clark":
                state = "wa"
            else:
                state = "or"
            current_address_file = os.path.join(os.path.join(open_addresses_dir, state), county.lower() + ".csv")
            with open(current_address_file, "rb") as csv_file:
                reader = csv.reader(csv_file, delimiter=",")
                for r in reader:
                    # legend = ["LON",
                    #           "LAT",
                    #           "NUMBER",
                    #           "STREET",
                    #           "UNIT",
                    #           "CITY",
                    #           "DISTRICT",
                    #           "REGION",
                    #           "POSTCODE",
                    #           "ID",
                    #           "HASH"]
                    num = r[2]
                    street = r[3]
                    cty = r[5]
                    st = r[7] if r[7] else state.upper()
                    zp = r[8]
                    if r[0] != "LON":
                        if num and street and cty and st and zp:
                            if street.count(" ") >= 2:
                                if street.split(" ")[0] in ["NW", "NE", "SW", "SE", "N", "S", "E", "W"]:
                                    prefix = street.split(" ")[0]
                                    suffix = street.split(" ")[street.count(" ")]
                                    street_name = " ".join(s for s in street.split(" ")[1:len(street.split(" ")) - 1])
                                else:
                                    prefix = ""
                                    suffix = street.split(" ")[street.count(" ")]
                                    street_name = " ".join(s for s in street.split(" ")[0:len(street.split(" ")) - 1])
                            elif street.count(" ") == 1:
                                prefix = ""
                                street_name, suffix = street.split(" ")
                            else:
                                prefix = ""
                                street_name = street
                                suffix = ""
                            a = [num, prefix, street_name, suffix, cty, st, zp]
                            current_county_address_list.append(a)
            random.seed(datetime.datetime.now())

            while len(county_addresses) < county_proportion:
                random_number = random.randint(1, len(current_county_address_list))
                if current_county_address_list[random_number] not in county_addresses:
                    county_addresses.append(current_county_address_list[random_number])
            address_samples_list += county_addresses
    return address_samples_list

seven_counties, block_groups = create_base_layers(county_fc, oregon_bg_fc, washington_bg_fc)
county_pop = calculate_population_by_county(seven_counties, block_groups)
pickle.dump(county_pop, open(county_pop_pickle_file, "wb"))
# county_pop = pickle.load(open(r"G:\PUBLIC\GIS\Geocoding\Log_Files\python\pickle\county_pop_dict.p", "rb"))

address_samples = sample_open_addresses(county_pop, 100)
pickle.dump(address_samples, open(address_sample_pickle_file, "wb"))
arcpy.Delete_management(seven_counties)
arcpy.Delete_management(block_groups)

trimet_geocoder_url = 'http://maps.trimet.org/solr'
trimet_geocoder = GeoSolr(trimet_geocoder_url)

with open(addresses_text, "wb") as text_file:
    writer = csv.writer(text_file, delimiter="\t")
    legend = ['Category', 'Type', 'Source', 'Name', 'Prefix', 'House_Number', 'Unit_Type', 'Unit_Number', 'Street_Name',
              'Street_Type', 'City', 'State', 'Zip', 'Stop_ID', 'X_Coord', 'Y_Coord', 'Lat', 'Lon']
    writer.writerow(legend)
    for a in address_samples:
        input_address = " ".join(i for i in [a[0], a[1], a[2], a[3]]) + ", " + a[4] + ", " + a[5] + " " + a[6]
        response = trimet_geocoder.query(input_address, rows=1, start=0).results[0]
        row = ["Residential", "Proportional to Population", "Open Addresses", "", a[1], a[0], "", "", a[2], a[3], a[4],
               a[5], a[6], "", "", "", response['lat'], response['lon']]
        writer.writerow(row)
del text_file
