import arcpy
import os
import pickle
import csv
import random
import datetime

county_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\rlis_co"
oregon_bg_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\bg_OR"
washington_bg_fc = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb\bg_WA"
gdb = r"G:\PUBLIC\GIS\Geocoding\Log_Files\data.gdb"
open_addresses_dir = r"G:\PUBLIC\GIS\Geocoding\OpenAddresses\openaddresses_us_west_03-16-17\us"
county_pop_pickle_file = r"G:\PUBLIC\GIS\Geocoding\Log_Files\python\pickle\county_pop_dict.p"
address_sample_pickle_file = r"G:\PUBLIC\GIS\Geocoding\Log_Files\python\pickle\address_samples.p"

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
    seven_counties_list = ["Multnomah", "Clackamas", "Washington", "Clark", "Yamhill", "Polk", "Marion"]
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


def sample_open_addresses(county_pop_dict):
    # pull number of openadresses addresses proportional to population
    total_population = sum(county_pop_dict.values()) - county_pop_dict["Yamhill"] - county_pop_dict["Polk"] - \
                       county_pop_dict["Marion"]
    address_samples_list = []

    for county in county_pop:
        if county not in ["Yamhill", "Polk", "Marion"]:  # Counties not in OpenAddresses
            county_proportion = int(round((county_pop_dict[county] / total_population) * 1000))
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

address_samples = sample_open_addresses(county_pop)
pickle.dump(address_samples, open(address_sample_pickle_file, "wb"))
arcpy.Delete_management(seven_counties)
arcpy.Delete_management(block_groups)
