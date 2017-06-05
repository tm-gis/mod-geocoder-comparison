import csv
import random
import datetime

in_text = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\random_open_addresses.txt"

address_list = []
with open(in_text, "r") as text_file:
    reader = csv.reader(text_file, delimiter="\t")
    for r in reader:
        if r[0] == "Category":
            legend = r
        else:
            address_list.append(r)
del text_file

qwerty_dict = {"a": "qwsz",
               "b": "vghn",
               "c": "xdfv",
               "d": "serfcx",
               "e": "wsdr",
               "f": "drtgvc",
               "g": "ftyhbv",
               "h": "gyujnb",
               "i": "ujko",
               "j": "huikmn",
               "k": "jiolm",
               "l": "pok",
               "m": "njk",
               "n": "bhjm",
               "o": "iklp",
               "p": "ol",
               "q": "wsa",
               "r": "edft",
               "s": "wedxza",
               "t": "rfgy",
               "u": "yhji",
               "v": "cfgb",
               "w": "qase",
               "x": "zsdc",
               "y": "tghu",
               "z": "asx"}


def typo(in_string):
    while True:
        random_index = random.randrange(0, len(in_string))
        current_letter = in_string[random_index].lower()
        if current_letter.isalpha():
            break
    typo_letters = qwerty_dict[current_letter]
    typo_index = random.randrange(0, len(typo_letters))
    typo_letter = typo_letters[typo_index]
    return '%s%s%s' % (in_string[:random_index], typo_letter, in_string[random_index+1:])


def wrong_suffix(in_string):
    suffix_list = ['AVE', 'ST', 'RD', 'DR', 'CT', 'BLVD', 'PL', 'PKWY', 'TER', '', 'LN', 'HWY', 'WAY', 'CIR']
    random_suffix_index = random.randint(0, len(suffix_list) - 1)
    if in_string in suffix_list:
        current_suffix_index = suffix_list.index(in_string)
        while current_suffix_index == random_suffix_index:
            random_suffix_index = random.randint(0, len(suffix_list) - 1)
    return suffix_list[random_suffix_index]


out_text = r"G:\PUBLIC\GIS\Geocoding\geocoder_comparison\csv\addresses_with_typos.txt"
with open(out_text, "w") as text_file:
    writer = csv.writer(text_file, delimiter="\t")
    writer.writerow(legend)
    for a in address_list:
        current = a[:]
        current[legend.index("Street_Name")] = typo(current[legend.index("Street_Name")])
        current[0] = "Anomaly"
        current[1] = "Misspelled Street"
        writer.writerow(current)

        current = a[:]
        current[legend.index("City")] = typo(current[legend.index("City")])
        current[0] = "Anomaly"
        current[1] = "Misspelled City"
        writer.writerow(current)

        current = a[:]
        current[legend.index("Street_Type")] = wrong_suffix(current[legend.index("Street_Type")])
        current[0] = "Anomaly"
        current[1] = "Wrong Suffix"
        writer.writerow(current)
del text_file
