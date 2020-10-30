#!/usr/bin/env python
# - This script prints addresses of properties up for sale in San Diego.
# - Outputs tab-separated rows of address, latitude, and longitude.
# - All output lines should have an address but some will not have latitude/longitude.
# - Requires list of zip codes to search if running as a script.
#
# ex: ./getListings.py | sort | uniq >filename.csv
#
import os
import json
import requests
import random

from time import sleep

from bs4 import BeautifulSoup as BS

sep = '\t'

# @zipcode: string zipcode
# returns list of tuples
def runZip(zipcode, prevzip='92111'):
    ret = []

    # Setup request
    url = f'https://www.zillow.com/homes/{zipcode}_rb/'

    # User-Agent is required at a minimum to prevent Zillow from not serving our
    # results. The other headers are to...throw them off the scent.
    headers = {}
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0'
    headers['Referer'] = f'https://www.zillow.com/homes/{prevzip}_rb/'
    headers['DNT'] = '1'

    # Send and extract request
    data = requests.get(url, headers=headers)
    soup = BS(data.text, features='lxml')

    # Most zip codes only return one 'ul' tag. Investigation could be done
    # into the instances where there are more than one, but it's not likely
    # worth the time.
    for properties in soup.find_all('ul', {'class': 'photo-cards'}):

        # Each 'ul' tag has multiple properties enclosed in 'li' tags.
        for prop in properties.find_all('li'):
            # Skip stuff like the following:
            # <li>1,328<abbr class="list-card-label"> <!-- -->sqft</abbr></li>
            if not (scripts := prop.find_all('script')):
                continue

            # This loop is generally a single iteration since most properties only
            # have one <script> tag.
            for script in scripts:
                # TODO: literally any validation
                jsondata = json.loads(script.contents[0])

                # Only other type I've seen is "Event"
                if jsondata['@type'] != 'SingleFamilyResidence':
                    continue

                # Below is an example "jsondata":
                #
                # Note that 'name' appears to be a reliable field, we don't
                # (necessarily) need to use the 'address' field to get the
                # address.
                #
                # {'@context': 'http://schema.org',
                # '@type': 'SingleFamilyResidence',
                # 'address': {'@context': 'http://schema.org',
                #             '@type': 'PostalAddress',
                #             'addressLocality': 'San Diego',
                #             'addressRegion': 'CA',
                #             'postalCode': '92128',
                #             'streetAddress': '13683 Essence Rd'},
                # 'floorSize': {'@context': 'http://schema.org',
                #             '@type': 'QuantitativeValue',
                #             'value': '1,831'},
                # 'geo': {'@context': 'http://schema.org',
                #         '@type': 'GeoCoordinates',
                #         'latitude': 32.969023,
                #         'longitude': -117.067697},
                # 'name': '13683 Essence Rd, San Diego, CA 92128',
                # 'url': 'https://www.zillow.com/homedetails/13683-Essence-Rd-San-Diego-CA-92128/16800594_zpid/'
                # }

                address = jsondata['name']

                try:
                    latitude = str(jsondata['geo']['latitude'])
                except KeyError:
                    latitude = ''

                try:
                    longitude = str(jsondata['geo']['longitude'])
                except KeyError:
                    longitude = ''

                ret.append((address, latitude, longitude))

    return ret

if __name__ == '__main__':
    hd = '/home/james/2019-jlogan/DSE203/proj/203_final/'

    # Load zip codes from disk
    with open(os.path.join(hd, 'data/SDzip.txt')) as f:
        zips = [z.strip() for z in f.readlines()]

    for zipcode in zips:
        #if int(zipcode) < 92121:
        #    continue
        results = runZip(zipcode, random.choice(zips))

        for res in results:
            print(sep.join(res))

        sleep(1)
