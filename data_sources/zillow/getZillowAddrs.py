#!/usr/bin/env python
# - This script prints info of properties up for sale in San Diego meant to be
#   passed to python-zillow.
# - Outputs tab-separated rows of address, zip code, latitude, and longitude.
# - All output lines should have an address/zip but some will not have latitude/longitude.
# - Requires list of zip codes to search if running as a script.
#
# ex: ./getZillowAddrs.py | sort | uniq >filename.csv
#
import os
import sys
import json
import requests
import random

from time import sleep

from bs4 import BeautifulSoup as BS

sep = '\t'
theyknow = 0

def _zip_GetSearch(zipcode, price):
    global theyknow

    ret = []

    # This is mostly copied+pasted from a browser request.
    searchQueryState = {
        'usersSearchTerm': zipcode,

        'filterState': {
            'isAllHomes': {'value': True},
            'monthlyPayment': {'max': 9000, 'min': 100},
            #'price': {'max': 500000, 'min': 100000}
            'price': price
        },
        'isListVisible': True,
        'isMapVisible': True,
        'mapBounds': {
            'east': -117.14017609448241,
            'north': 32.76853622440369,
            'south': 32.70702450626666,
            'west': -117.34101990551757
        },
        'mapZoom': 13,
        'pagination': {},
        'regionSelection': [{'regionId': 96649, 'regionType': 7}]
    }

    # This is copied+pasted from a browser request.
    wants = {
        'cat1': ['listResults', 'mapResults']
    }

    url  =  'https://www.zillow.com/search/GetSearchPageState.htm'
    # dump json objects without any whitespace to match what zillow.com does normally.
    url += f'?searchQueryState={json.dumps(searchQueryState, separators=(",", ":"))}'
    url += f'&wants={json.dumps(wants, separators=(",", ":"))}'
    url +=  '&requestId=1'

    headers = {}
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0'
    headers['DNT'] = '1'

    # Send and extract request
    data = requests.get(url, headers=headers)
    soup = BS(data.text, features='lxml')

    # The result is (expected to be) a large json object enclosed in a few tags:
    # <html><body><p>{object}</p></body></html>
    jsondata = json.loads(soup.find('p').contents[0])

    if jsondata['user']['isBot']:
        if not theyknow:
            print(f'THEY KNOW ({zipcode})', file=sys.stderr)
        theyknow += 1

    # Iterate over all result categories that exist
    for key in ['listResults', 'mapResults', 'relaxedResults']:
        results = jsondata['cat1']['searchResults'][key]

        # shouldn't happen
        if not isinstance(results, list):
            print(f'WEIRD: {results}', file=sys.stderr)
            continue

        # each result is expected to be a real estate property
        #
        # Goal is to get an address and lat/long coordinates. Address is
        # mandatory but lat/long are optional.
        for result in results:

            # Address
            try:
                address = result['address']
            except KeyError:
                # Note that we could attempt to construct an address from the
                # 'streetAddress' key in results['hdpData'], but we'd have to make sure
                # catch all edge cases (like apartment unit numbers).
                with open('debug.txt', 'a') as f:
                    f.write(f'result has no address(?): f{result}\n\n')
                continue

            # Lat/Long
            if 'latLong' in result:
                latitude = result['latLong']['latitude']
                longitude = result['latLong']['longitude']
            else:
                try:
                    latitude = result['hdpData']['homeInfo']['latitude']
                    longitude = result['hdpData']['homeInfo']['longitude']
                except KeyError:
                    latitude = ''
                    longitude = ''

            ret.append((address, latitude, longitude))
    
    return ret

def zip_GetSearch(zipcode):
    return _zip_GetSearch(zipcode, {'max': 750000})
    #return _zip_GetSearch(zipcode, price={'max': 750000}) + \
    #       _zip_GetSearch(zipcode, price={'min': 750000}) 

# @zipcode: string zipcode
# returns list of tuples
def zip_rb(zipcode, prevzip='92111'):
    ret = []

    # Setup request
    #url = f'https://www.zillow.com/homes/{zipcode}_rb/'
    #url += '?fromHomePage=true'
    #url += '&shouldFireSellPageImplicitClaimGA=false'
    #url += '&fromHomePageTab=buy'

    url = f'https://www.zillow.com/homes/for_sale/{zipcode}_rb/'
    url += 'any_days/globalrelevanceex_sort/11_zm/0_mmm'

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

                # Not sure what this means but it's safe to discard these.
                if address == '--':
                    continue

                try:
                    # this often doesn't actually match the input zipcode
                    returned_zip = jsondata['address']['postalCode']
                except KeyError: # I haven't observed this
                    continue

                try:
                    latitude = str(jsondata['geo']['latitude'])
                except KeyError:
                    latitude = ''

                try:
                    longitude = str(jsondata['geo']['longitude'])
                except KeyError:
                    longitude = ''

                ret.append((address, returned_zip, latitude, longitude))

    return ret

if __name__ == '__main__':
    hd = '/home/james/2019-jlogan/DSE203/proj/203_final/'

    try:
        os.remove('debug.txt')
    except:
        pass

    # Load zip codes from disk
    with open(os.path.join(hd, 'data/SDzip.txt')) as f:
        zips = [z.strip() for z in f.readlines()]

    for zipcode in zips:
        #if int(zipcode) < 92121:
        #    continue
        results = zip_rb(zipcode, random.choice(zips))

        for res in results:
            print(sep.join(res))

        sleep(1)

    if theyknow:
        print(f'They caught us {theyknow} times.')

    if os.path.exists('debug.txt'):
        print('Debug file created.', file=sys.stderr)
