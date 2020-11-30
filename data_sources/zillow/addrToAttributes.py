#!/usr/bin/env python
# input: csv input data (tab-separated)
# output: csv output data (comma-separated?)
import os
import sys
import csv
import json
import requests

from time import sleep
from pprint import pprint

from bs4 import BeautifulSoup as BS

try:
    import zillow
    from zillow import ZillowError
except ImportError:
    print('pip install python-zillow')
    raise

try:
    sys.path.append('../google')
    from google_api import NeighborhoodLookup
    NL = NeighborhoodLookup()
except ImportError:
    print('google_api ImportError: extra neighborhood data will not be retrieved.')
    NL = None
finally:
    sys.path.remove('../google')


csvInput  = '../../data/SDaddr.csv'
csvOutput = '../../data/zillow_properties.csv'

api = zillow.ValuationApi()

# The attributes below are assigned later to ZillowProperty objects.
# The commented-out ones are just in case we want the data later.
ATTRIBUTES = [
    'id',
    'price',
    'url',
    'latitude',
    'longitude',
    'street',
    'city',
    'state',
    'zipcode',
    'neighborhood',
    'bed',
    'bath',
    'year_built',
    'size',
    #'lot_size',
    #'lot_size_units',
    'description',
]

# csv setup
if os.path.exists(csvOutput):
    writer = csv.writer(open(csvOutput, 'a', newline=''))
else:
    writer = csv.writer(open(csvOutput, 'w', newline=''))
    writer.writerow(ATTRIBUTES)

class ZillowProperty():
    def __init__(self, **kwargs):
        if not set(kwargs).issubset(ATTRIBUTES):
            raise ValueError('Input attributes are not contained in global attributes.')

        for attr in ATTRIBUTES:
            try:
                self.__setattr__(attr, kwargs[attr])
            except KeyError:
                self.__setattr__(attr, None) # helps find missing attributes
        return

    # convert string to attribute (see tryFill for how this is used)
    def __getitem__(self, key):
        return self.__getattribute__(key)

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    def getMissing(self):
        return [attr for attr in ATTRIBUTES if self.__getattribute__(attr) is None]

    # Tries to fill emtpy attributes with data but swallows any errors while
    # attempting to do so.
    #
    # returns True if the value was updated
    def tryFill(self, myattribute, data, keys):
        if self[myattribute] is not None:
            return False

        try:
            # it works...
            if len(keys) == 1:
                self[myattribute] = data[keys[0]]
                return True
            elif len(keys) == 2:
                self[myattribute] = data[keys[0]][keys[1]]
                return True
            else:
                raise ValueError
        except:
            return False

    # Returns a "row" intended for csv writing
    def getRow(self):
        return [self[a] for a in ATTRIBUTES]


# Fill out as much data as we can using python-zillow. The 'url' field is
# especially important since we will use it next to get more/missing info.
#
# TODO: verify latitude longitude using input data when possible
def getInitialData(key, address, zipcode, latitude=None, longitude=None, ident=0):

    # Throws exceptions if zillow returns weird stuff or errors
    data = api.GetSearchResults(key, address, zipcode)

    zp = ZillowProperty(**{
        'id': ident,

        'url': data.links.home_details,

        'latitude':  data.full_address.latitude,
        'longitude': data.full_address.longitude,
        'street':    data.full_address.street,
        'city':      data.full_address.city,
        'state':     data.full_address.state,
        'zipcode':   data.full_address.zipcode,

        'bed':        data.extended_data.bedrooms,
        'bath':       data.extended_data.bathrooms,
        'year_built': data.extended_data.year_built ,
        'size':       data.extended_data.finished_sqft,
        #'lot_size':   data.extended_data.lot_size_sqft,
    })

    return zp, data


# Fills in missing data using the url link returned by python-zillow.
#
# Some attributes are commented out with descriptions of what they represent
# in case they can be used later.
def fillIn(zp):
    h = zp.url.lstrip('https://').lstrip('www.zillow.com').replace(' ', '_').replace('/', '_')
    fname = f'html/{h}.html'

    if not os.path.exists('html'):
        os.mkdir('html')
    
    # Cache files to disk. This exists to reduce api calls but also because I 
    # needed to work without wifi for a little bit :).
    #
    # Note that for me, ~1400 files was 1.3 Gb on disk.
    if not os.path.exists(fname):
        headers = {}
        headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0'
        headers['DNT'] = '1'

        data = requests.get(zp.url, headers=headers)
        soup = BS(data.text, features='lxml')

        with open(fname, 'w') as f:
            f.write(soup.prettify())
    else:
        with open(fname) as f:
            soup = BS(f, features='lxml')

    # zillow returns weird data so the resulting code is funky
    tag = soup.find('script', {'id': 'hdpApolloPreloadedData'})
    jsondata = json.loads(list(tag.children)[0].strip())
    apicache = json.loads(jsondata['apiCache'])

    # The data here is expected to be split under 2 keys, both with relevant
    # information. We treat the data as unreliable and use all sources possible
    # to fill in missing data.
    for key in apicache.keys():
        # example key we're trying to match: 'VariantQuery{"zpid":67404285}'
        if 'VariantQuery' in key:
            moredata = apicache[key]['property']

            _ = zp.tryFill('price',         moredata, ['price'])
            _ = zp.tryFill('latitude',      moredata, ['latitude'])
            _ = zp.tryFill('longitude',     moredata, ['longitude'])
            _ = zp.tryFill('street',        moredata, ['streetAddress'])
            _ = zp.tryFill('city',          moredata, ['city'])
            _ = zp.tryFill('state',         moredata, ['state'])
            _ = zp.tryFill('zipcode',       moredata, ['zipcode'])
            _ = zp.tryFill('bed',           moredata, ['bedrooms'])
            _ = zp.tryFill('bath',          moredata, ['bathrooms'])
            _ = zp.tryFill('year_built',    moredata, ['yearBuilt'])
            _ = zp.tryFill('size',          moredata, ['livingArea'])

            #
            # Lot size and lot size units (acres, etc).
            #

            #z = zp.tryFill('lot_size',   moredata, ['lotAreaValue'])
            #if z:
            #    _ = zp.tryFill('lot_size_units', moredata, ['lotAreaUnit'])
            #        break

        # example key...: 'ForSaleDoubleScrollFullRenderQuery{"zpid":67404285,"contactFormRenderParameter":{"zpid":67404285,"platform":"desktop","isDoubleScroll":true}}'
        elif 'ForSaleDoubleScrollFullRenderQuery' in key:
            moredata = apicache[key]['property']

            _ = zp.tryFill('price',         moredata, ['price'])
            _ = zp.tryFill('latitude',      moredata, ['latitude'])
            _ = zp.tryFill('longitude',     moredata, ['longitude'])
            _ = zp.tryFill('street',        moredata, ['address', 'streetAddress'])
            _ = zp.tryFill('city',          moredata, ['address', 'city'])
            _ = zp.tryFill('state',         moredata, ['address', 'state'])
            _ = zp.tryFill('zipcode',       moredata, ['address', 'zipcode'])
            _ = zp.tryFill('neighborhood',  moredata, ['address', 'neighborhood'])
            _ = zp.tryFill('bed',           moredata, ['bedrooms'])
            _ = zp.tryFill('bath',          moredata, ['bathrooms'])
            _ = zp.tryFill('size',          moredata, ['livingArea'])

            #
            # parseable text data given by a human
            #

            _ = zp.tryFill('description', moredata, ['description'])
            zp.description = zp.description.replace('"', '') # this is for ONE property that
                                                             # has data that trips up cypher

            #
            # I think sqft is implied here since 'lotAreaValue' and 'lotAreaUnits'
            # should have acreage.
            #

            #z = zp.tryFill('lot_size',   moredata, ['lotSize'])
        else:
            with open('JAMES.txt', 'a') as f:
                f.write(key + '\n')

    return zp, apicache

def fillNeighborhood(zp):
    # A sneaky type coersion is here.
    if isinstance(zp.neighborhood, str):
        zp.neighborhood = [zp.neighborhood]

    if NL is None:
        return zp, None

    res = NL.neighborhood_lookup((zp.latitude, zp.longitude), None)
    if 'neighborhood' in res:
        zp.neighborhood = res['neighborhood']
    
    return zp, res


count = 0

#if __name__ == '__main__':
if True:

    # zillow api key expected to be at ~/.zkey
    with open(os.path.join(os.path.expanduser('~'), '.zkey')) as f:
        key = f.read().strip()

    with open(csvInput) as f:
        for i, row in enumerate(csv.reader(f, delimiter='\t')):
            address, zipcode, latitude, longitude = row

            try:
                zp, apidata = getInitialData(key, address, zipcode, latitude, longitude, i)
            except ZillowError:
                print(f'Zillow api did not like this property: {address}', file=sys.stderr)
                continue

            _, a = fillIn(zp)
            _, b = fillNeighborhood(zp)
            
            writer.writerow(zp.getRow())

            sleep(.5)    
