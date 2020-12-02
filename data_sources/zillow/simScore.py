#!/usr/bin/env python
import os
import sys
import json

from pprint import pprint
from string import digits

from py_stringmatching import Cosine

from neo4j import GraphDatabase


uri = 'bolt://localhost:7687'
user = 'neo4j'
pw = os.environ['NEO4JPW']

airbnb_json = './../../data/airbnb_listings.json'

cosine_sim = Cosine().get_sim_score


# attributes that need to be converted from strings
key_to_type = {
    'id':    int,
    'price': int,
    'size':  int,
    'bed':   float,
    'bath':  float, # zillow doesn't seem to use 'half' bathrooms but we allow it.
}

# All airbnb attributes that we store.
airbnb_attrs = [
    'id',
    'bed',
    'bath',
    'room_type_category',
    'neighborhood',
]

# zillow attributes that are named differently in airbnb
zillow_to_airbnb_keys = {
    'bed':  'bedrooms',
    'bath': 'bathrooms',
}


# for debugging purposes
def getobj(iterable, input_id):
    try:
        for obj in iterable:
            if obj.id == input_id:
                return obj
    except:
        pass
    return None

# Returns a value from 0 to 1 depending on how similar the numeric inputs
# are to each other.
#
# Note that this is designed so that num_sim(a,b,c) == num_sim(b,a,c)
#
# Take the example where:
# ratio = .3
# base_val = 100,000
#
# Then if "comp_val" has a price within 70,000 and 130,000, a value > 0 will be
# returned. 100,000 would be a perfect match and would return 1.
#
def num_sim(base_val, comp_val, ratio):
    diff = min(abs(base_val - comp_val) / ((base_val+comp_val)/2), ratio)
    return (ratio - diff) / ratio

# zillow property from graph
class ZPFG:
    def __init__(self, info):
        self.scores = {}    # track zillow vs zillow comparisons
        self.bnbscores = {} # track zillow vs airbnb comparisons
        self.print_keys = []

        for k, v in info.items():
            # ex: 'p.price'
            actual_key = k.split('.')[1]
            self.print_keys.append(actual_key)

            if actual_key in key_to_type:
                if v is None:
                    self[actual_key] = None
                else:
                    self[actual_key] = key_to_type[actual_key](v)
            else:
                self[actual_key] = v

        # Remove digits from street
        self.stripped_street = self.street.translate({ord(d): None for d in digits})

        # Convert neighborhood to set
        try:
            self.neighborhood_set = set(self.neighborhood)
        except TypeError:
            self.neighborhood_set = set()

    def __str__(self):
        return f'{dict(((k, self[k]) for k in self.print_keys))}'

    def __getitem__(self, key):
        return self.__getattribute__(key)

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    # This is called in an NxN fashion but only ~half of the of calls will do
    # anything since comparisons are symmetric.
    def zillowCompare(self, other):
        # Check if zillowCompare(other, self) was already called
        if self.id == other.id or self.id in other.scores:
            return

        score = 0

        # Price is most important factor; it sums up other attributes.
        if all((self.price, other.price)):
            score += .5 * num_sim(self.price, other.price, .3)

        # Comparing addresses is the most costly comparison and is therefore
        # currently designed for simplicity and speed.
        #
        # Possible modifications could be done here given more time:
        # - Since street names aren't unique, we could validate using gps coordinates
        # - Use some sort of edit distance to compare street names in a more robust fashion
        # - Make the comparison aware of abbreviations such as 'street' -> 'st' or
        #   'avenue' -> 'ave'.
        if self.stripped_street == other.stripped_street:
            score += .2

        # Compare beds and baths
        if all((self.bed, other.bed)):
            score += .1 * num_sim(self.bed,  other.bed,  .5)
        if all((self.bath, other.bath)):
            score += .1 * num_sim(self.bath, other.bath, .5)

        # Compare square footage
        if all((self.size, other.size)):
            score += .1 * num_sim(self.size, other.size, .3)

        self.scores[other.id] = score

    def airbnbCompare(self, other):
        score = 0

        # Compare beds and baths
        if all((self.bed, other.bed)):
            score += 1/3 * num_sim(self.bed,  other.bed,  .5)

        if all((self.bath, other.bath)):
            score += 1/3 * num_sim(self.bath, other.bath, .5)

        # Compare neighborhoods
        score += 1/3 * cosine_sim(self.neighborhood_set, other.neighborhood_set)

        self.bnbscores[other.id] = score

# Container for airbnb data
class APFJ():
    def __init__(self, data):
        # Save data, editing airbnb keys to zillow counterparts where needed
        for key in airbnb_attrs:
            try:
                airbnb_key = zillow_to_airbnb_keys[key]
            except KeyError:
                airbnb_key = key

            try:
                self[key] = data[airbnb_key]
            except KeyError:
                self[key] = None

        # Speed up comparisons later by pre-calculating neighborhood attributes.
        try:
            self.neighborhood_set = set(self.neighborhood)
        except TypeError: # set(None)
            self.neighborhood_set = set()

        self.num_neighborhoods = len(self.neighborhood_set)

    def __getitem__(self, key):
        return self.__getattribute__(key)

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)


def zillowZillowConnect(driver):
    # Get data from db
    with driver.session() as session:
        query  = 'MATCH (p:Property) '
        query += 'RETURN p.id, p.price, p.street, p.size, p.bed, p.bath, p.neighborhood '
        results = session.run(query)
        zillow_props = [ZPFG(r) for r in results]
    
    # Do all comparisons
    for prop1 in zillow_props:
        for prop2 in zillow_props:
            prop1.zillowCompare(prop2)

    all_ids = [o.id for o in zillow_props] # make one copy

    threshold = .70

    # Link similar properties together in the db.
    #
    # Note that Neo4j does not support creating undirected edges. Therefore we
    # end up creating both incoming and outgoing (directed) relationships between
    # "similar" nodes.
    with driver.session() as session:
        count = 0

        # This could be removed if the CREATE is MERGE below, but I believe that
        # would be slower.
        query = 'MATCH ()-[r:Is_Similar]-() DELETE r;'
        _ = session.run(query)

        for prop in zillow_props:
            for id in all_ids:
                if id in prop.scores and prop.scores[id] > threshold:
                    query = f'''MATCH (p1:Property), (p2:Property) 
                                WHERE p1.id = '{prop.id}' AND p2.id = '{id}' 
                                CREATE (p1)-[:Is_Similar]->(p2), (p2)-[:Is_Similar]->(p1)'''
                    results = session.run(query)
                    count += 2

        print(f'{count} new zillow <--> zillow Is_Similar relationships.')
    return zillow_props

def zillowAirbnbConnect(driver, zillow_props):
    # Get data from file
    with open(airbnb_json) as f:
        bnb_data = json.load(f)

    # Comparing zillow properties with airbnb rentals makes most sense when
    # we filter out rentals that only consist of a portion of a residence.
    # airbnb_props = [obj for r in bnb_data if (obj := APFJ(r)).room_type_category == 'entire_home']
    airbnb_props = []
    for r in bnb_data:
        bnb_obj = APFJ(r)
        if bnb_obj.room_type_category == 'entire_home':
            airbnb_props.append(bnb_obj)

    # Do all comparisons
    for z_prop in zillow_props:
        for a_prop in airbnb_props:
            z_prop.airbnbCompare(a_prop)

    count = 0
    threshold = .5

    with driver.session() as session:
        for z_prop in zillow_props:
            for a_prop in airbnb_props:
                if z_prop.bnbscores[a_prop.id] > threshold:
                    query = f'''MATCH (p:Property), (r:Rental) 
                                WHERE p.id = '{z_prop.id}' AND r.id = {a_prop.id} 
                                CREATE (p)-[:Is_Similar]->(r), (r)-[:Is_Similar]->(p)'''
                    results = session.run(query)
                    count += 2

    total = len(zillow_props) * len(airbnb_props)
    print(f'{count} new zillow <--> airbnb Is_Similar relationships. (total={total})')
    
    return airbnb_props

#if __name__ == '__main__':
if True:
    driver = GraphDatabase.driver(uri, auth=(user, pw))
    zillow_props = zillowZillowConnect(driver)
    airbnb_props = zillowAirbnbConnect(driver, zillow_props)
