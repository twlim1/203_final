#!/usr/bin/env python
import os
import sys

from pprint import pprint
from string import digits

from neo4j import GraphDatabase


uri = 'bolt://localhost:7687'
user = 'neo4j'
pw = os.environ['NEO4JPW']


# attributes that need to be converted from strings
key_to_type = {
    'id':    int,
    'price': int,
    'size':  int,
    'bed':   float,
    'bath':  float, # zillow doesn't seem to use 'half' bathrooms but we allow it.
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
# Note that this is designed so that get_sim(a,b,c) == get_sim(b,a,c)
#
# Take the example where:
# ratio = .3
# base_val = 100,000
#
# Then if "comp_val" has a price within 70,000 and 130,000, a value > 0 will be
# returned. 100,000 would be a perfect match and would return 1.
#
def get_sim(base_val, comp_val, ratio):
    diff = min(abs(base_val - comp_val) / ((base_val+comp_val)/2), ratio)
    return (ratio - diff) / ratio
    

# zillow property from graph
class ZPFG:
    def __init__(self, info):
        self.scores = {}
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

    def __str__(self):
        return f'{dict(((k, self[k]) for k in self.print_keys))}'

    def __getitem__(self, key):
        return self.__getattribute__(key)

    def __setitem__(self, key, value):
        return self.__setattr__(key, value)

    # This is called in an NxN fashion but only ~half of the of calls will do
    # anything since comparisons are symmetric.
    def compareScore(self, other):
        # Check if compareScore(other, self) was already called
        if self.id == other.id or self.id in other.scores:
            return

        score = 0

        # Price is most important factor; it sums up other attributes.
        if all((self.price, other.price)):
            score += .5 * get_sim(self.price, other.price, .3)

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
            score += .1 * get_sim(self.bed,  other.bed,  1)
        if all((self.bath, other.bath)):
            score += .1 * get_sim(self.bath, other.bath, .5)

        # Compare square footage
        if all((self.size, other.size)):
            score += .1 * get_sim(self.size, other.size, .5)

        # Enforce a score ceiling of 1.0
        score = min(1, score)

        self.scores[other.id] = score


if __name__ == '__main__':
    driver = GraphDatabase.driver(uri, auth=(user, pw))

    # Get data from db
    with driver.session() as session:
        query  = 'MATCH (p:Property) '
        query += 'RETURN p.id, p.price, p.street, p.size, p.bed, p.bath '
        results = session.run(query)
        property_objects = [ZPFG(r) for r in results]
    
    # Do all comparisons
    for prop1 in property_objects:
        for prop2 in property_objects:
            prop1.compareScore(prop2)

    all_ids = [o.id for o in property_objects] # make one copy

    sim_threshold = .55

    # Link similar properties together in the db.
    #
    # Note that Neo4j does not support creating undirected edges. Therefore we
    # end up creating both incoming and outgoing (directed) relationships between
    # "similar" nodes.
    with driver.session() as session:
        count = 0

        # This could be removed if the CREATE is MERGE below, but I believe that
        # would be slower.
        query = 'MATCH (:Property)-[r:SIMILAR]-(:Property) DELETE r;'
        _ = session.run(query)

        for prop in property_objects:
            for id in all_ids:
                if id in prop.scores and prop.scores[id] > sim_threshold:
                    query = f'''MATCH (p1:Property), (p2:Property) 
                                WHERE p1.id = '{prop.id}' AND p2.id = '{id}' 
                                CREATE (p1)-[:SIMILAR]->(p2), (p2)-[:SIMILAR]->(p1)'''
                    results = session.run(query)
                    count += 2

        print(f'Created {count} new relationships.')
