#!/usr/bin/env python
import ast
import datetime
import os
import time
from abc import ABC, abstractmethod
from itertools import combinations, product
from string import digits

from neo4j import GraphDatabase
from py_stringmatching import Cosine
from tqdm import tqdm

uri = 'bolt://localhost:7687'
user = 'neo4j'
pw = os.environ['NEO4JPW']

cosine_sim = Cosine().get_sim_score

# attributes that need to be converted from strings
key_to_type = {
    'id':    int,
    'price': int,
    'size':  int,
    'bed':   float,
    'bath':  float,  # zillow doesn't seem to use 'half' bathrooms but we allow it.
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
    diff = min(abs(base_val - comp_val) / ((base_val + comp_val) / 2), ratio)
    return (ratio - diff) / ratio


class PropertyContainer(ABC):
    def __init__(self, info):
        self.print_keys = []

        for k, v in info.items():
            # Convert neo4j string to something we want to use ex: 'p.price' -> 'price'
            actual_key = k.split('.')[1] if '.' in k else k
            self.print_keys.append(actual_key)

            if actual_key in key_to_type:
                if v is None:
                    self[actual_key] = None
                else:
                    self[actual_key] = key_to_type[actual_key](v)
            elif actual_key == 'neighborhood' and isinstance(v, str):
                # ex: "['San Diego']"
                self[actual_key] = ast.literal_eval(v)
            else:
                self[actual_key] = v

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

    @abstractmethod
    def compare(self, other):
        pass


# zillow property from graph
class ZPFG(PropertyContainer):
    data_source = 'zillow'
    node_name = 'Property'

    def __init__(self, info):
        super().__init__(info)

        # Remove digits from street
        self.stripped_street = self.street.translate({ord(d): None for d in digits})

    def compare(self, other):
        if isinstance(other, ZPFG):
            return self.zillowCompare(other)
        else:
            return self.airbnbCompare(other)

    # This is called in an NxN fashion but only ~half of the of calls will do
    # anything since comparisons are symmetric.
    def zillowCompare(self, other):
        score = 0

        # Price is most important factor; it sums up other attributes.
        if all((self.price, other.price)):
            score += 0.5 * num_sim(self.price, other.price, 0.3)

        # Comparing addresses is the most costly comparison and is therefore
        # currently designed for simplicity and speed.
        #
        # Possible modifications could be done here given more time:
        # - Since street names aren't unique, we could validate using gps coordinates
        # - Use an edit distance metric to compare street names in a more robust fashion
        # - Make the comparison aware of abbreviations such as 'street' -> 'st' or
        #   'avenue' -> 'ave'.
        if self.stripped_street == other.stripped_street:
            score += 0.2

        # Compare beds and baths
        if all((self.bed, other.bed)):
            score += 0.1 * num_sim(self.bed, other.bed, 0.5)

        if all((self.bath, other.bath)):
            score += 0.1 * num_sim(self.bath, other.bath, 0.5)

        # Compare square footage
        if all((self.size, other.size)):
            score += 0.1 * num_sim(self.size, other.size, 0.3)

        return score

    def airbnbCompare(self, other):
        score = 0

        # Compare beds and baths
        if all((self.bed, other.bed)):
            score += 1 / 3 * num_sim(self.bed, other.bed, 0.5)

        if all((self.bath, other.bath)):
            score += 1 / 3 * num_sim(self.bath, other.bath, 0.5)

        # Compare neighborhoods
        score += 1 / 3 * cosine_sim(self.neighborhood_set, other.neighborhood_set)

        return score


# Container for airbnb data
class APFG(PropertyContainer):
    data_source = 'airbnb'
    node_name = 'Rental'

    def compare(self, other):
        if isinstance(other, APFG):
            return self.airbnbCompare(other)
        else:
            return other.airbnbCompare(self)

    def airbnbCompare(self, other):
        score = 0

        # Compare beds and baths
        if all((self.bed, other.bed)):
            score += 0.25 * num_sim(self.bed, other.bed, 0.5)

        if all((self.bath, other.bath)):
            score += 0.25 * num_sim(self.bath, other.bath, 0.5)

        # Compare neighborhoods or city
        if len(self.neighborhood_set) > 0 and len(other.neighborhood_set) > 0:
            score += 0.3 * cosine_sim(self.neighborhood_set, other.neighborhood_set)
        elif self.city == other.city:
            score += 0.3

        # Compare property type
        if self.type_id == other.type_id:
            score += 0.1

        # Compare amenity IDs
        score += 0.05 * cosine_sim(self.amenity_ids, other.amenity_ids)

        # Compare amenity names (subset of amenity IDs)
        score += 0.05 * cosine_sim(self.amenity_names, other.amenity_names)

        return score


# Link similar properties together in the db.
#
# Note that Neo4j does not support creating undirected edges. Therefore we
# end up creating both incoming and outgoing (directed) relationships between
# "similar" nodes.
def connect_nodes(driver, pairs, threshold):
    pairs = list(pairs)
    relation = f'{pairs[0][0].data_source} <--> {pairs[0][1].data_source} relationships'
    count = 0

    print(f'Creating {relation}')

    with driver.session() as session:
        for p1, p2 in tqdm(pairs):
            score = p1.compare(p2)  # compare properties

            # create relationship
            if score >= threshold:
                is_similar = f'[:Is_Similar {{score: {score}}}]'
                query = f'''MATCH (n1:{p1.node_name}), (n2:{p2.node_name})
                            WHERE n1.id = '{p1.id}' AND n2.id = {p2.id}
                            CREATE (n1)-{is_similar}->(n2), (n2)-{is_similar}->(n1)'''
                _ = session.run(query)
                count += 2

    total = len(pairs)
    pct = count / total * 100
    print(f'{count} new {relation} (total={total}, {pct:.2f}%)')


def zillowZillowConnect(driver):
    # Get data from db
    print('Fetching zillow data')

    with driver.session() as session:
        query = '''
            MATCH (p:Property)
            RETURN p.id, p.price, p.street, p.size, p.bed, p.bath, p.neighborhood
        '''
        results = session.run(query)
        zillow_props = [ZPFG(r) for r in results]

    # Do all comparisons and add relationships
    threshold = 0.7
    connect_nodes(driver, combinations(zillow_props, 2), threshold)

    return zillow_props


def zillowAirbnbConnect(driver, zillow_props):
    # Get data from db
    print('Fetching airbnb data')

    with driver.session() as session:
        query = '''
            MATCH (r:Rental)-[:Located_In]->(c:City)
            OPTIONAL MATCH (r)-[:Located_In]->(n:Neighborhood)
            RETURN r.id, r.bed, r.bath, r.type_id, r.amenity_ids, r.amenity_names,
                    c.name AS city,
                    collect(n.name) AS neighborhood
        '''
        results = session.run(query)
        airbnb_props = [APFG(r) for r in results]

    # Do all airbnb comparisons and add relationships
    threshold = 0.98
    connect_nodes(driver, combinations(airbnb_props, 2), threshold)

    # Do all zillow comparisons and add relationships
    threshold = 0.95
    connect_nodes(driver, product(zillow_props, airbnb_props), threshold)

    return airbnb_props


if __name__ == '__main__':
    start = time.time()
    driver = GraphDatabase.driver(uri, auth=(user, pw))

    # This could be removed if MERGE is used instead of CREATE, but I believe that
    # would be slower.
    with driver.session() as session:
        query = 'MATCH ()-[r:Is_Similar]-() DELETE r;'
        _ = session.run(query)

    zillow_props = zillowZillowConnect(driver)
    airbnb_props = zillowAirbnbConnect(driver, zillow_props)

    print(f'\nElapsed time: {datetime.timedelta(seconds=time.time()-start)}')
