import googlemaps
import os
import time


class NeighborhoodLookup(googlemaps.Client):
    def __init__(self, api_key=None):
        if api_key is None:
            api_key = os.getenv('GOOGLE_API_KEY')

        super().__init__(key=api_key)

    def neighborhood_lookup(self, coordinate, ids, delay=0.1):
        """
        Get reverse geocode results for a given location and extract data from results.

        Parameters:
            coordinate (dict, tuple): Coordinate to search
            ids (list): IDs of places associated with this location
            delay (int, float): Amount of time to sleep between API calls

        Returns:
            results (dict): Reverse geocoding results
        """

        if type(coordinate) is tuple:
            coordinate = {'latitude': coordinate[0], 'longitude': coordinate[1]}

        # query Google Geocoding API
        result = self.reverse_geocode(coordinate)
        time.sleep(delay)

        # extract neighborhoods from result
        neighborhoods = list(
            {
                x['long_name']
                for r in result
                for x in r['address_components']
                if 'neighborhood' in x['types']
            }
        )

        # extract cities from result
        cities = list(
            {
                x['long_name']
                for r in result
                for x in r['address_components']
                if 'locality' in x['types']
            }
        )

        if len(neighborhoods) > 0:
            return {
                'coordinate': coordinate,
                'id': ids,
                'neighborhood': neighborhoods,
                'city': cities,
                'result': result,
            }

        return {'coordinate': coordinate, 'id': ids, 'city': cities, 'result': result}
