import airbnb
import math
import os
import pandas as pd
import sys
import time


class Airbnb(airbnb.Api):
    """
    Extends the Api class to provide a few convenience methods.
    """

    def get_listings(self, query, limit=50, pages=10, delay=1):
        """
        Get results from a given query. Calls get_homes in a loop to get all listings.
        Maximum results per query appears to be 306.

        Parameters:
            query (str): Query to search
            limit (int): Number of listings to return per API call
            pages (int): Number of "pages" to search, controls offset in API
            delay (int, float): Amount of time to sleep between API calls

        Returns:
            listings (pandas.DataFrame, None): DataFrame of unique listings or None
        """

        if not isinstance(limit, int) or limit < 1:
            raise ValueError(f'Items per page ({limit}) must be a positive integer.')

        if not isinstance(pages, int) or pages < 1:
            raise ValueError(f'Number of pages ({pages}) must be a positive integer.')

        listings = None
        last_page = False

        for i in range(pages):
            try:
                # get listings on current page
                result = self.get_homes(query, items_per_grid=limit, offset=i*limit)
                time.sleep(delay)
            except Exception:
                print(f'Error encountered for {query} on page {i+1}')
                break

            # handle case when API returns results, but no listings
            if 'listings' not in result['explore_tabs'][0]['sections'][0]:
                print(f'No results for {query} on page {i+1}')
                break

            # convert current listings to DataFrame and append to all listings
            current_listings = result['explore_tabs'][0]['sections'][0]['listings']
            df_list = pd.DataFrame([x['listing'] for x in current_listings])
            df_price = pd.DataFrame([x['pricing_quote'] for x in current_listings])
            df = df_list.merge(df_price, left_index=True, right_index=True)
            listings = listings.append(df) if listings is not None else df

            # check if there are additional pages
            # looping once more after has_next_page is false returns a few more results
            if not result['explore_tabs'][0]['pagination_metadata']['has_next_page']:
                if last_page:
                    print(f'Finished searching {query}')
                    break
                else:
                    last_page = True

        # drop duplicate listings just in case
        if listings is not None:
            listings = listings.drop_duplicates(subset='id')

        return listings

    def get_neighborhood_listings(
        self, neighborhoods, city, limit=50, pages=10, delay=1
    ):
        """
        Get listings for multiple neighborhoods in a given city.

        Parameters:
            neighborhoods (iterable): Collection of neighborhood strings
            city (str): City to search
            limit (int): Number of listings to return per API call
            pages (int): Number of "pages" to search, controls offset in API
            delay (int, float): Amount of time to sleep between API calls

        Returns:
            listings (pandas.DataFrame, None): DataFrame of unique listings or None
        """

        listings = None

        for n in neighborhoods:
            # get listings for current neighborhood and append to all listings
            df = self.get_listings(
                f'{n}, {city}', limit=limit, pages=pages, delay=delay
            )
            listings = listings.append(df) if listings is not None else df
            time.sleep(delay)

        # drop duplicate listings just in case
        if listings is not None:
            listings = listings.drop_duplicates(subset='id')

        return listings

    def get_all_reviews(self, listing_ids, limit=100, delay=1):
        """
        Get all reviews for multiple listings.
        The get_reviews method only returns a batch of reviews for a single listing.

        Parameters:
            listing_ids (iterable): Collection of listing IDs
            limit (int): Number of reviews to return per API call
            delay (int, float): Amount of time to sleep between API calls

        Returns:
            reviews (pandas.DataFrame, None): DataFrame of unique reviews or None
        """

        # block printing to stdout (get_reviews method prints unnecessary messages)
        sys.stdout = open(os.devnull, 'w')

        reviews = None

        for listing_id in listing_ids:
            # get reviews for current listing
            result = self.get_reviews(listing_id, limit=limit)
            count = result['metadata']['reviews_count']
            time.sleep(delay)

            # check if there are reviews and append to all reviews
            if count > 0:
                df = pd.DataFrame(result['reviews'])
                reviews = reviews.append(df) if reviews is not None else df

            # loop over remaining pages to get all results
            for page in range(1, math.ceil(count / limit)):
                # get reviews on current page and append to all reviews
                result = self.get_reviews(listing_id, limit=limit, offset=page*limit)
                df = pd.DataFrame(result['reviews'])
                reviews = reviews.append(df)
                time.sleep(delay)

        # enable printing to stdout
        sys.stdout = sys.__stdout__

        # drop duplicate reviews just in case
        if reviews is not None:
            reviews = reviews.drop_duplicates(subset='id')

        return reviews
