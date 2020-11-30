# 203_final

## How to load data into local Neo4J

1. Install Neo4j Desktop**

[Download Neo4j](https://neo4j.com/download/)

Then, launch the Neo4j Browser, create an empty database, set the password to "neo4jbinder", and close the database.

2. Edit NEO4J_HOME and NEO4J_IMPORT ./cyphers/run_cyphers.sh to point to local installation NEO4J installation locations.
```
NEO4J_HOME="/Users/<name>/Library/Application Support/com.Neo4j.Relate/Data/dbmss/dbms-<guid>"
NEO4J_IMPORT="/Users/<name>/Library/Application Support/com.Neo4j.Relate/Data/dbmss/dbms-<guid>/import"
```

3. Run the following command to upload csv into local Neo4J Database
```
sh ./cyphers/run_cyphers.sh
```

## Reference code

1. Clone Google maps services python repo to ./data_sources/google/

[Google maps services python](https://github.com/googlemaps/google-maps-services-python)

2. Clone Yelp API python repo to ./data_sources/yelp/

[Yelp API python](https://github.com/gfairchild/yelpapi)
