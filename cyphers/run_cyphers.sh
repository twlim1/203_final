#!/bin/bash

# Replace with local path
NEO4J_HOME="/Users/<user>/Library/Application Support/com.Neo4j.Relate/Data/dbmss/dbms-<guid>"
NEO4J_IMPORT="/Users/<user>/Library/Application Support/com.Neo4j.Relate/Data/dbmss/dbms-<guid>/import"

DEFAULT_ENDPOINT=bolt://localhost:7687
ENDPOINT=${NEO4J_URI:-$DEFAULT_ENDPOINT}
USERNAME=${NEO4J_USERNAME:-neo4j}
PASSWORD=${NEO4J_PASSWORD:-neo4jbinder}
CYPHER=${NEO4J_BIN:-$NEO4J_HOME/bin}
CYPHERS=./cyphers

echo "Neo4J Home:"$NEO4J_HOME
echo "Endpoint: "$ENDPOINT
echo "Username: "$USERNAME
echo "Password: "$PASSWORD
echo "Cypher: "$CYPHER
echo "Cyphers: "$CYPHERS

export cypher_shell="$CYPHER/cypher-shell"

function run_cypher {
    echo " "
    echo "----------------------------------------------"
    echo "Running $1:"
    echo " "
    cat "$CYPHERS/$1"
    cat "$CYPHERS/$1" | "$cypher_shell" -a "$ENDPOINT" -u "$USERNAME" -p "$PASSWORD"
}

# Copy from data folder
cp data/yelp.csv "$NEO4J_IMPORT/yelp.csv"

# run cypher scripts to import to Neo4J
run_cypher 0_init.cyphers
run_cypher 1_yelp.cyphers
