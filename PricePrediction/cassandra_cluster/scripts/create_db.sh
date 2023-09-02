
KEYSPACE="app_data"
TABLE1="close_prices"
TABLE2="moving_avg"
CREATE_KEYSPACE=true
CREATE_TABLES=true
DROP_KEYSPACE=false
DROP_TABLES=false
TRUNCATE_TABLES=false
FROM_FILE=true
KEYSPACE_CQL_PATH="/var/lib/cassandra/scripts/app_data.cql"
TABLE_1_CQL_PATH="/var/lib/cassandra/scripts/moving_avg.cql"
TABLE_2_CQL_PATH="/var/lib/cassandra/scripts/closed_prices.cql"


create_keyspace() {
    if [[ $FROM_FILE == true ]]; then
        cqlsh -f "$KEYSPACE_CQL_PATH"
    else
        cqlsh -e "CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"
    fi
}

create_tables() {
    if [[ $FROM_FILE == true ]]; then
        cqlsh -k $KEYSPACE -f "$TABLE_1_CQL_PATH"
        cqlsh -k $KEYSPACE -f "$TABLE_2_CQL_PATH"
    else
        cqlsh -k $KEYSPACE -e "CREATE TABLE IF NOT EXISTS $TABLE1 (
                symbol TEXT,
                timestamp TIMESTAMP,
                close_price DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            );"
        cqlsh -k $KEYSPACE -e "CREATE TABLE IF NOT EXISTS $TABLE2 (
                symbol TEXT,
                timestamp TIMESTAMP,
                moving_avg DOUBLE,
                PRIMARY KEY (symbol, timestamp)
            );
            "
    fi
}

drop_keyspace() {
  cqlsh -e "DROP KEYSPACE IF EXISTS $KEYSPACE;"
}

drop_tables() {
  cqlsh -k $KEYSPACE -e "DROP IF EXISTS $TABLE1;"
  cqlsh -k $KEYSPACE -e "DROP IF EXISTS $TABLE2;"
}

truncate_tables() {
  cqlsh -k $KEYSPACE -e "TRUNCATE $TABLE1;"
  cqlsh -k $KEYSPACE -e "TRUNCATE $TABLE2;"
}


# if [[ $1 == "create" ]]; then
#   create_schema
#   echo "Keyspace $KEYSPACE and tables $TABLE1, $TABLE2 created."
# elif [[ $1 == "drop" ]]; then
#   drop_keyspace
#   echo "Keyspace $KEYSPACE dropped."
# elif [[ $1 == "truncate" ]]; then
#   truncate_tables
#   echo "Tables $TABLE1 and $TABLE2 truncated."
# else
#   echo "Usage: $0 [create|drop|truncate]"
# fi

if [[ $CREATE_KEYSPACE == true ]]; then
  create_keyspace
  echo "Keyspace $KEYSPACE created."
fi

if [[ $CREATE_TABLES == true ]]; then
  create_tables
  echo "Keyspace $KEYSPACE's tables $TABLE1, $TABLE2 created."
fi

if [[ $DROP_KEYSPACE == true ]]; then
  drop_keyspace
  echo "Keyspace $KEYSPACE dropped."
fi

if [[ $DROP_TABLES == true ]]; then
  drop_tables
  echo "Keyspace $KEYSPACE's Tables $TABLE1 and $TABLE2 dropped."
fi

if [[ $TRUNCATE_TABLES == true ]]; then
  truncate_tables
  echo "Keyspace $KEYSPACE's Tables $TABLE1 and $TABLE2 truncated."
fi