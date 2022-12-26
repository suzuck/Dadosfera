import psycopg2
import psycopg2.extras as extras


def get_conection():
    conn = psycopg2.connect(
        dbname = 'db1', 
        host = 'database-2.chaeu1yz7n3h.us-east-1.rds.amazonaws.com', 
        port = '5432', 
        user = 'postgres', 
        password = '123central' 
    )  
    return conn

def create_tables():
    #dropa e recria tabela principal para input de dados
    conn = get_conection()

    cursor = conn.cursor()
    
    sql = """
        DROP TABLE IF EXISTS TX_TRIP;
        
        CREATE TABLE TX_TRIP (
        VENDOR_ID VARCHAR(6),
        PICKUP_DATETIME TIMESTAMP,
        DROPOFF_DATETIME TIMESTAMP,
        PASSENGER_COUNT INT,
        TRIP_DISTANCE FLOAT,
        PICKUP_LONGITUDE FLOAT,
        PICKUP_LATITUDE FLOAT ,
        RATE_CODE VARCHAR(500),
        STORE_AND_FWD_FLAG VARCHAR(500),
        DROPOFF_LONGITUDE FLOAT,
        DROPOFF_LATITUDE FLOAT,
        PAYMENT_TYPE VARCHAR(12),
        FARE_AMOUNT FLOAT,
        SURCHARGE INT,
        TIP_AMOUNT FLOAT,
        TOLLS_AMOUNT FLOAT ,
        TOTAL_AMOUNT FLOAT
        );
    """
    print(sql)
    cursor.execute(sql)

    print(f'commit')
    conn.commit()  

    print(f'close')
    conn.close()

def insert(tabela,frame):
    conn = get_conection()

    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in frame.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(frame.columns))
    # SQL quert to execute
    query  = 'INSERT INTO %s (%s) VALUES %%s'%(tabela,cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()