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

def create_tb_trip():
    #dropa e recria tabela principal para input de dados
    conn = get_conection()

    cursor = conn.cursor()
    
    sql = """
        DROP TABLE IF EXISTS TB_TRIP;
        
        CREATE TABLE TB_TRIP (
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

def create_tb_vendor():
    #dropa e recria tabela principal para input de dados
    conn = get_conection()

    cursor = conn.cursor()
    
    sql = """
        DROP TABLE IF EXISTS TB_VENDOR;
        
        CREATE TABLE TB_VENDOR (
        VENDOR_ID VARCHAR(10) NULL,
        NAME VARCHAR(100) NULL,
        ADDRESS VARCHAR(200) NULL,
        CITY VARCHAR(30) NULL,
        STATE VARCHAR(2) NULL,
        ZIP INT NULL,
        COUNTRY VARCHAR(3) NULL,
        CONTACT VARCHAR(100) NULL,
        CURRENT VARCHAR(3) NULL
    );
    """
    print(sql)
    cursor.execute(sql)

    print(f'commit')
    conn.commit()  

    print(f'close')
    conn.close()


def create_tb_payment():
    #dropa e recria tabela principal para input de dados
    conn = get_conection()

    cursor = conn.cursor()
    
    sql = """
        DROP TABLE IF EXISTS TB_PAYMENT;
        
        CREATE TABLE TB_PAYMENT (
            PAYMENT_TYPE VARCHAR(15),
            PAYMENT_LOOKUP VARCHAR(50)
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