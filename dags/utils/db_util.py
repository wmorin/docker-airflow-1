import psycopg2


def connect(db_config):
    try:
        print(f"connecting to {db_config['dbname']}")
        connect_str = "dbname={} user={} host={} port={} password={}".format(
            db_config['dbname'],
            db_config['user'],
            db_config['host'],
            db_config['port'],
            db_config['password'])
        # use our connection values to establish a connection
        conn = psycopg2.connect(connect_str)
        # create a psycopg2 cursor that can execute queries
        return conn
    except Exception as e:
        print("Uh oh, can't connect. Invalid dbname, user or password?")
        print(e)



def run_query(conn, query, params=None):
    cursor = conn.cursor()
    cursor.execute(query, params)
    return cursor
