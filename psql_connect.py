import psycopg2
try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "test123",
                                  host = "192.168.56.1",
                                  port = "5432",
                                  database = "test_db")

    cursor = connection.cursor()
    #print(connection.get_dsn_parameters(),"\n")

    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")

except (Exception, psycopg2.Error):
 print("Error while connecting to PostgreSQL")
finally:
    #closing database connection.
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
