import happybase

#create connection
connection = happybase.Connection('localhost', port=9090 ,autoconnect=False)

#open connection to perform operations
def open_connection():
    connection.open()


#close the opened connection
def close_connection():
    connection.close()


#get the pointer to a table
def get_table():
    open_connection()
    print(connection.tables())
    table_name = 'taxidata'
    table = connection.table(table_name)
    close_connection()
    return table


#batch insert data in events table 
def batch_insert_data(filename):
    print("starting batch insert of events")
    file = open(filename, "r")
    table = get_table()
    open_connection()

    i= 0
    cols = []

    with table.batch(batch_size=1000) as b:
        for line in file:
            if i!=0:
                temp = line.strip().split(",")
                row_key = str(i).encode()  # Encode row key to bytes
                for j in range(10):
                    b.put(row_key, {b'trip_info:' + cols[j].encode(): temp[j].encode()})
                for k in range(10, 19):
                    b.put(row_key, {b'fare_info:' + cols[k].encode(): temp[k].encode()})
                print('inserted {}th line'.format(i))
            else:
                cols = line.strip().split(",")
            i+=1
    

    file.close()
    print("batch insert done, {} lines inserted".format(i-1))
    close_connection()


# insert batch data from march.csv and april.csv
batch_insert_data('mar.csv')
batch_insert_data('apr.csv')