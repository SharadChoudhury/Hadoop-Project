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
    table_name = 'taxi_data'
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
                for j in range(1,11):
                    b.put(temp[0], {'trip_info:'+ cols[j] :temp[j] })
                for k in range(11,20):
                    b.put(temp[0], {'fare_info:'+ cols[k] :temp[k] })
            else:
                cols = line.strip().split(",")
            i+=1

    file.close()
    print("batch insert done, {} lines inserted".format(i))
    close_connection()


# insert batch data from march.csv and april.csv
batch_insert_data('march.csv')
batch_insert_data('april.csv')