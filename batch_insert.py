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
    # print(connection.tables())
    table_name = 'taxidata'
    table = connection.table(table_name)
    return table


def getlastrow(table):
    # Start a scanner with descending order to get the last row key
    scanner = table.scan(reverse=True, limit=1)
    # Get the last row key (if any)
    last_row_key = next(iter(scanner), [0])
    # Close the scanner
    scanner.close()
    # Extract the row key from the result, if available
    last_row_key = last_row_key[0]
    return int(last_row_key)

    
#batch insert data in events table 
def batch_insert_data(filename):
    open_connection()

    file = open(filename, "r")
    table = get_table()
    lastrow = getlastrow(table)
    i = lastrow 
    cols = []

    print("starting batch insert of events")
    with table.batch(batch_size=1000) as b:
        for line in file:
            if i != lastrow :
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


if __name__ == '__main__':
    batch_insert_data('mar.csv')
    batch_insert_data('apr.csv')
