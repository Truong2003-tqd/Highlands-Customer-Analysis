# Import PostgreSQL adapter and libraries
import psycopg2
import pandas as pd

# Write connection parameters
hostname = 'localhost'
database = 'highlands'
username = 'postgres'
pwd = 'trust'
port_id = 5432 

# Initialize connection and cursor
# Assign to None to avoid UnboundLocalError in finally block
conn = None
cur = None

# Connect to the PostgreSQL server
# Use try-except-finally to ensure proper cleanup
try:
    # Create connection with PostgreSQL database
    conn = psycopg2.connect( 
        host=hostname,
        dbname=database,
        user=username,
        password=pwd,
        port=port_id
    )

    # Create a cursor object
    cur = conn.cursor()

    # Execute a query

    # Select first 5 rows from brand_image table
    cur.execute('SELECT * FROM public.brand_image LIMIT 5;')

    # Fetch all rows from the executed query
    rows = cur.fetchall()
    
    # Get column names from cursor description
    # Cursor description provides metadata about the result set (e.g., column names)
    colnames = [desc[0] for desc in cur.description]

    # Create a DataFrame from the fetched rows and column names
    df = pd.DataFrame(rows, columns=colnames)

    # Print the first 5 rows of the DataFrame
    print(df)

    # Save the dataframe to a CSV file
    df.to_csv('brand_image_sample.csv', index=False)

    # Commit any changes
    conn.commit()

# Return error if any occurs
except Exception as error:
    print(error)

# Close cursor and connection to free resources
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()