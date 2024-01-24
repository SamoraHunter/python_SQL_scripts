import os
import pytds
import pandas as pd

# Connection parameters
server_name = '%SERVER_NAME%'
database_name = ''
user_name = '%USERNAME%'
password = '%PASSWORD%'

# Establish a connection
conn = pytds.connect(server=server_name, database=database_name, user=user_name, password=password)

# Create a cursor
cursor = conn.cursor()

# Execute a query to get all tables with their schema names
cursor.execute("""
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE'
""")

# Fetch all the tables with schema names
tables = cursor.fetchall()

# Create a folder for exporting tables
output_folder = "./tables_export"
os.makedirs(output_folder, exist_ok=True)

# Loop through each table and export to CSV
for schema, table_name in tables:
    try:
        # Check if the CSV file already exists
        csv_filename = os.path.join(output_folder, f"{schema}_{table_name}.csv")
        if os.path.exists(csv_filename):
            print(f"Skipped exporting '{table_name}' as '{csv_filename}' already exists.")
            continue

        # Execute a query to fetch all rows from the current table with the correct schema
        cursor.execute(f"SELECT * FROM {schema}.{table_name}")
        
        # Fetch all rows
        rows = cursor.fetchall()
        
        # Convert rows to a DataFrame
        df = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])
        
        # Export DataFrame to CSV
        df.to_csv(csv_filename, index=False)
        
        print(f"Table '{schema}.{table_name}' exported to '{csv_filename}'")
    
    except Exception as e:
        print(f"Error exporting table '{schema}.{table_name}': {e}")

# Close the cursor and connection
cursor.close()
conn.close()

