from sqlalchemy import MetaData, create_engine, Table, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timezone
from field_ranges1 import (
    daroot_field_ranges, dapermit_field_ranges, dadafield_field_ranges,
    dafldspc_field_ranges, dafldbhl_field_ranges, dacanres_field_ranges, dacanfld_field_ranges,
    dafreres_field_ranges, dafrefld_field_ranges, dapmtbhl_field_ranges,
    daaltadd_field_ranges, daremark_field_ranges, dacheck_field_ranges
)

DB_URI = 'postgresql+psycopg2://airflow:airflow@192.168.29.226:5432/airflow'

# Total record count and batch
total_record_count = 0
api_number = None
batch_records = []

# Sequence dictionaries to store current sequence for each record type
record_sequences = {
    "01": 1,
    "02": 1,
    "03": 1,
    "04": 1,
    "05": 1,
    "06": 1,
    "07": 1,
    "08": 1,
    "09": 1,
    "10": 1,
    "11": 1,
    "12": 1,
    "13": 1,
}

# Field ranges dictionary
field_ranges_dict = {
    "01": daroot_field_ranges,
    "02": dapermit_field_ranges,
    "03": dadafield_field_ranges,
    "04": dafldspc_field_ranges,
    "05": dafldbhl_field_ranges,
    "06": dacanres_field_ranges,
    "07": dacanfld_field_ranges,
    "08": dafreres_field_ranges,
    "09": dafrefld_field_ranges,
    "10": dapmtbhl_field_ranges,
    "11": daaltadd_field_ranges,
    "12": daremark_field_ranges,
    "13": dacheck_field_ranges
}

# MetaData initialization and schema association
metadata = MetaData()

table01=Table('daroot_table_staging', metadata, autoload_with=create_engine(DB_URI))
table02=Table('dapermit_table_staging', metadata, autoload_with=create_engine(DB_URI))
table03=Table('dadafield_table_staging', metadata, autoload_with=create_engine(DB_URI))
table04=Table('dafldspc_table_staging', metadata, autoload_with=create_engine(DB_URI))
table05=Table('dafldbhl_table_staging', metadata, autoload_with=create_engine(DB_URI))
table06=Table('dacanres_table_staging', metadata, autoload_with=create_engine(DB_URI))
table07=Table('dacanfld_table_staging', metadata, autoload_with=create_engine(DB_URI))
table08=Table('dafreres_table_staging', metadata, autoload_with=create_engine(DB_URI))
table09=Table('dafrefld_table_staging', metadata, autoload_with=create_engine(DB_URI))
table10=Table('dapmtbhl_table_staging', metadata, autoload_with=create_engine(DB_URI))
table11=Table('daaltadd_table_staging', metadata, autoload_with=create_engine(DB_URI))
table12=Table('daremark_table_staging', metadata, autoload_with=create_engine(DB_URI))
table13=Table('dacheck_table_staging', metadata, autoload_with=create_engine(DB_URI))


# Tables mapping (define once at the beginning)
tables = {
    "01":table01,
    "02":table02,
    "03":table03,
    "04":table04,
    "05":table05,
    "06":table06,
    "07":table07,
    "08":table08,
    "09":table09,
    "10":table10,
    "11":table11,
    "12":table12,
    "13":table13
}

class TreeNode:
    def __init__(self, parent=None):
        self.data = None
        self.parent = parent
        self.children = []
        if parent:
            parent.children.append(self)

    def get_parent_seq_id(self):
        return self.parent.data if self.parent else None
    
    def set_data(self, data):
        self.data = data

    def get_data(self):
        return self.data

    def clear_data(self):
        if self.data is not None:
            self.data = None

# Define TreeNode structure for records
node_01 = TreeNode(None)
node_02 = TreeNode(node_01)
node_12 = TreeNode(node_01)
node_13 = TreeNode(node_01)
node_03 = TreeNode(node_02)
node_06 = TreeNode(node_02)
node_08 = TreeNode(node_02)
node_10 = TreeNode(node_02)
node_11 = TreeNode(node_02)
node_04 = TreeNode(node_03)
node_05 = TreeNode(node_03)
node_07 = TreeNode(node_06)
node_09 = TreeNode(node_08)

record_nodes = {
    "01": node_01,
    "02": node_02,
    "12": node_12,
    "13": node_13,
    "03": node_03,
    "06": node_06,
    "08": node_08,
    "10": node_10,
    "11": node_11,
    "04": node_04,
    "05": node_05,
    "07": node_07,
    "09": node_09
}

def process_record(line, record_id):
    global total_record_count, batch_records, record_sequences
    dbgStr = ""
    try:
        # Get the field ranges for the current record ID
        field_ranges = field_ranges_dict.get(record_id)
        if not field_ranges:
            raise ValueError(f"Unknown field ranges for record ID: {record_id}")

        if not isinstance(line, str):
            line = str(line)

        values = []
        columns = []
        
        # Extract field values based on field ranges
        for field_name, (start, end) in field_ranges.items():
            if end <= len(line):
                value = line[start:end].strip()  # Extract field value
                if value:
                    value = value.replace("'", "''")  # Escape single quotes for SQL
                    values.append(f"'{value}'")
                    columns.append(field_name)

        # Add the API number if applicable
        if record_id not in ['02', '01', '12', '13'] and api_number:
            values.append(f"'{api_number}'")
            columns.append("da_api_number")

        # Manually generate the sequence (no auto-increment)
        sequence = str(record_sequences[record_id]).zfill(8)
        values.append(f"'{sequence}'")
        columns.append('da_record_sequence')

        # Increment the sequence for the next record of the same type
        record_sequences[record_id] += 1

        # Directly fetch the table from the mapping
        table = tables[record_id]

        # Construct the SQL INSERT statement
        values_str = ', '.join(values)
        columns_str = ', '.join(columns)
        insert_query = f"INSERT INTO {table.name} ({columns_str}) VALUES ({values_str})"

        # Add the query to the batch
        batch_records.append(insert_query)

        # Increment the total record count
        total_record_count += 1

        # Trigger batch upload every 500 records
        if total_record_count % 500 == 0:
            print(f"Processed {total_record_count} records. Uploading batch...")
            upload_batch(batch_records)  # Upload the batch
            batch_records.clear()  # Clear the batch after upload

            current_time = datetime.now()
            print("Batch uploaded at:", current_time)

    except Exception as e:
        print("Error processing record:", record_id, e, dbgStr)


def upload_batch(batch_records):
    try:
        # Only upload if there are records in the batch
        if batch_records:
            engine = create_engine(DB_URI)
            with engine.connect() as connection:
                with connection.begin():
                    for query in batch_records:
                        try:
                            connection.execute(text(query))  # Execute each insert query
                        except Exception as query_error:
                            print(f"Error executing query: {query_error}")
                            raise

            print(f"Batch of {len(batch_records)} records uploaded successfully.")

    except Exception as e:
        print("Error uploading batch:", e)

def main():
    # Open and process the file
    file_path = './daf802.txt'
    
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()

        for line in lines:
            line = line.strip()  # Remove leading/trailing whitespace

            # Determine record ID based on the first two characters of the line
            record_id = line[:2]
            process_record(line, record_id)

        # Upload any remaining records if batch is not empty
        if batch_records:
            upload_batch(batch_records)
            print("Final batch uploaded.")
    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

if __name__ == "__main__":
    main()