import sys
from sqlalchemy import create_engine, MetaData, Table, text, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from field_ranges1 import (
    daroot_field_ranges, dapermit_field_ranges, dadafield_field_ranges,
    dafldspc_field_ranges, dafldbhl_field_ranges, dacanres_field_ranges, dacanfld_field_ranges,
    dafreres_field_ranges, dafrefld_field_ranges, dapmtbhl_field_ranges, 
    daaltadd_field_ranges, daremark_field_ranges, dacheck_field_ranges
)
from datetime import datetime

# Database connection details
DATABASE_URL = "postgresql+psycopg2://airflow:airflow@192.168.29.226:5432/airflow"

# Initialize database connection
engine = create_engine(DATABASE_URL)
metadata = MetaData()
SessionLocal = sessionmaker(bind=engine)
_persistent_session = None  # Persistent session instance

# Table references dictionary
tables = {}
sequence_counters = {}

# TreeNode Class for Parent-Child Relationships
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

# Record hierarchy mapping
record_nodes = {
    "01": node_01, "02": node_02, "12": node_12, "13": node_13,
    "03": node_03, "06": node_06, "08": node_08, "10": node_10,
    "11": node_11, "04": node_04, "05": node_05, "07": node_07,
    "09": node_09
}

# Table name mapping based on record prefixes
table_map = {
    "01": "daroot_table_staging", "02": "dapermit_table_staging",
    "03": "dadafield_table_staging", "04": "dafldspc_table_staging",
    "05": "dafldbhl_table_staging", "06": "dacanres_table_staging",
    "07": "dacanfld_table_staging", "08": "dafreres_table_staging",
    "09": "dafrefld_table_staging", "10": "dapmtbhl_table_staging",
    "11": "daaltadd_table_staging", "12": "daremark_table_staging",
    "13": "dacheck_table_staging"
}

# Field range mapping for parsing records
field_ranges_map = {
    "01": daroot_field_ranges, "02": dapermit_field_ranges, "03": dadafield_field_ranges,
    "04": dafldspc_field_ranges, "05": dafldbhl_field_ranges, "06": dacanres_field_ranges,
    "07": dacanfld_field_ranges, "08": dafreres_field_ranges, "09": dafrefld_field_ranges,
    "10": dapmtbhl_field_ranges, "11": daaltadd_field_ranges, "12": daremark_field_ranges,
    "13": dacheck_field_ranges
}


def read_in_chunks(file_path, chunk_size=500):
    """Generator function to read a file in chunks."""
    with open(file_path, 'r') as file:
        while True:
            chunk = file.readline(chunk_size)  # Read a specific chunk size
            if not chunk:
                break
            yield chunk




def init_db():
    """Initializes the database connection and loads metadata."""
    global metadata, tables, sequence_counters, start_time
    start_time = datetime.now()  # Track start time for runtime calculation
    metadata.reflect(bind=engine)

    # Load table references
    tables.update({table_name: Table(table_name, metadata, autoload_with=engine) for table_name in metadata.tables.keys()})


    # Initialize sequence counters for each table
    sequence_counters.update({table_name: 0 for table_name in tables})

    print("✅ Database initialized successfully.", start_time)

def get_persistent_db_session():
    """Maintains a persistent SQLAlchemy session."""
    global _persistent_session
    if _persistent_session is None:
        _persistent_session = SessionLocal()
    else:
        try:
            _persistent_session.execute(text("SELECT 1"))  # Use text() for raw SQL
        except OperationalError:
            print("⚠️ Session is stale. Reconnecting...")
            _persistent_session.close()
            _persistent_session = SessionLocal()
    return _persistent_session

def generate_sequence_id(table_name):
    """Generates a unique sequence ID for each table."""
    global sequence_counters
    if table_name not in sequence_counters:
        sequence_counters[table_name] = 1  # Initialize if missing

    sequence_id = f"{sequence_counters[table_name]:08d}"
    sequence_counters[table_name] += 1
    return sequence_id

def insert_data(table_name, data):
    """Inserts data into the specified table using a persistent session."""
    session = get_persistent_db_session()

    if table_name not in tables:
        #print(f"Table {table_name} does not exist.")
        return

    table = tables[table_name]

    try:
        #print(f"📝 Data to be inserted into {table_name}: {data}")
        stmt = insert(table).values(data)  # Corrected this line
        session.execute(stmt)
        current_time = datetime.now()
        print("Printing current time after execution", current_time)
        session.commit()
        data.clear()
        #print(f"✅ Data inserted into {table_name} successfully.")
    except Exception as e:
        session.rollback()
        #print(f"❌ Error inserting data into {table_name}: {e}")


def read_and_process_file(filename, chunk_size=500):
    """Reads and processes records from a file in batches using chunk size."""
    buffer = "" 
    total_records = 0

    for chunk in read_in_chunks(filename, chunk_size):
        buffer += chunk
        lines = buffer.split('\n')
        buffer = lines.pop()  

        if lines:
            # Process and print the batch details with line ranges
            end_line = total_records + len(lines)
            print(f"Processing {len(lines)} lines from {total_records + 1} to {end_line}.")
            process_batch(lines)
            total_records += len(lines) 

    if buffer.strip():  # Process remaining data in buffer
        #print(f"Processing 1 line from {total_records + 1} to {total_records + 1}.")
        process_batch([buffer])
        total_records += 1

    print(f" Total records processed: {total_records}")


def process_batch(batch):
    """Processes a batch of records and inserts them into the correct table, maintaining parent-child relationships."""
    data_by_table = {}

    for line in batch:
        record_id = line[:2]
        table_name = table_map.get(record_id, "unknown_table")

        if table_name == "unknown_table":
            print(f" Warning: Unknown record prefix {record_id}, skipping...")
            continue

        record_data = parse_line_to_dict(line, record_id)
        sequence_id = generate_sequence_id(table_name)

        current_node = record_nodes.get(record_id)
        if current_node:
            current_node.set_data(sequence_id)
            record_data["parent_id"] = current_node.get_parent_seq_id()

        record_data["da_record_sequence"] = sequence_id

        if table_name in tables:
            table_columns = {column.name for column in tables[table_name].columns}
            record_data = {key: value for key, value in record_data.items() if key in table_columns}
        else:
            print(f"⚠️ Warning: Table {table_name} not found in metadata.")

        if table_name not in data_by_table:
            data_by_table[table_name] = []
        data_by_table[table_name].append(record_data)

    for table, records in data_by_table.items():
        insert_data(table, records)
    current_time = datetime.now()
    print(f"✅ Batch processed: {len(batch)} records at {current_time}")



def get_field_ranges(record_id: str):
    """
    Returns the field ranges for a given table prefix.
    """
    return field_ranges_map.get(record_id)  # Return an empty list if prefix not found


def parse_line_to_dict(line: str, record_id: str) -> dict:
    """
    Parses a line from the input file into a dictionary using field ranges with start and end positions.
    """
    # Fetch the field ranges using the record_id from field_ranges1
    field_ranges = get_field_ranges(record_id)  # Ensure this returns valid data (like `daroot_field_ranges`)

    # Debugging Output
    #print(f"DEBUG: Parsing line for record_id '{record_id}' with field ranges: {field_ranges}")

    # Initialize an empty dictionary to store the parsed data
    #record = {}
    record = {field_name: (line[start:end].strip() or None) for field_name, (start, end) in field_ranges.items() if end <= len(line)}
    #print (record)
    return record


def teardown_db():
    """Closes the persistent session if open."""
    global _persistent_session
    if _persistent_session:
        _persistent_session.close()
        _persistent_session = None
        print("✅ Database session closed.")

if __name__ == "__main__":
    file_name = "daf802.txt"
    init_db()
    if (len(sys.argv) > 1):
        file_name=sys.argv[1]
    print(file_name)
    read_and_process_file(file_name)
    teardown_db()
