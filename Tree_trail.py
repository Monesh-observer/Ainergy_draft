#import logging
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import OperationalError
from datetime import datetime, timezone

from field_ranges import (
    daroot_field_ranges, dapermit_field_ranges, dadafield_field_ranges,
    dafldspc_field_ranges, dafldbhl_field_ranges, dacanres_field_ranges, dacanfld_field_ranges,
    dafreres_field_ranges, dafrefld_field_ranges, dapmtbhl_field_ranges,
    daaltadd_field_ranges, daremark_field_ranges, dacheck_field_ranges
)

DB_URI = 'postgresql+psycopg2://airflow:airflow@192.168.29.223:5432/airflow'


engine = None
SessionLocal = None
_persistent_session = None
metadata = None  
tables = {} 
total_record_count = 0


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
            #logging.info("Clearing existing data for node %s.", self.data)
            self.data = None

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

api_number = None

def init_db():
    global engine, SessionLocal, metadata, tables

    if engine is None:
        engine = create_engine(DB_URI)
        SessionLocal = sessionmaker(bind=engine)

    _persistent_session = SessionLocal()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    for table_name in metadata.tables:
        tables[table_name] = metadata.tables[table_name]
    
    print(f"Loaded {len(tables)} tables from the database.")
    return _persistent_session

def get_persistent_db_session():
    global _persistent_session
    if _persistent_session is None:
        _persistent_session = init_db()
    else:
        try:
            _persistent_session.execute(text("SELECT 1")) 
        except OperationalError:
            print("⚠️ Session is stale. Reconnecting...")
            _persistent_session.close()
            _persistent_session = init_db()
    return _persistent_session

def read_and_process_file(file_path):
    global api_number
    record_processors = {
        '01': (daroot_field_ranges, upload_daroot_to_postgres),
        '02': (dapermit_field_ranges, upload_dapermit_to_postgres),
        '03': (dadafield_field_ranges, upload_dafield_to_postgres),
        '04': (dafldspc_field_ranges, upload_dafldspc_to_postgres),
        '05': (dafldbhl_field_ranges, upload_dafldbhl_to_postgres),
        '06': (dacanres_field_ranges, upload_dacanres_to_postgres),
        '07': (dacanfld_field_ranges, upload_dacanfld_to_postgres),
        '08': (dafreres_field_ranges, upload_dafreres_to_postgres),
        '09': (dafrefld_field_ranges, upload_dafrefld_to_postgres),
        '10': (dapmtbhl_field_ranges, upload_dapmtbhl_to_postgres),
        '11': (daaltadd_field_ranges, upload_daaltadd_to_postgres),
        '12': (daremark_field_ranges, upload_daremark_to_postgres),
        '13': (dacheck_field_ranges, upload_dacheck_to_postgres),
    }

    try:
        with open(file_path, 'r', encoding='ISO-8859-1') as file:
            for line in file:
                record_id = line[:2]
                if record_id in record_processors:
                    field_ranges, upload_function = record_processors[record_id]
                    if record_id == '02':
                        api_number = line[503:511].strip()
                    process_record(line, field_ranges, upload_function, record_id)
    except Exception as e:
        #logging.error("Error processing file: %s", e)
        print("Error processing file: {}".format(str(e)))

def process_record(line, field_ranges, upload_function, record_id):
    global api_number, total_record_count
    structured_record = {field_name: line[start:end].strip() for field_name, (start, end) in field_ranges.items() if end <= len(line)}
    node = record_nodes.get(record_id)
    if node and node.get_data() is not None:
        node.clear_data()
    if record_id not in ['02', '01', '12', '13'] and api_number:
        structured_record['api_number'] = api_number
        #logging.info("Shared API number %s with record %s", api_number, record_id)
    parent_seq_id = node.get_parent_seq_id() if node else None
    if parent_seq_id is not None:
        structured_record['parent_id'] = parent_seq_id
    if 'da_record_sequence' not in structured_record:
        structured_record['record_id'] = record_id
    sequence_created = upload_function(structured_record)
    node.set_data(sequence_created)

    # Increment the total record count and print runtime every 500 records
    total_record_count += 1
    if total_record_count % 500 == 0:
        current_time = datetime.now()
        print("printing current time:", current_time)
        #logging.info(f"Processed {total_record_count} records, Elapsed time: {elapsed_time}")
        #print(f"Processed {total_record_count} records, Elapsed time: {elapsed_time}")

# Modified function to increment the record count for each table
def upload_record_to_postgres(record, table_name, conflict_columns=None):
    """Upload the record to the specified table in the database."""
    session = get_persistent_db_session()  # Use the persistent session
    current_timestamp = datetime.now(timezone.utc)

    try:
        if table_name in tables:
            table = tables[table_name]  # Use the table from the loaded references
            record['updated_at'] = current_timestamp
            record['created_at'] = current_timestamp

            # Remove unconsumed columns
            record = {k: v for k, v in record.items() if k in table.columns}

            stmt = insert(table).values(record)

            if conflict_columns:
                update_values = {k: v for k, v in record.items() if k not in ['da_record_sequence', 'api_number', 'created_at']}
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_columns,
                    set_=update_values
                )

            stmt = stmt.returning(table.c.da_record_sequence)
            result = session.execute(stmt)
            sequence_created = result.fetchone()[0]
            return sequence_created
    except Exception as e:
        print(f"Error uploading record: {str(e)}")
        return None


# Insert functions that use the same upload logic
def upload_daroot_to_postgres(daroot_record):
    return upload_record_to_postgres(daroot_record, 'daroot_table', ['da_permit'])

def upload_dapermit_to_postgres(dapermit_record):
    return upload_record_to_postgres(dapermit_record, 'dapermit_table', ['da_permit_number'])

def upload_dafield_to_postgres(dadafield_record):
    return upload_record_to_postgres(dadafield_record, 'dadafield_table', ['da_field_number'])

def upload_dafldspc_to_postgres(dafldspc_record):
    return upload_record_to_postgres(dafldspc_record, 'dafldspc_table',None)

def upload_dafldbhl_to_postgres(dafldbhl_record):
    return upload_record_to_postgres(dafldbhl_record, 'dafldbhl_table',None)

def upload_dacanres_to_postgres(dacanres_record):
    return upload_record_to_postgres(dacanres_record, 'dacanres_table', ['da_can_restr_key', 'da_can_restr_type'])

def upload_dacanfld_to_postgres(dacanfld_record):
    return upload_record_to_postgres(dacanfld_record, 'dacanfld_table',None)

def upload_dafreres_to_postgres(dafreres_record):
    return upload_record_to_postgres(dafreres_record, 'dafreres_table', ['da_free_restr_key'])

def upload_dafrefld_to_postgres(dafrefld_record):
    return upload_record_to_postgres(dafrefld_record, 'dafrefld_table',None)

def upload_dapmtbhl_to_postgres(dapmtbhl_record):
    return upload_record_to_postgres(dapmtbhl_record, 'dapmtbhl_table',None)

def upload_daaltadd_to_postgres(daaltadd_record):
    return upload_record_to_postgres(daaltadd_record, 'daaltadd_table',None)

def upload_daremark_to_postgres(daremark_record):
    return upload_record_to_postgres(daremark_record, 'daremark_table', ['da_remark_sequence_number'])

def upload_dacheck_to_postgres(dacheck_record):
    return upload_record_to_postgres(dacheck_record, 'dacheck_table',None)

# Main function to process the file and track time
def main(file_path):
    global start_time
    start_time = datetime.now()
    print(f"Starting data processing and upload: {start_time}")
    init_db()

    try:
        read_and_process_file(file_path)
        print("Data processing and upload completed successfully!")
    except Exception as e:
        print(f"Error during processing: {str(e)}")
    
    end_time = datetime.now()
    print(f"Total records processed: {total_record_count}")
    if _persistent_session:
        _persistent_session.close()

if __name__ == "__main__":
    file_path = './daf8021.txt'
    main(file_path)
