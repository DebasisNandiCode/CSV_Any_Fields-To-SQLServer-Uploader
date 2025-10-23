import argparse
import logging
import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# --- Configuration ---
LOG_FILE = 'upload.log'

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def get_db_engine():
    """Creates and returns a SQLAlchemy engine using credentials from .env file."""
    load_dotenv()
    db_server = os.getenv("DB_SERVER")
    db_database = os.getenv("DB_DATABASE")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    if not all([db_server, db_database, db_user]):
        logging.error("DB_SERVER, DB_DATABASE, and DB_USER must be set in the .env file.")
        raise ValueError("DB_SERVER, DB_DATABASE, and DB_USER must be set in the .env file.")

    # Connection string for SQL Server using pyodbc
    conn_str = (
        "Driver={SQL Server Native Client 11.0};"
        f"Server={db_server};"
        f"Database={db_database};"
        f"UID={db_user};"
        f"PWD={db_password};"
    )
    
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn_str))
        # Test connection
        with engine.connect() as connection:
            logging.info("Successfully connected to the database.")
        return engine
    except SQLAlchemyError as e:
        # Mask password in log output
        safe_conn_str = (
            f"mssql+pyodbc://{db_user}:****@{db_server},1433/{db_database}"
            f"?driver=ODBC+Driver+17+for+SQL+Server&timeout=30"
        )
        logging.error(f"Database connection failed using connection string: {safe_conn_str}")
        logging.error("Please check the following:")
        logging.error("1. The DB credentials and server IP in your .env file are correct.")
        logging.error("2. The SQL Server is configured to allow remote connections.")
        logging.error("3. A firewall is not blocking TCP port 1433 between this machine and the server.")
        logging.error(f"Original error: {e}")
        raise

def get_table_schema(engine, table_name, schema_name):
    """Fetches column names and data types for a given table and schema."""
    logging.info(f"Fetching schema for table: {schema_name}.{table_name}")
    schema = {}
    query = text(f"""
        SELECT COLUMN_NAME, DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = :table_name AND TABLE_SCHEMA = :schema_name
    """)
    try:
        with engine.connect() as connection:
            result = connection.execute(query, {'table_name': table_name, 'schema_name': schema_name})
            for row in result:
                schema[row[0]] = row[1]
        if not schema:
            logging.warning(f"Table '{schema_name}.{table_name}' not found or has no columns.")
        return schema
    except SQLAlchemyError as e:
        logging.error(f"Could not fetch schema for table '{schema_name}.{table_name}': {e}")
        return None

def upload_data(filepath, table_name):
    """Main function to process and upload data from a CSV file to a database table."""
    logging.info(f"Starting data upload process for file: {filepath}")
    
    try:
        db_schema = os.getenv("DB_SCHEMA", 'tcn') 
        engine = get_db_engine()
        table_schema = get_table_schema(engine, table_name, db_schema)

        if not table_schema:
            logging.error(f"Halting process: Could not retrieve schema for table '{db_schema}.{table_name}'.")
            return

        logging.info(f"Reading CSV file: {filepath}")
        # Read everything as string and disable NA detection so blanks stay "".
        # We'll explicitly coerce numeric/date columns later based on table schema.
        df = pd.read_csv(
            filepath,
            dtype=str,
            keep_default_na=False,
            na_filter=False
        )
        logging.info(f"Read {len(df)} rows from CSV.")

        # --- Column Matching ---
        csv_columns = df.columns.tolist()
        db_columns = list(table_schema.keys())
        
        discarded_columns = [col for col in csv_columns if col not in db_columns]
        if discarded_columns:
            logging.warning(f"Discarding columns not found in table '{db_schema}.{table_name}': {discarded_columns}")
        
        final_columns = [col for col in csv_columns if col in db_columns]
        df_filtered = df[final_columns].copy() # Create a copy to avoid SettingWithCopyWarning

        # --- Normalize textual blanks ---
        # Ensure any literal strings like "NULL"/"null" and whitespace-only become truly empty strings
        text_type_markers = [
            'char', 'nchar', 'varchar', 'nvarchar', 'text', 'ntext', 'xml', 'uniqueidentifier'
        ]
        for col, dtype in table_schema.items():
            if col in df_filtered.columns and any(t in dtype for t in text_type_markers):
                # Strip whitespace, turn placeholders into blanks
                df_filtered.loc[:, col] = (
                    df_filtered[col]
                    .astype(str)
                    .str.strip()
                    .replace({
                        'NULL': '', 'null': '', 'NaN': '', 'nan': ''
                    })
                )

        # --- Derived Columns for _Parsed fields ---
        for db_col in db_columns:
            if db_col.endswith('_Parsed'):
                source_col = db_col.replace('_Parsed', '')
                if source_col in df.columns:
                    logging.info(f"Deriving column '{db_col}' from '{source_col}'.")
                    # Ensure the source column is treated as a string before slicing
                    df_filtered[db_col] = pd.to_datetime(df[source_col].astype(str).str[:-4], errors='coerce')
                    # Fill blank/NaT values in _Parsed columns with default date
                    df_filtered[db_col] = df_filtered[db_col].fillna(pd.Timestamp('1900-01-01 00:00:00'))

        # --- Data Type Coercion ---
        for col, dtype in table_schema.items():
            if col in df_filtered.columns:
                try:
                    if 'int' in dtype or 'bigint' in dtype:
                        # For integer columns, convert empty/blank values to 0
                        df_filtered[col] = df_filtered[col].replace(['', 'NULL', 'null', 'NaN', 'nan'], '0')
                        df_filtered[col] = pd.to_numeric(df_filtered[col], errors='coerce').fillna(0).astype('Int64')
                    elif any(t in dtype for t in ['float', 'decimal', 'numeric', 'money']):
                        # For string columns, remove '$' and ',' before converting to numeric
                        if df_filtered[col].dtype == 'object':
                            df_filtered.loc[:, col] = df_filtered[col].str.replace('$', '', regex=False).str.replace(',', '', regex=False)
                        df_filtered.loc[:, col] = pd.to_numeric(df_filtered[col], errors='coerce')
                    elif 'date' in dtype or 'time' in dtype:
                        if not col.endswith('_Parsed'):
                            df_filtered.loc[:, col] = pd.to_datetime(df_filtered[col], errors='coerce')
                    else:
                        # Ensure textual columns keep blanks as empty strings, not NaN
                        if df_filtered[col].dtype == 'object':
                            df_filtered.loc[:, col] = df_filtered[col].fillna('')
                except Exception as e:
                    logging.warning(f"Could not coerce column '{col}' to match DB type '{dtype}': {e}")

        # --- Data Upload ---
        logging.info(f"Uploading {len(df_filtered)} rows to table '{db_schema}.{table_name}'.")
        try:
            # Use a connection from the engine to control the transaction
            with engine.begin() as connection:
                df_filtered.to_sql(table_name, connection, schema=db_schema, if_exists='append', index=False, chunksize=5000)
                logging.info("Data upload completed successfully.")
        except SQLAlchemyError as e:
            logging.error(f"Error during data upload: {e}")
            logging.error("The transaction has been rolled back. Please check the data for issues (e.g., data type mismatches, values too long for a column).")

    except FileNotFoundError:
        logging.error(f"File not found: {filepath}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # usage_example = '''
    # Usage Example:
    #   python main.py "C:\\path\\to\\your\\file.csv" "your_table_name"
    # '''
    # parser = argparse.ArgumentParser(
    #     description="Upload data from a CSV file to a SQL Server table.",
    #     epilog=usage_example,
    #     formatter_class=argparse.RawDescriptionHelpFormatter
    # )
    # parser.add_argument("filepath", type=str, help="The full path to the CSV file.")
    # parser.add_argument("table_name", type=str, help="The name of the database table.")

    # if len(sys.argv) < 3:
    #     parser.print_help()
    #     sys.exit(1)

    # args = parser.parse_args()
    
    filepath = r"D:\PROJECTS\Marged_Raw_Files\10222025\Master_Record_Report_Merged_10-22-2025.csv"
    table_name = r"recordDump"

    # upload_data(args.filepath, args.table_name)

    upload_data(filepath, table_name)
