from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import numpy as np
from airflow.utils.log.logging_mixin import LoggingMixin

# Set up logging
logger = LoggingMixin().log

# Helper function for database connection
def get_connection():
    try:
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432"
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        raise

def sftp_file():
    
    my_list = [file1,fil2,file3]
    
    return my_list

# Helper function to insert data into tables
def insert_data(cursor, table_name, columns, values):
    column_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(values))
    query = f"""
        INSERT INTO {table_name} ({column_names})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING;
    """
    try:
        cursor.execute(query, values)
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {str(e)}")
        raise

# Extract function
def extract_data():
    try:
        data = pd.read_csv('/opt/airflow/data/credit_train.csv')
        logger.info("Data extracted successfully.")
        return data.to_dict()
    except Exception as e:
        logger.error(f"Failed to extract data: {str(e)}")
        raise

# Data cleaning and transformation
def transform_data(ti):
    try:
        data = pd.DataFrame.from_dict(ti.xcom_pull(task_ids='extract_data'))
        
        # Drop empty rows and duplicates
        data = data.dropna(how='all')
        data = data.drop_duplicates()
        
        # Clean up strings
        data['Years in current job'] = data['Years in current job'].str.strip()
        data['Home Ownership'] = data['Home Ownership'].str.strip()

        # Handle credit score outliers
        data['Credit Score'] = pd.to_numeric(data['Credit Score'], errors='coerce').fillna(0)
        data['Credit Score'] = np.where(data['Credit Score'] > 999, data['Credit Score'] / 10, data['Credit Score'])

        # Replace invalid current loan amounts
        data['Current Loan Amount'] = data['Current Loan Amount'].replace(99999999, 0)

        # Numeric columns clean-up
        numeric_cols = ['Monthly Debt', 'Years of Credit History', 'Months since last delinquent', 
                        'Number of Open Accounts', 'Number of Credit Problems', 
                        'Current Credit Balance', 'Maximum Open Credit', 
                        'Bankruptcies', 'Tax Liens']
        data[numeric_cols] = data[numeric_cols].apply(pd.to_numeric, errors='coerce')
        data[numeric_cols] = data[numeric_cols].where(data[numeric_cols] >= 0)
        data[numeric_cols] = data[numeric_cols].fillna(data[numeric_cols].median())

        logger.info("Data transformed successfully.")
        return data.to_dict()
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

# Load Customer table
def load_customer_table(ti):
    try:
        data = pd.DataFrame.from_dict(ti.xcom_pull(task_ids='transform_data'))
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS customer (
                customer_id VARCHAR(50) PRIMARY KEY,
                years_in_current_job VARCHAR(15),
                home_ownership VARCHAR(20),
                annual_income NUMERIC
            );
        """)

        data_tuples = [(row['Customer ID'], row['Years in current job'], row['Home Ownership'], row['Annual Income'])
                        for _, row in data.iterrows()]

        insert_query = """
            INSERT INTO customer (customer_id, years_in_current_job, home_ownership, annual_income)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING;
        """
        cur.executemany(insert_query, data_tuples)

        conn.commit()
        logger.info("Customer data loaded successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading customer data: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

# Load Loan table
def load_loan_table(ti):
    try:
        data = pd.DataFrame.from_dict(ti.xcom_pull(task_ids='transform_data'))
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS loan (
                loan_id VARCHAR(50) PRIMARY KEY,
                customer_id VARCHAR(50) REFERENCES customer(customer_id),
                loan_status VARCHAR(15),
                current_loan_amount NUMERIC,
                term VARCHAR(15),
                purpose VARCHAR(25)
            );
        """)

        data_tuples = [(row['Loan ID'], row['Customer ID'], row['Loan Status'], row['Current Loan Amount'], 
                        row['Term'], row['Purpose']) for _, row in data.iterrows()]

        insert_query = """
            INSERT INTO loan (loan_id, customer_id, loan_status, current_loan_amount, term, purpose)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (loan_id) DO NOTHING;
        """
        cur.executemany(insert_query, data_tuples)

        conn.commit()
        logger.info("Loan data loaded successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading loan data: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

# Load Credit History table
def load_credit_history_table(ti):
    try:
        data = pd.DataFrame.from_dict(ti.xcom_pull(task_ids='transform_data'))
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS credit_history (
                loan_id VARCHAR(50) PRIMARY KEY REFERENCES loan(loan_id),
                credit_score NUMERIC,
                monthly_debt NUMERIC,
                years_of_credit_history NUMERIC,
                months_since_last_delinquent NUMERIC,
                number_of_open_accounts NUMERIC,
                number_of_credit_problems NUMERIC,
                current_credit_balance NUMERIC,
                maximum_open_credit NUMERIC,
                bankruptcies NUMERIC,
                tax_liens NUMERIC
            );
        """)

        data_tuples = [(row['Loan ID'], row['Credit Score'], row['Monthly Debt'], row['Years of Credit History'], 
                        row['Months since last delinquent'], row['Number of Open Accounts'], 
                        row['Number of Credit Problems'], row['Current Credit Balance'], 
                        row['Maximum Open Credit'], row['Bankruptcies'], row['Tax Liens']) 
                        for _, row in data.iterrows()]

        insert_query = """
            INSERT INTO credit_history (loan_id, credit_score, monthly_debt, years_of_credit_history, 
                                        months_since_last_delinquent, number_of_open_accounts, 
                                        number_of_credit_problems, current_credit_balance, 
                                        maximum_open_credit, bankruptcies, tax_liens)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (loan_id) DO NOTHING;
        """
        cur.executemany(insert_query, data_tuples)

        conn.commit()
        logger.info("Credit history data loaded successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading credit history data: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

# Null Value Check
def check_null_values():
    conn = get_connection()
    cur = conn.cursor()

    # Check for NULL values in critical columns in Customer table
    cur.execute("SELECT COUNT(*) FROM customer WHERE customer_id IS NULL;")
    null_count = cur.fetchone()[0]
    assert null_count == 0, "There are NULL values in the customer_id column in customer table."

    # Check for NULL values in Loan table
    cur.execute("SELECT COUNT(*) FROM loan WHERE loan_id IS NULL;")
    loan_null_count = cur.fetchone()[0]
    assert loan_null_count == 0, "There are NULL values in the loan_id column in loan table."

    conn.close()


# Range Check for credit_score, loan amounts
def check_value_ranges():
    conn = get_connection()
    cur = conn.cursor()

    # Check for invalid credit scores
    cur.execute("SELECT COUNT(*) FROM credit_history WHERE credit_score > 999;")
    invalid_credit_count = cur.fetchone()[0]
    assert invalid_credit_count == 0, "There are invalid credit scores in the credit_history table."

    # Check for negative values in loan amounts
    cur.execute("SELECT COUNT(*) FROM loan WHERE current_loan_amount < 0;")
    negative_loan_count = cur.fetchone()[0]
    assert negative_loan_count == 0, "There are negative values in the current_loan_amount column in loan table."

    conn.close()


# Duplicate Check for primary keys
def check_duplicates():
    conn = get_connection()
    cur = conn.cursor()

    # Check for duplicate customer IDs
    cur.execute("""
        SELECT customer_id, COUNT(*)
        FROM customer
        GROUP BY customer_id
        HAVING COUNT(*) > 1;
    """)
    duplicates = cur.fetchall()
    assert len(duplicates) == 0, "There are duplicate customer_id entries in the customer table."

    # Check for duplicate loan IDs
    cur.execute("""
        SELECT loan_id, COUNT(*)
        FROM loan
        GROUP BY loan_id
        HAVING COUNT(*) > 1;
    """)
    loan_duplicates = cur.fetchall()
    assert len(loan_duplicates) == 0, "There are duplicate loan_id entries in the loan table."

    conn.close()
    
    
# Foreign Key Validation
def check_foreign_keys():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*)
            FROM loan l
            WHERE NOT EXISTS (
                SELECT 1 FROM customer c WHERE c.customer_id = l.customer_id
            );
        """)
        missing_customers_count = cur.fetchone()[0]
        assert missing_customers_count == 0, f"There are {missing_customers_count} loans with missing customer references."

        logger.info("Foreign key validation passed.")
    except Exception as e:
        logger.error(f"Error during foreign key validation: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 3),
    'retries': 1,
}

# Define the DAG
with DAG(dag_id='credit_pipelineworkflow', default_args=default_args, schedule_interval='@daily') as dag:
    
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )
    
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )
    
    load_customer_task = PythonOperator(
        task_id='load_customer_table',
        python_callable=load_customer_table
    )
    
    load_loan_task = PythonOperator(
        task_id='load_loan_table',
        python_callable=load_loan_table
    )
    
    load_credit_history_task = PythonOperator(
        task_id='load_credit_history_table',
        python_callable=load_credit_history_table
    )
    
    check_null_values_task = PythonOperator(
        task_id='check_null_values',
        python_callable=check_null_values
    )

    check_value_ranges_task = PythonOperator(
        task_id='check_value_ranges',
        python_callable=check_value_ranges
    )

    check_duplicates_task = PythonOperator(
        task_id='check_duplicates',
        python_callable=check_duplicates
    )

    check_foreign_keys_task = PythonOperator(
        task_id='check_foreign_keys',
        python_callable=check_foreign_keys
    )


    # Defining task dependencies
    # extract_task >> transform_task >> [load_customer_task, load_loan_task, load_credit_history_task]
    extract_task >> transform_task >> load_customer_task >> load_loan_task >> load_credit_history_task

    # Run quality checks after all tables are loaded
    [load_customer_task, load_loan_task] >> check_null_values_task
    [load_loan_task, load_credit_history_task] >> check_value_ranges_task
    [load_customer_task, load_loan_task] >> check_duplicates_task
    [load_customer_task, load_loan_task] >> check_foreign_keys_task

