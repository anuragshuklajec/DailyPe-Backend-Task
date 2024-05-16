import json
import psycopg2
import os
from psycopg2.extras import RealDictCursor

connection = None

def get_db_connection():
    global connection
    if connection is None or connection.closed != 0:
        db_host = os.environ['RDS_HOST']
        db_name = os.environ['RDS_DATABASE']
        db_user = os.environ['RDS_USERNAME']
        db_password = os.environ['RDS_PASSWORD']
        db_port = 5432

        connection = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
    return connection

def lambda_handler(event, context):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS managers (
        manager_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
        name VARCHAR(255) NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE
    );
    """

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory = RealDictCursor)
        
        cursor.execute(create_table_query)
        conn.commit()
        
        cursor.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps('Table created successfully!')
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error creating table')
        }

