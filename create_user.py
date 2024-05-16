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
    query = """
        INSERT INTO users (full_name, mob_num, pan_num, is_active) VALUES ('John Doe', '9125436252', 'ABCDE1234F', true);
    """

    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory = RealDictCursor)
        
        cursor.execute(query)
        conn.commit()
        
        cursor.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps('User created successfully!')
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error creating User')
        }

