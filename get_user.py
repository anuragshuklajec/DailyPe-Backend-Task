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
    try:
        params = event.get('body', '{}')
        query = "SELECT user_id, manager_id, full_name, mob_num, pan_num, created_at, updated_at, is_active FROM users"
        conditions = []
        query_params = []

        if 'mob_num' in params:
            conditions.append("mob_num = %s")
            query_params.append(params['mob_num'])

        if 'user_id' in params:
            conditions.append("user_id = %s")
            query_params.append(params['user_id'])

        if 'manager_id' in params:
            conditions.append("manager_id = %s")
            query_params.append(params['manager_id'])

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query, query_params)
        users = cursor.fetchall()
        
        for user in users:
            user["created_at"] = str(user["created_at"])
            user["updated_at"] = str(user["updated_at"])
            
        cursor.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'users': users})
        }

    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error retrieving users')
        }

