import json
import uuid
import psycopg2
import os
from psycopg2.extras import RealDictCursor
from pydantic import BaseModel, field_validator, ValidationError
from typing import Optional

connection = None

class UserInput(BaseModel):
    full_name: str
    mob_num: str
    pan_num: str
    manager_id: Optional[int] = None
    
    @field_validator('full_name')
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError('Full name must not be empty')
        return v

    @field_validator('mob_num')
    def validate_and_transform_mob_num(cls, v):
        v = v.strip()
        if v.startswith('+91'):
            v = v[3:]
        elif v.startswith('0'):
            v = v[1:]
        if not v.isdigit() or len(v) != 10:
            raise ValueError('Mobile number must be a valid 10-digit number')
        return v

    @field_validator('pan_num')
    def validate_and_transform_pan_num(cls, v):
        v = v.strip().upper()
        if not v.isalnum() or len(v) != 10:
            raise ValueError('PAN number must be a valid 10-character alphanumeric string')
        return v

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
    
    body = event.get('body', '{}')

    query = """
        INSERT INTO users (full_name, mob_num, pan_num) VALUES ('John Doe', '9125436252', 'ABCDE1234F' );
    """

    try:
        user_input = UserInput(**body)
        
        query = """
            INSERT INTO users (full_name, mob_num, pan_num, manager_id) 
            VALUES (%s, %s, %s, %s)
            RETURNING user_id, full_name, mob_num, pan_num, manager_id;
        """
        query_params = (user_input.full_name, user_input.mob_num, user_input.pan_num, user_input.manager_id)

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        cursor.execute(query, query_params)
        # inserted_row = cursor.fetchone()
        conn.commit()
        
        cursor.close()
        
        return {
            'statusCode': 200,
            'body': json.dumps('User created successfully!')
        }
    except ValidationError as e:
        
        return {
            'statusCode': 400,
            'body': e.json()
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps('Error creating User')
        }

