import psycopg2
from psycopg2.extras import execute_batch
from rich import inspect, print as rp
import os
import pandas as pd
from io import StringIO

def loadEnv():
    """
    Load Env and keep in dict
    """
    from pydantic import BaseModel
    import os
    from dotenv import load_dotenv

    load_dotenv()
    class EnvList(BaseModel):
        DB_PASSWORD: str
        DB_NAME: str
        DB_USER: str
        DB_PORT: str

    def get_all_env() -> EnvList:
        """
        ฟังก์ชันสำหรับดึงค่า environment ทั้งหมดที่ใช้ในระบบ

        Returns:
            dict: dictionary ที่มี key เป็นชื่อตัวแปร environment และ value เป็นค่าที่ตั้งไว้
        """
        env_vars = {
            "DB_PASSWORD": os.getenv("DB_PASSWORD"),
            "DB_NAME": os.getenv("DB_NAME"),
            "DB_USER": os.getenv("DB_USER"),
            "DB_PORT": os.getenv("DB_PORT"),
        }

        return EnvList(**env_vars)
    return get_all_env()

ENVS = loadEnv()
CWD = os.getcwd()
CONNECT_STRING = f"host=localhost port={ENVS.DB_PORT} dbname={ENVS.DB_NAME} user={ENVS.DB_USER} password={ENVS.DB_PASSWORD} target_session_attrs=read-write"

def generate_file_path():
    csvs = os.listdir(CWD+'/customer')
    return [{
        "path": CWD + '/customer/' + i,
        "name": i.split(".")[0]
        } for i in csvs]

def create_customer_table(schema, task):
    """
    Create Table and load data from csv filw
    """
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    
    try:
        # # Create Schema
        create_schema_statement = f'create schema IF NOT EXISTS {schema};'
        cur.execute(create_schema_statement)
        conn.commit()
        
        # Create Table
        for t in task:
            # create_table_statement = f"""
            # CREATE TABLE IF NOT EXISTS {schema}.{t['name']} (
            # Handle table ซ้ำ raise error แล้วไป ทำ table ถัดไปเลย
            create_table_statement = f"""
            CREATE TABLE {schema}.{t['name']} (
                event_time date,
                event_type varchar(20),
                product_id numeric,
                price money,
                user_id numeric,
                user_session varchar(64)
            );
            """
            rp(create_table_statement)
            cur.execute(create_table_statement)
            conn.commit()
            rp(f"\t\tTable {t['name']} created successfully")
            rp(f"\t\tLoad Data")
            with open(t['path'], 'r') as f:
                next(f)
                # copy from only work for schema 'public'
                cur.copy_expert(f"COPY {schema}.{t['name']} FROM STDIN WITH CSV", f)
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        rp(e)
        cur.close()
        conn.close()


def get_item_file_path():
    csvs = os.listdir(CWD+'/items')
    return [{
        "path": CWD + '/items/' + i,
        "name": i.split(".")[0]
        } for i in csvs]

def create_item_table(schema, task):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    try:
        # # Create Schema
        create_schema_statement = f'create schema IF NOT EXISTS {schema};'
        cur.execute(create_schema_statement)
        conn.commit()
        
        # Create Table
        for t in task:
            # create_table_statement = f"""
            # CREATE TABLE IF NOT EXISTS {schema}.{t['name']} (
            # Handle table ซ้ำ raise error แล้วไป ทำ table ถัดไปเลย
            # product_id,category_id,category_code,brand
            create_table_statement = f"""
            CREATE TABLE {schema}.{t['name']} (
                product_id numeric,
                category_id varchar(255),
                category_code varchar(255),
                brand varchar(255)
            );
            """
            rp(create_table_statement)
            cur.execute(create_table_statement)
            conn.commit()
            rp(f"\t\tTable {t['name']} created successfully")
            rp(f"\t\tLoad Data")
            with open(t['path'], 'r') as f:
                next(f)
                # copy from only work for schema 'public'
                cur.copy_expert(f"COPY {schema}.{t['name']} FROM STDIN WITH CSV", f)
            conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        rp(e)
        cur.close()
        conn.close()




if __name__ == "__main__":
    customer_task = generate_file_path()
    schema_name = "ex04"
    create_customer_table(schema_name, customer_task)
    
    item_task = get_item_file_path()
    create_item_table(schema_name, item_task)
