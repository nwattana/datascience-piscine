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


# LOAD CSV TO DATABASE
def insert_csv(cur, task, table):
    ...

def create_table(schema, task):
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
            create_table_statement = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{t['name']} (
                event_time timestamptz not null,
                event_type varchar(32),
                product_id bigint,
                price money,
                user_id numeric,
                user_session char(36)
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
    task = generate_file_path()
    schema_name = "ex02"
    create_table(schema_name, task)
    # create_table_2(schema_name, task)
