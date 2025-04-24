# MUST DO ex01 first
import psycopg2
from rich import inspect, print as rp
import random


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

## You have to join all the data_202*_*** tables together in a table called "customers"
ENVS = loadEnv()
CONNECT_STRING = f"host=localhost port={ENVS.DB_PORT} dbname={ENVS.DB_NAME} user={ENVS.DB_USER} password={ENVS.DB_PASSWORD} target_session_attrs=read-write"
SCHEMA_NAME = "data_werehouse"


def clear_duplicate_data(schema, table_name):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    
    
    # https://neon.tech/postgresql/postgresql-tutorial/how-to-delete-duplicate-rows-in-postgresql
    # CREATE TEMP TABLE
    # insert distinct
    # DROP OLD
    # RENAME TEMP
    
    temp_table_name = ''.join(random.choice('abcdefghijklmnopqrstuvwxyzz') for i in range(20))
    
    create_temp_table_statement = f"""
        CREATE TABLE {schema}.{temp_table_name} (LIKE {schema}.{table_name});
    """
    cur.execute(create_temp_table_statement)
    conn.commit()
    rp("CRETE TEMP TABLE {} success".format(temp_table_name))
    
    insert_distinct_statement = f"""
        INSERT INTO {schema}.{temp_table_name} (event_time, event_type, product_id, price, user_id, user_session)
        SELECT DISTINCT ON (event_time, event_type, product_id, price, user_id, user_session) event_time, event_type, product_id, price, user_id, user_session 
        FROM {schema}.{table_name};
    """
    cur.execute(insert_distinct_statement)
    conn.commit()
    rp("INSERT DATA FROM {} TO {} SUCCESS".format(table_name, temp_table_name))
    
    drop_old_statement = f"""
    DROP TABLE iF EXISTS {schema}.{table_name};
    """
    cur.execute(drop_old_statement)
    conn.commit()
    rp("DROP TABLE {} SUCCESS".format(table_name))
    
    rename_temp = f"""
    ALTER TABLE {schema}.{temp_table_name}
    RENAME TO {table_name}
    """
    cur.execute(rename_temp)
    conn.commit()
    rp("RENAME TABLE {} TO {} SUCCESS".format(temp_table_name,table_name))
    
    conn.commit()
    cur.close()
    conn.close()

def main():
    clear_duplicate_data(SCHEMA_NAME, 'customer')

if __name__ == "__main__":
    main()