# You must combine the "customers" tables with "items" in the "customers" table
# JOIN ผลลัพ ใส่ ตารางใหม่ ละมั้ง

# MUST DO ex01 first
import psycopg2
from rich import inspect, print as rp
import random

import functools
from typing import Callable
import psycopg2
from psycopg2.extensions import connection, cursor


def with_db_connection(func: Callable):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        conn: connection = psycopg2.connect(CONNECT_STRING)
        cur: cursor = conn.cursor()
        try:
            # เรียกฟังก์ชันที่ถูก decorate และส่ง cursor กับ connection ไปด้วย
            result = func(cur=cur, conn=conn, *args, **kwargs)
            conn.commit()
            return result
        except Exception as e:
            conn.rollback()
            print(f"Error in {func.__name__}: {str(e)}")
            raise
        finally:
            cur.close()
            conn.close()

    return wrapper


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

def fusion( des, src1, src2):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    # Create new table
    create_statemet = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{des} (
        event_time date,
        event_type varchar(20),
        price money,
        user_id numeric,
        user_session varchar(64),
        product_id numeric,
        category_id varchar(255),
        category_code varchar(255),
        brand varchar(255)
    )
    """
    cur.execute(create_statemet)
    conn.commit()
    
    # INSERT WITH WHER
    insert_statement = f"""
    INSERT INTO {SCHEMA_NAME}.{des} (event_time, event_type, price, user_id, user_session, product_id, category_id, category_code, brand)
    SELECT t1.event_time, t1.event_type, t1.price, t1.user_id, t1.user_session, t2.product_id, t2.category_id, t2.category_code, t2.brand
    FROM {SCHEMA_NAME}.{src1} t1
    JOIN {SCHEMA_NAME}.{src2} t2
    ON t1.product_id = t2.product_id 
    """
    cur.execute(insert_statement)
    conn.commit()
    cur.close()
    conn.close()



def main():
    fusion(des="customer_items_2", src1="customer", src2="items")


if __name__ == "__main__":
    main()
