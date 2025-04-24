import psycopg2
from rich import inspect, print as rp


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
DES_TABLE_NAME = 'customer3'


# https://www.w3schools.com/sql/sql_insert_into_select.asp
def insert_into_customer(schema, des, src):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    create_table_statement = f"""
        INSERT INTO {schema}.{des}
        SELECT * FROM {schema}.{src}
    """
    cur.execute(create_table_statement)
    conn.commit()
    cur.close()
    conn.close()

def create_table_customer(schema):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    create_table_statement = f"""
        CREATE TABLE {schema}.{DES_TABLE_NAME} (
            event_time date,
            event_type varchar(20),
            product_id numeric,
            price money,
            user_id numeric,
            user_session varchar(64)
    );
    """
    cur.execute(create_table_statement)
    conn.commit()
    cur.close()
    conn.close()
    
    return DES_TABLE_NAME
    

def list_table_name(schema):
    conn = psycopg2.connect(CONNECT_STRING)
    cur = conn.cursor()
    
    quertStatement = f"""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '{schema}'
    """
    rp(quertStatement)
    cur.execute(quertStatement)
    a = cur.fetchall()
    rp(a)
    cur.close()
    conn.close()
    
    return [i[0] for i in a if i[0].startswith('data_202')]
    

def main():
    # list database data_202*
    table_listed = list_table_name(SCHEMA_NAME)
    print(table_listed)
    # create table customers
    des = create_table_customer(SCHEMA_NAME)
    
    # insert all data from data_202* in to customer
    for table in table_listed:
        insert_into_customer(SCHEMA_NAME, des, table)
        

if __name__ == "__main__":
    main()