# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
import psycopg2
import traceback
import pyodbc
import os
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text, Column, Integer, String
# from sqlalchemy.orm import declarative_base, sessionmaker

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.

    #Kết nối đến Azure SQL Database sửa lại username và password của bạn
    server = 'xznozrobo3funm76yoyaoh75wm-lvvgvquleiuurnfvyvnetw7hoq.datamart.pbidedicated.windows.net'
    database = 'Oil price forecast'
    username = 'api@oilgas.ai'
    password = 'Vpi167YmWwnLEgac'
    driver = '{ODBC Driver 18 for SQL Server}'
    params = 'Driver=' + driver + ';Server=' + server + ',1433;Database=' + database + ';Uid={' + username + '};Pwd={' + password + '};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword'
    engin_datamart = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # cnxn = cnxn_datamart.connect()
    # Tạo một đối tượng Cursor để đọc dữ liệu từ bảng azure_table trên Azure SQL Database
    # cur_datamart = cnxn_datamart.cursor()
    # cur_datamart.execute('SELECT * FROM fact_oilprice_input')


    # db_name = os.environ['POSTGRES_DB']
    # db_user = os.environ['POSTGRES_USER']
    # db_password = os.environ['POSTGRES_PASSWORD']
    # db_host = os.environ['POSTGRES_HOST']
    # db_port = os.environ['POSTGRES_PORT']
    #
    # # Kết nối đến cơ sở dữ liệu
    # conn_postgres = psycopg2.connect(
    #     dbname=db_name,
    #     user=db_user,
    #     password=db_password,
    #     host=db_host,
    #     port=db_port
    # )

    db_name = os.environ['POSTGRES_DB']
    db_user = os.environ['POSTGRES_USER']
    db_password = os.environ['POSTGRES_PASSWORD']
    db_host = os.environ['POSTGRES_HOST']
    db_port = os.environ['POSTGRES_PORT']


    engine_datamart = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # Tạo engine để kết nối đến cơ sở dữ liệu
    engine_postgresql = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Tạo kết nối

    metadata = MetaData()
    table_from_another_database = Table('fact_oilprice_input', metadata, autoload_with=engin_datamart)

    # Tạo bảng tương tự trong engine_postgresql
    table_in_postgresql = Table('fact_oilprice_input', metadata,
                                *[Column(c.name,  String if 'NVARCHAR' in str(c.type) else c.type, nullable=c.nullable) for c in
                                  table_from_another_database.columns], extend_existing=True)

    metadata.create_all(engine_postgresql)

    # Ghi dữ liệu vào bảng trong engine_postgresql
    # with engine_datamart.connect() as conn_datamart, engine_postgresql.connect() as conn_postgresql:
    #     select_query = table_from_another_database.select()
    #     data = [dict(zip(table_from_another_database.columns.keys(), row)) for row in conn_datamart.execute(select_query).fetchall()]
    #     insert_query = table_in_postgresql.insert()
    #     conn_postgresql.execute(insert_query, data)

    with engine_datamart.connect() as conn_datamart, engine_postgresql.connect() as conn_postgresql:
        try:
            select_query = table_from_another_database.select()
            data = [dict(zip(table_from_another_database.columns.keys(),
                             [str(val) if isinstance(val, bytes) else val for val in row])) for row in
                    conn_datamart.execute(select_query).fetchall()]

            insert_query = table_in_postgresql.insert()
            conn_postgresql.execute(insert_query, data)
            conn_postgresql.commit()
        except:
            print(traceback.format_exc())
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    # Tạo một đối tượng Cursor để thực thi các truy vấn SQL trên PostgreSQL
    # cur_postgres = conn_postgres.cursor()

    # # Đọc từng bản ghi và ghi vào bảng postgres_table trên PostgreSQL
    # for row in cur_datamart:
    #     cur_postgres.execute("INSERT INTO postgres_table VALUES (%s, %s, %s)", row)
    # # Lưu các thay đổi vào cơ sở dữ liệu
    # conn_postgres.commit()

    # Đóng các kết nối


    # Đọc dữ liệu từ azure_table sang DataFrame của pandas
    # query = text("SELECT * FROM fact_oilprice_input")
    # df = pd.read_sql(query, cnxn)
    #
    # # Lưu dữ liệu từ DataFrame sang postgresql_table
    # df.to_sql(name='postgresql_table', con=conn_postgres, if_exists='replace', index=False)
    #
    # # cnxn_datamart.close()
    # cnxn.close()
    # # # cur_postgres.close()
    # conn_postgres.close()
    # # Tạo một đối tượng Cursor để thực thi
    # các truy vấn SQL
    # cur = conn.cursor()
    # # Tạo model map với bảng fact_oilprice_input
    # metadata = MetaData()
    # OilPrices = Table('fact_oilprice_input', metadata,
    #                   autoload_with=engine)
    #
    # # Truy vấn đến DB limit 10 record
    # results = db.query(OilPrices).limit(10).all()
    #
    # for col in OilPrices.columns:
    #     print(col.name)
    #     print(col.type)
    #
    # # Thay đổi các giá trị sau để phù hợp với thông tin đăng nhập của bạn
    # # db_name = 'test'
    # # db_user = 'postgres'
    # # db_password = 'mac0901'
    # # db_host = 'localhost'
    # # db_port = '5432'
    #
    # db_name = os.environ['POSTGRES_DB']
    # db_user = os.environ['POSTGRES_USER']
    # db_password = os.environ['POSTGRES_PASSWORD']
    # db_host = os.environ['POSTGRES_HOST']
    # db_port = os.environ['POSTGRES_PORT']
    #
    # # Kết nối đến cơ sở dữ liệu
    # conn = psycopg2.connect(
    #     dbname=db_name,
    #     user=db_user,
    #     password=db_password,
    #     host=db_host,
    #     port=db_port
    # )
    #
    # # Tạo một đối tượng Cursor để thực thi các truy vấn SQL
    # cur = conn.cursor()
    #
    # # Thực thi một truy vấn SQL
    # cur.execute('SELECT * FROM test_table')
    #
    # # Lấy tất cả các bản ghi từ kết quả truy vấn
    # records = cur.fetchall()
    #
    # # In ra tất cả các bản ghi
    # for record in records:
    #     print(record)
    # print("abc")
    # # Đóng kết nối
    # cur.close()
    # conn.close()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
