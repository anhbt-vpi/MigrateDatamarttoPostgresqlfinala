from sqlalchemy import create_engine, Table, MetaData, text, Column, Integer, String
from sqlalchemy.schema import PrimaryKeyConstraint, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import ForeignKey
def migrateDB():

    server = 'xznozrobo3funm76yoyaoh75wm-fr3e3p3dk6eejffi7w4p27iybe.datamart.pbidedicated.windows.net'
    database = '2023_LPG_Datamart_Hanhdh'
    username = 'api@oilgas.ai'
    password = 'Vpi167YmWwnLEgac'
    driver = '{ODBC Driver 18 for SQL Server}'
    params = 'Driver=' + driver + ';Server=' + server + ',1433;Database=' + database + ';Uid={' + username + '};Pwd={' + password + '};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryPassword'


    db_name = 'test'
    db_user = 'postgres'
    db_password = 'mac0901'
    db_host = 'host.docker.internal'
    db_port = 5432

    # Tạo engine để kết nối đến cơ sở dữ liệu cổng sql datamart
    engine_datamart = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    # Tạo engine để kết nối đến cơ sở dữ liệu postgresql trên local
    engine_postgresql = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    metadata_datamart = MetaData()
    table_from_datamart = Table('Date_table', metadata_datamart, autoload_with=engine_datamart)
    metadata_postgresql = MetaData()
    table_in_postgresql = Table('Date_table', metadata_postgresql,
                      autoload_with=engine_postgresql)


    with engine_datamart.connect() as conn_datamart, engine_postgresql.connect() as conn_postgresql:
        select_query = table_from_datamart.select().limit(111)
        data = [dict(zip(table_from_datamart.columns.keys(),
                         [str(val) if isinstance(val, bytes) else val for val in row])) for row in
                conn_datamart.execute(select_query).fetchall()]

        # insert_query = table_in_postgresql.insert()
        # conn_postgresql.execute(insert_query, data)
        insert_query = insert(table_in_postgresql).values(data).on_conflict_do_nothing()
        conn_postgresql.execute(insert_query)
        conn_postgresql.commit()

    # # Định nghĩa cột primary key
    # primary_key_columns = [c for c in table_in_postgresql.columns if c.primary_key]
    # insert_stmt = insert(table_in_postgresql).values(data)
    #
    # # Chỉ định bỏ qua các giá trị trùng lặp
    # on_conflict_stmt = insert_stmt.on_conflict_do_nothing(index_elements=primary_key_columns)
    # with engine_postgresql.connect() as conn_postgresql:
    #     conn_postgresql.execute(on_conflict_stmt)
    #     conn_postgresql.commit()
if __name__ == '__main__':
    migrateDB()

