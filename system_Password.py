import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text


def get_password(engine, system):
    query = text(f"SELECT username, password FROM db_hisystem "
                 f"WHERE hi_system = '{system}';")
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
    return result if result else None


def main(mode, system):
    if mode == 'test':
        db_config = {
            'host': 'localhost',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'modulemte',
            'charset': 'utf8mb4',
            'port': 3306,
        }
    else:
        db_config = {
            'host': '172.27.154.57',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'modulemte',
            'charset': 'utf8mb4',
            'port': 3306,
        }
    # 创建数据库连接
    engine = create_engine(
        f'mysql+pymysql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
    )
    user, password = get_password(engine, system)
    print(user, password)
    return user, password


if __name__ == '__main__':
    main('test', 'hibim')
    os.system('pause')