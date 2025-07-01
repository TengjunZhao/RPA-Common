import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text


def getLotList(engine):
    maxWorkdtQuery = text("SELECT max(workdt) FROM db_etLog;")
    with engine.connect() as connection:
        maxWorkdt = connection.execute(maxWorkdtQuery).fetchone()
    lotListQuery = text(f"SELECT workdt, lot_id, oper_old, trans_time, main_equip_id, equip_model "
                        f"FROM db_yielddetail "
                        f"WHERE workdt >= '{maxWorkdt[0]}' AND oper_old = '5600'"
                        f"GROUP BY lot_id;")
    with engine.connect() as connection:
        lotList = pd.read_sql(lotListQuery, connection)
    return lotList


# 将lotList中的数据插入到MySQL数据库中
def import_data(engine, df):
    # 列名映射：source_column -> target_column
    column_mapping = {
        'workdt': 'workdt',  # 列名相同
        'lot_id': 'lot_id',  # lot_id映射到lot_id
        'oper_old': 'oper',  # oper_old映射到oper
        'trans_time': 'trans_time',
        'main_equip_id': 'equip',
        'equip_model': 'model'
    }

    # 选择需要的列并进行重命名
    df_to_insert = df[list(column_mapping.keys())].rename(columns=column_mapping)

    # 记录成功和失败的行数
    success_count = 0
    failure_count = 0
    failed_rows = []

    # 使用事务确保数据一致性
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # 逐条插入数据
            for index, row in df_to_insert.iterrows():
                try:
                    # 将单行数据转换为DataFrame并插入
                    pd.DataFrame([row]).to_sql(
                        name='db_etlog',
                        con=connection,
                        if_exists='append',
                        index=False
                    )
                    success_count += 1
                except Exception as e:
                    failure_count += 1
                    failed_rows.append((index, str(e)))
                    print(f"行 {index} 插入失败: {e}")
                    continue  # 跳过当前行继续处理下一行

            # 提交事务
            transaction.commit()
            print(f"数据导入完成: 成功 {success_count} 行, 失败 {failure_count} 行")

            # 打印失败行的详细信息
            if failed_rows:
                print("\n失败行详情:")
                for idx, error in failed_rows:
                    print(f"行 {idx}: {error}")

        except Exception as e:
            # 发生严重错误时回滚整个事务
            transaction.rollback()
            print(f"发生严重错误，所有数据未提交: {e}")
        finally:
            engine.dispose()  # 关闭数据库连接池


# 获取还未执行过的lot_id
def taskLot(engine):
    # 查询db_etLog中已有的lot_id
    existing_lot_ids_query = text("SELECT lot_id FROM db_etLog WHERE log_check IS NULL;")
    with engine.connect() as connection:
        existing_lot_ids = connection.execute(existing_lot_ids_query).fetchall()
    existing_lot_ids = [lot_id[0] for lot_id in existing_lot_ids]
    return existing_lot_ids


def main(mode):
    if mode == 'test':
        db_config = {
            'host': 'localhost',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }
    else:
        db_config = {
            'host': '172.27.154.57',
            'user': 'remoteuser',
            'password': 'password',
            'database': 'cmsalpha',
            'charset': 'utf8mb4',
            'port': 3306,
        }
    # 创建数据库连接
    engine = create_engine(
        f'mysql+pymysql://{db_config["user"]}:{db_config["password"]}@{db_config["host"]}:{db_config["port"]}/{db_config["database"]}'
    )
    lotList = getLotList(engine)
    import_data(engine, lotList)
    lotList = taskLot(engine)
    return lotList


if __name__ == '__main__':
    main('test')
    os.system('pause')