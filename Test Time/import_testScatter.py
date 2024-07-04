import pandas as pd
import pymysql
import os
from datetime import datetime


def read_xls(path):
    # 读取Excel文件
    df = pd.read_excel(path)
    # 将 DataFrame 中的 NaN 替换为 None
    df = df.applymap(lambda x: None if pd.isna(x) else x)
    return df


def import_data(df):
    # 数据库连接配置，请根据你的实际情况调整
    connection = pymysql.connect(host='localhost',
                                 user='remoteuser',
                                 password='password',
                                 database='cmsalpha',
                                 cursorclass=pymysql.cursors.DictCursor)

    with connection:
        with connection.cursor() as cursor:
            # 定义 SQL 插入或更新语句
            sql = """
            INSERT INTO db_test_scatter (lot_id, rwk_cnt, device, oper, model, table_id, fix, dimm, spd_lotid, serial_no, 
            max_m, pgm, start_time, end_time, test_time, h_diag, result, retest, first_fail, 
            only_fail, first_fail_time, down)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                rwk_cnt = VALUES(rwk_cnt),
                model = VALUES(model),
                table_id = VALUES(table_id),
                fix = VALUES(fix),
                dimm = VALUES(dimm),
                spd_lotid = VALUES(spd_lotid),
                serial_no = VALUES(serial_no),
                max_m = VALUES(max_m),
                pgm = VALUES(pgm),
                start_time = VALUES(start_time),
                end_time = VALUES(end_time),
                test_time = VALUES(test_time),
                h_diag = VALUES(h_diag),
                result = VALUES(result),
                retest = VALUES(retest),
                first_fail = VALUES(first_fail),
                only_fail = VALUES(only_fail),
                first_fail_time = VALUES(first_fail_time),
                down = VALUES(down)
            """

            # 将DataFrame中的数据转换为用于插入的元组列表
            records = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]
            try:
                # 循环插入每行数据
                for record in records:
                    cursor.execute(sql, record)
                connection.commit()  # 提交事务
                print("数据成功插入或更新到MySQL数据库。")
            except Exception as e:
                print(f"插入数据时发生错误: {e}")


def main():
    # Excel文件路径
    file_path = r'E:\sync\临时存放\source.xlsx'
    # 读取数据
    data = read_xls(file_path)
    # 导入数据
    import_data(data)


if __name__ == '__main__':
    main()
    os.system('pause')