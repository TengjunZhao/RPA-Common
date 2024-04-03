import pandas as pd
import pymysql
from datetime import datetime, timedelta
import os
import glob


def read_xls(path):
    # 尝试读取Excel文件
    try:
        # 跳过文件中的非数据行，这需要您根据实际文件调整skiprows的值
        df = pd.read_excel(path)
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        exit()
    known_columns = [
        "DATE", "EQUIP Name", "Product", "Lot No", "Event2","Event3","TRANSMISSION TIME","SERVER TIME"
    ]
    df = df.loc[:, df.columns.isin(known_columns)]
    # 定义替换规则
    replacements = [
        (' ', '_'),
        ('(', '_'),
        (')', ''),
        # 可以继续添加其他替换规则
    ]
    # 应用替换规则到列名
    new_columns = []
    for column in df.columns:
        for old, new in replacements:
            column = column.replace(old, new)
        new_columns.append(column)
    # 更新 DataFrame 列名
    df.columns = new_columns
    # 将 NaN 替换为 ""
    df.fillna("", inplace=True)
    # 调整个字段数据类型
    df = df[~df['Event2'].str.contains('BINFUNC')]
    df['EQUIP_Name'] = df['EQUIP_Name'].str.split('-', n=1).str[0]
    # 将日期和时间列转换为datetime类型
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['TRANSMISSION_TIME'] = pd.to_datetime(df['TRANSMISSION_TIME'])
    df['SERVER_TIME'] = pd.to_datetime(df['SERVER_TIME'])
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
            # 定义 SQL 插入语句
            sql = """
                INSERT INTO db_event_et (
                    `DATE`, `EQUIP_Name`, `Product`, `Lot_No`,
                    `Event2`, `Event3`, `TRANSMISSION_TIME`, `SERVER_TIME`
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    `DATE` = VALUES(`DATE`), `Product` = VALUES(`Product`),
                    `Lot_No` = VALUES(`Lot_No`),
                    `Event3` = VALUES(`Event3`), 
                    `SERVER_TIME` = VALUES(`SERVER_TIME`);
                """
            # 将DataFrame中的数据转换为用于插入的元组列表
            records = [tuple(x) for x in df.to_numpy()]
            print(records)
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
    file_path = r'D:\Python\RPA Common\14. ET Event Status'
    # 获取路径下所有的 .xlsx 文件
    xlsx_files = glob.glob(os.path.join(file_path, '*.xlsx'))
    for xlsx in xlsx_files:
        data = read_xls(xlsx)
        import_data(data)
        os.remove(xlsx)

if __name__ == '__main__':
    main()
    os.system('pause')