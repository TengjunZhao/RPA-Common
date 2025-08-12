import pandas as pd
import pymysql
from datetime import datetime, timedelta
import os


def read_xls(path):
    # 尝试读取Excel文件
    try:
        # 跳过文件中的非数据行，这需要您根据实际文件调整skiprows的值
        df = pd.read_excel(path, skiprows=10)  # 示例中跳过前10行
    except Exception as e:
        print(f"读取Excel文件时发生错误: {e}")
        exit()
    known_columns = [
        "Serial Number", "PKG Density", "Tech", "Module Density", "Hold Time(h)",
        "Occurred Oper", "OWNER", "SAP CODE", "Product Special Handling", "Occurred Date",
        "Lot ID", "Hold Code", "Device", "Module Type", "Grade", "Qty",
        "Yield", "Equip", "Abnormal Contents", "Complete Charger", "Complete Date",
        "Complete TAT(h)", "Cause", "Action Flow", "Delay Date"
    ]
    # 从df_corrected中选择这些列，忽略不存在的列
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
    df['workdt'] = df['Occurred_Date'].apply(calculate_workdt)
    df['Occurred_Date'] = pd.to_datetime(df['Occurred_Date'])
    df['Complete_Date'] = pd.to_datetime(df['Complete_Date'])
    df['Yield'] = df['Yield'].str.rstrip('%').astype('float')
    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce')
    return df


# 计算workdt字段
def calculate_workdt(occurred_date):
    if pd.isnull(occurred_date):
        return None
    occurred_datetime = pd.to_datetime(occurred_date)
    if occurred_datetime.time() < datetime.strptime('07:00:00', '%H:%M:%S').time():
        workdt = occurred_datetime - timedelta(days=1)
    else:
        workdt = occurred_datetime
    return workdt.strftime('%Y%m%d')


def import_data(df):
    # 数据库连接配置，请根据你的实际情况调整
    connection = pymysql.connect(
        host='localhost',
        user='remoteuser',
        password='password',
        database='cmsalpha',
        cursorclass=pymysql.cursors.DictCursor,
        # 增加对旧版本数据库的兼容设置
        charset='utf8',
        use_unicode=True
    )

    # 定义需要特殊处理的字段
    date_fields = ['Occurred_Date', 'Complete_Date']
    numeric_fields = ['Hold_Time', 'Qty', 'Yield', 'Complete_TAT', 'Delay_Date']

    try:
        with connection:
            with connection.cursor() as cursor:
                # 定义 SQL 插入语句
                sql = """
                    INSERT INTO db_lyld (
                        `Serial_Number`, `PKG_Density`, `Tech`, `Module_Density`,
                        `Hold_Time`, `Occurred_Oper`, `OWNER`, `SAP_CODE`, `Product_Special_Handling`,
                        `Occurred_Date`, `Lot_ID`, `Hold_Code`, `Device`, `Module_Type`,
                        `Grade`, `Qty`, `Yield`, `Equip`, `Abnormal_Contents`,
                        `Complete_Charger`, `Complete_Date`, `Complete_TAT`, `Cause`, `Action_Flow`, `Delay_Date`,`workdt`
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s
                    ) ON DUPLICATE KEY UPDATE
                        `workdt` = VALUES(`workdt`), `PKG_Density` = VALUES(`PKG_Density`), `Tech` = VALUES(`Tech`),
                        `Module_Density` = VALUES(`Module_Density`), `Hold_Time` = VALUES(`Hold_Time`),
                        `Occurred_Oper` = VALUES(`Occurred_Oper`), `OWNER` = VALUES(`OWNER`),
                        `SAP_CODE` = VALUES(`SAP_CODE`), `Product_Special_Handling` = VALUES(`Product_Special_Handling`),
                        `Occurred_Date` = VALUES(`Occurred_Date`), `Lot_ID` = VALUES(`Lot_ID`),
                        `Hold_Code` = VALUES(`Hold_Code`), `Device` = VALUES(`Device`), `Module_Type` = VALUES(`Module_Type`),
                        `Grade` = VALUES(`Grade`), `Qty` = VALUES(`Qty`), `Yield` = VALUES(`Yield`),
                        `Equip` = VALUES(`Equip`), `Abnormal_Contents` = VALUES(`Abnormal_Contents`),
                        `Complete_Charger` = VALUES(`Complete_Charger`), `Complete_Date` = VALUES(`Complete_Date`),
                        `Complete_TAT` = VALUES(`Complete_TAT`), `Cause` = VALUES(`Cause`),
                        `Action_Flow` = VALUES(`Action_Flow`), `Delay_Date` = VALUES(`Delay_Date`);
                    """

                # 处理每条记录，确保兼容性
                processed_records = []
                for idx, row in df.iterrows():
                    record = []
                    for col, value in row.items():
                        # 处理空值
                        if pd.isna(value) or value == '':
                            record.append(None)
                            continue

                        # 处理日期字段
                        if col in date_fields:
                            try:
                                # 转换为数据库兼容的日期格式
                                if isinstance(value, pd.Timestamp):
                                    record.append(value.strftime('%Y-%m-%d %H:%M:%S'))
                                else:
                                    # 尝试解析字符串日期
                                    parsed_date = pd.to_datetime(value)
                                    record.append(parsed_date.strftime('%Y-%m-%d %H:%M:%S'))
                            except:
                                # 无法解析的日期视为空值
                                record.append(None)
                            continue

                        # 处理数字字段
                        if col in numeric_fields:
                            try:
                                # 转换为浮点数，处理可能的格式问题
                                num_value = float(value)
                                record.append(num_value)
                            except:
                                # 无法转换的数字视为空值
                                record.append(None)
                            continue

                        # 其他字段直接添加，确保是字符串类型
                        record.append(str(value) if not isinstance(value, str) else value)

                    processed_records.append(tuple(record))

                # 执行插入
                row_count = 0
                for record in processed_records:
                    try:
                        cursor.execute(sql, record)
                        row_count += 1
                    except Exception as e:
                        print(f"处理记录 {row_count + 1} 时发生错误: {e}")
                        print(f"有问题的记录: {record}")
                        # 继续处理下一条记录
                        continue

                connection.commit()  # 提交事务
                print(f"数据处理完成，成功插入/更新 {row_count} 条记录。")

    except Exception as e:
        connection.rollback()  # 发生错误时回滚
        print(f"操作发生错误: {e}")
    finally:
        if connection.open:
            connection.close()


def main():
    # Excel文件路径
    file_path = r'C:\Users\Tengjun Zhao\Desktop\lyld.xlsx'
    data = read_xls(file_path)
    import_data(data)
    # os.remove(file_path)

if __name__ == '__main__':
    main()
    os.system('pause')