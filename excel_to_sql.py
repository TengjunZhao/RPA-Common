import os
import glob
import pymysql
from openpyxl import load_workbook
import re
from datetime import datetime
from datetime import timedelta



def main():
    # 配置数据库连接
    db_config = {
        'host': '172.27.154.57',
        'user': 'remoteuser',
        'password': 'password',
        'database': 'modulemte'
    }

    # 建立数据库连接
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # 指定文件夹路径
    folder_path = r'D:\Sync\临时存放'

    # 获取路径下所有的 .xlsx 文件
    xlsx_files = glob.glob(os.path.join(folder_path, '*.xlsx'))

    for xlsx_file in xlsx_files:
        workbook = load_workbook(filename=xlsx_file, read_only=True)
        sheet = workbook.active

        for row in sheet.iter_rows(min_row=4, values_only=True):

            # 提取equip_id中的部分
            sk_hynix_charger = re.sub(r'/.*','', row[2])
            # 确认B_id
            current_date = datetime.now().strftime('%Y%m%d')
            # 获取当前日期的起始和结束时间
            start_time = datetime.strptime(current_date, '%Y%m%d')
            end_time = start_time + timedelta(days=1)
            print(start_time, end_time)
            cursor.execute(f"SELECT MAX(B_id) FROM bus_detail "
                           f"WHERE Occur_Time >= %s AND Occur_Time < %s", (start_time, end_time))
            max_bid = cursor.fetchone()[0]

            # 如果没有找到最大的B_id，则将其设置为0
            if max_bid is None:
                max_bid = 0
                # 生成新的B_id，将最后三位自增
                max_bid_int = int(max_bid)
                max_bid = str(max_bid_int + 1).zfill(3)
                new_bid = f"{current_date}{max_bid}"
            else:
                max_bid_int = int(max_bid)
                new_bid = str(max_bid_int + 1)


            # 写入db_pgm表格
            insert_query = """
            INSERT INTO db_pgm (time_send , sk_hynix_charger, pgm_name, dir_local, 
            dir_src, pgm_id, time_down, time_get, time_apply, charger)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_data = (row[1], sk_hynix_charger, row[3], row[4], row[5], row[6], row[7], "", "", "z130157")
            cursor.execute(insert_query, insert_data)

            # 任务添加模块
            user_id = task_generator(row[3])
            insert_query = """
            INSERT INTO bus_detail (B_id , B_Category, Occur_Time, Description, 
            Solution, user_id, B_Status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            insert_data_task = (new_bid, "6", row[7], row[6], row[3], user_id, "1")
            cursor.execute(insert_query, insert_data_task)

            connection.commit()
            # 写任务
        workbook.close()
        os.remove(xlsx_file)

    # 关闭数据库连接
    cursor.close()
    connection.close()


def task_generator(my_string):
    string_lower = my_string.lower()
    print(string_lower)
    task_mapping = {
        'et': 'z130157',
        'ern': 'z130157',
        'rd': 'z110160',
        'sd': 'z110447',
        'ud': 'z110447',
        'server': 'z110160',
        'client': 'z110447',
        'bios' : 'z110447',
        'unisdk': 'z130157',
        'smart': 'z110447'
    }
    for key in task_mapping:
        if key in string_lower:
            return task_mapping[key]

    return 'z130157'  # 如果没有匹配项，则返回默认值


if __name__ == '__main__':
    print(task_generator('DA16GD5 SDP X4,X8 RD& QD ARN REVISION'))