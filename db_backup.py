import pymysql
import time
import os
import zipfile
import datetime


def Backup_db(backup_dir, info, db, time_str):
    host = info['host']
    user = info['user']
    password = info['password']

    # 数据库连接
    try:
        # 使用with语句自动管理数据库连接的打开和关闭
        with pymysql.connect(host=host, user=user, password=password, database=db) as con, con.cursor() as cursor:
            # 获取所有数据表
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]

            # 创建备份文件
            backup_file_path = os.path.join(backup_dir, f'{db}_{time_str}.sql')
            with open(backup_file_path, 'w', encoding='utf-8') as backup_file:
                # 添加DROP TABLE语句和CREATE TABLE语句
                for table in tables:
                    cursor.execute(f"SHOW CREATE TABLE {table}")
                    create_table_sql = cursor.fetchone()[1]
                    backup_file.write(f"DROP TABLE IF EXISTS `{table}`;\n")
                    backup_file.write(f"{create_table_sql};\n")
                    cursor.execute(f"SELECT * FROM {table}")
                    table_data = cursor.fetchall()
                    if table_data:
                        for row in table_data:
                            values = [f"'{value}'" if isinstance(value, (str, datetime.datetime))
                                      else 'NULL' if value is None
                            else f"'{value}'" if isinstance(value, datetime.date)
                            else str(value) for value in row]
                            backup_file.write(f"INSERT INTO {table} VALUES ({', '.join(values)});\n")
            print("数据库导出成功!")

    except pymysql.Error as e:
        print(f"数据库备份失败: {str(e)}")

def compress(backup_dir, db, time_str):
    backup_file = os.path.join(backup_dir, f'{db}_{time_str}.sql')
    compressed_file = os.path.join(backup_dir, f'{db}_{time_str}.zip')
    # 创建zip文件并添加备份文件
    with zipfile.ZipFile(compressed_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(backup_file, os.path.basename(backup_file))
    if os.path.exists(backup_file):
        os.remove(backup_file)
    else:
        print(f"File {backup_file} does not exist.")
    print("备份文件压缩完成!")


def delete_history(num, dir):
    # 获取当前系统日期
    current_date = datetime.datetime.now()
    # 获取一周前的日期
    one_week_ago = current_date - datetime.timedelta(days=num)
    # 列出目录下所有的zip文件
    zip_files = [f for f in os.listdir(dir) if f.endswith(".zip")]
    for zip_file in zip_files:
        # 提取日期部分，假设日期部分在文件名的末尾，形如 "cmsalpha_20231007.zip"
        file_name_without_extension = os.path.splitext(zip_file)[0]
        date_part = file_name_without_extension.split("_")[-1]
        try:
            # 解析日期
            file_date = datetime.datetime.strptime(date_part, "%Y%m%d")
            # 比较日期
            if file_date < one_week_ago:
                # 删除一周前的文件
                file_path = os.path.join(dir, zip_file)
                os.remove(file_path)
                print(f"Deleted {zip_file}")
        except ValueError:
            # 如果无法解析日期，忽略该文件
            print(f"Ignored {zip_file} due to date parsing error")
    print("Finished processing zip files")


def main():
    db_info = {
        'host': 'localhost',
        'user': 'remoteuser',
        'password': 'password'
    }
    # 备份保存路径
    dir = r'D:/'
    db_list = ['cmsalpha', 'modulemte']
    timestamp = time.strftime('%Y%m%d')
    for db in db_list:
        Backup_db(dir, db_info, db, timestamp)
        compress(dir, db, timestamp)
    delete_history(30, dir)


if __name__ == '__main__':
    main()

