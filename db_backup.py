import pymysql
import time
import os
import subprocess
import zipfile
import datetime


def Backup_db(backup_dir, info, db, time_str):
    host = info['host']
    user = info['user']
    password = info['password']

    backup_cmd = f'mysqldump -h {host} -u {user} -p{password} {db} > "{os.path.join(backup_dir, f"{db}_{time_str}.sql")}"'
    # 数据库连接
    try:
        subprocess.run(backup_cmd, shell=True, check=True)
        print("数据库导出成功!")

    except Exception as e:
        print(f"数据库备份失败: {str(e)}")

    finally:
        pass

def compress(backup_dir, db, time_str):
    backup_file = os.path.join(backup_dir, f'{db}_{time_str}.sql')
    compressed_file = os.path.join(backup_dir, f'{db}_{time_str}.zip')
    # 创建zip文件并添加备份文件
    with zipfile.ZipFile(compressed_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(backup_file)
    try:
        os.remove(backup_file)
    finally:
        pass
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
    dir = r'D:\Sync\临时存放'
    db_list = ['cmsalpha', 'modulemte']
    timestamp = time.strftime('%Y%m%d')
    for db in db_list:
        Backup_db(dir, db_info, db, timestamp)
        compress(dir, db, timestamp)
    delete_history(30, dir)


if __name__ == '__main__':
    main()

