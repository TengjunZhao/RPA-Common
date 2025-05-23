import os
import time
import subprocess
import shutil
from datetime import datetime

# 配置信息
DB_CONFIG = {
    'host': 'localhost',
    'user': 'remoteuser',
    'password': 'password',
    'database': 'cmsalpha',
}
BACKUP_DIR = r'D:\backups'
TEMP_DIR = r'D:\temp_backup'
LAST_BACKUP_FILE = os.path.join(BACKUP_DIR, 'last_backup.txt')


def get_last_backup_time():
    if os.path.exists(LAST_BACKUP_FILE):
        with open(LAST_BACKUP_FILE, 'r') as f:
            return f.read().strip()
    return None


def set_last_backup_time(time_str):
    with open(LAST_BACKUP_FILE, 'w') as f:
        f.write(time_str)


def create_incremental_backup():
    os.makedirs(TEMP_DIR, exist_ok=True)
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = os.path.join(TEMP_DIR, f'incremental_{current_time}.sql')

    last_time = get_last_backup_time()
    if last_time:
        # 增量备份（需要MySQL二进制日志支持）
        cmd = [
            'mysqldump',
            f"--host={DB_CONFIG['host']}",
            f"--user={DB_CONFIG['user']}",
            f"--password={DB_CONFIG['password']}",
            f"--database={DB_CONFIG['database']}",
            f"--single-transaction",
            f"--master-data=2",
            f"--since={last_time}",
            f"--result-file={backup_file}"
        ]
    else:
        # 全量备份
        cmd = [
            'mysqldump',
            f"--host={DB_CONFIG['host']}",
            f"--user={DB_CONFIG['user']}",
            f"--password={DB_CONFIG['password']}",
            f"--database={DB_CONFIG['database']}",
            f"--single-transaction",
            f"--all-tablespaces",
            f"--result-file={backup_file}"
        ]

    subprocess.run(cmd, check=True)
    set_last_backup_time(current_time)

    # 压缩备份文件
    compressed_file = os.path.join(BACKUP_DIR, f'incremental_{current_time}.zip')
    shutil.make_archive(os.path.splitext(compressed_file)[0], 'zip', TEMP_DIR)

    # 清理临时文件
    shutil.rmtree(TEMP_DIR)

    return compressed_file


if __name__ == "__main__":
    backup_file = create_incremental_backup()
    print(f"备份完成: {backup_file}")