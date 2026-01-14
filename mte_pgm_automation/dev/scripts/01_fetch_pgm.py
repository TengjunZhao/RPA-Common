import requests
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Union
from urllib.parse import urljoin
import time
import hashlib
import sys
import os
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录
sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from core.oms_client import OMSClient
from database.db_manager import DBManager

def main():
    print("Testing 01_fetch_pgm...")
    oms_client = OMSClient()
    # 示例：获取PGM分发状态
    # pgm_list = oms_client.get_pgm_distribution_status()
    # print(f"Retrieved {len(pgm_list)} PGM records")

    # Step1 获取数据库最近的PGM时间
    with DBManager() as db:
        latest_pgm = db.get_max_value("pgm_main", "created_at")
        latest_time = latest_pgm.get('step1_time') if latest_pgm.get('step1_time') else ""
    # Step2 根据时间获取OMS PGM记录

    # Step3 生成PGM记录并写入数据库

    # Step4 下载PGM文件到指定文件夹

if __name__ == '__main__':
    main()