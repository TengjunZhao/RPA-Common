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
from database.models import get_table_schema

def main():
    env = get_config().get_current_environment()
    logger = get_pgm_logger()
    logger.info(f"✅ 当前是{env}环境")
    oms_client = OMSClient()

    # Step1 获取数据库最近的PGM时间
    with DBManager() as db:
        latest_pgm = db.get_max_value("pgm_main", "create_time")
        latest_time = latest_pgm.get('max_value') if latest_pgm.get('create_time') not in [None, '0000-00-00 00:00:00'] else ""

    # Step2 根据时间获取OMS PGM记录
    if latest_time:
        pgm_list = oms_client.get_pgm_distribution_status(latest_time)
    else:
        pgm_list = oms_client.get_pgm_distribution_status()

    # Step3 生成PGM记录并写入数据库
    with DBManager() as db:
        for pgm in pgm_list:
            # 插入pgm_oms_history
            pgm_oms_history = get_table_schema('pgm_oms_history').copy()
            
            # 将PGM数据映射到数据库字段
            pgm_oms_history.update({
                'process_id': pgm.get('processId'),
                'process_type': pgm.get('processType'),
                'process_type_desc': pgm.get('processTypeDesc'),
                'fac_id': pgm.get('facId'),
                'process_name': pgm.get('processName'),
                'draft_id': pgm.get('draftId'),
                'complete_yn': pgm.get('completeYn'),
                'process_status_code': pgm.get('processStatusCode'),
                'work_type_desc': pgm.get('workTypeDesc'),
                'work_prgs_mag_cd': pgm.get('workPrgsMagCd'),
                'work_sequence': pgm.get('workSequence'),
                'prev_work_sequence': pgm.get('prevWorkSequence'),
                'work_type': pgm.get('workType'),
                'work_status': pgm.get('workStatus'),
                'organ_name': pgm.get('organName'),
                'user_name': pgm.get('userName'),
                'user_id': pgm.get('userId'),
                'work_start_tm': pgm.get('workStartTm'),
                'bp_id': pgm.get('bpId'),
                'linked_bp_id': pgm.get('linkedBpId'),
                # work_type_no为work_type_desc '[1Step] 기안'的数字编号
                'work_type_no': re.search(r'\[(\d+)', pgm.get('workTypeDesc')).group(1) if pgm.get('workTypeDesc') else None,
            })
            
            # 检查记录是否已存在，避免重复插入
            exists_condition = "draft_id = :draft_id AND work_type_desc = :work_type_desc"
            exists_params = {
                'draft_id': pgm_oms_history['draft_id'],
                'work_type_desc': pgm_oms_history['work_type_desc']
            }
            
            if not db.record_exists('pgm_oms_history', exists_condition, exists_params):
                try:
                    result = db.insert_record('pgm_oms_history', pgm_oms_history)
                    logger.info(f"Inserted PGM record with draft_id: {pgm_oms_history['draft_id']}")
                except Exception as e:
                    logger.info(f"Failed to insert PGM record with draft_id {pgm_oms_history['draft_id']}: {str(e)}")
            else:
                logger.info(f"PGM record with draft_id {pgm_oms_history['draft_id']} and work_type_desc {pgm_oms_history['work_type_desc']} already exists, skipping.")

            # 初步更新pgm_main,新增填写基本信息
            pgm_main = get_table_schema('pgm_main').copy()
            pgm_main.update({
                'draft_id': pgm.get('draftId'),
                'process_id': pgm.get('processId'),
                'pgm_type': '',
                'status': '',
                'pgm_name': pgm.get('processName'),
                'tat': '',
            })
            exists_condition = "draft_id = :draft_id"
            exists_params = {
                'draft_id': pgm_main['draft_id']
            }
            if not db.record_exists('pgm_main', exists_condition, exists_params):
                try:
                    result = db.insert_record('pgm_main', pgm_main)
                    logger.info(f"Inserted PGM record with draft_id: {pgm_main['draft_id']}")
                except Exception as e:
                    logger.info(f"Failed to insert PGM record with draft_id {pgm_main['draft_id']}: {str(e)}")
            else:
                logger.info(
                    f"PGM record with draft_id {pgm_main['draft_id']} already exists, skipping.")
            # 细致更新pgm_main字段
            # step1时更新create_time, sk_user, pgm_type
            step = pgm_oms_history['work_type_no']
            user = pgm_oms_history['user_name']
            update_pgm_main = set()
            if step == '1':
                # 创建时间格式'%Y-%m-%d %H:%M:%S'
                create_time = pgm_oms_history['work_start_tm']
                #work_type_desc中有E/T则为ET， AT则为AT
                pgm_type = 'AT' if 'AT' in pgm_oms_history['process_type_desc'] else 'ET'
                update_pgm_main = {
                    'current_step': step,''
                    'create_time': create_time,
                    'sk_user': user,
                    'pgm_type': pgm_type,
                    'status': '0'
                }
            # step2时更新hitech_user
            elif step == '2':
                update_pgm_main = {
                    'current_step': step,
                    'hitech_user': user
                }
            # 其他情况更新current_step
            else:
                update_pgm_main = {
                    'current_step': step
                }
            # 查询pgm_main中该draft_id中 current_step最大的记录, current_step<=step时才更新
            max_step_record_result = db.get_max_value_by_condition_with_params('pgm_main','current_step','draft_id = :draft_id',{'draft_id': pgm_main['draft_id']})
            max_step_record = max_step_record_result['current_step']
            if int(max_step_record) <= int(step):
                try:
                    result = db.update_records('pgm_main', update_pgm_main,
                                              'draft_id = :draft_id',
                                              {'draft_id': pgm_main['draft_id']})
                    logger.info(f"Updated PGM record with draft_id: {pgm_main['draft_id']}")
                except Exception as e:
                    print()

    # Step4 下载PGM文件到指定文件夹
        # 从pgm_main 重新获取pgm_list
        pgm_list = db.select_records('pgm_main', "*", 'status < :status', {'status': 1})
        for pgm in pgm_list:
            download_dict = {
                'processId': pgm.get('process_id'),
                'processType': pgm.get('pgm_type'),
                'workSequence': '1'
            }
            # 文件存放文件夹
            # 获取PGM日期，创建当前日期的文件夹“YYYYMMDD”
            folderDate = pgm.get('create_time').strftime("%Y%m%d")
            # 获取config_template中file_paths
            folderRoot = get_config().get_file_paths().get('local_apply')
            # 拼接文件存放路径
            folderPath = os.path.join(folderRoot, folderDate, pgm.get('draft_id'))
            # 如果路径不存在，则创建目录
            if not os.path.exists(folderPath):
                os.makedirs(folderPath)
            pgm_detial = oms_client.download_pgm(download_dict, save_dir=folderPath)
            fileList = pgm_detial.get('file_info')


    # step5 保存hess
            hess_list = pgm_detial.get('pgm_records')
            # print(hess_list)

if __name__ == '__main__':
    main()