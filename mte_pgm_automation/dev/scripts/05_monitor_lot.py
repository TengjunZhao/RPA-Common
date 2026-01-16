import sys
import os
import time
from datetime import datetime, timedelta
import json
# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录
sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.db_manager import DBManager
from database.models import get_table_schema


def main():
    # 初始化运行条件
    env = get_config().get_current_environment()

    # 配置log
    logger = get_pgm_logger()
    logger.info(f"✅ 当前是{env}环境")

    with DBManager() as db:
        # 获取PGM_main表中current_step<4的的记录
        pgm_main_records = db.select_records('PGM_main', condition='status = :status', params={'status': 2})
        # 1. ET/AT 公共基础字段
        HESS_LOT_SCHEMA = {
            'fab_id': '',
            'grade_code': '',
            'operation_id': '',
            'pgm_rev_ver': '',
            'qual_opt_cd2': '',
            'sap_history_code': '',
        }
        HESS_DEVICE_COMMON = {
            'del_yn': 'N',
            'den_typ': '',
            'factory_id': '',
            'fmw_ver_val': '',
            'mask_cd1': '',
            'mod_section_typ': '',
            'organiz_cd': '',
            'pkg_den_typ': '',
            'pkg_typ2': '',
            'product_type': '',
            'special_cd': None,
            'tech_nm': '',
            'tsv_die_typ': '',
            'ver_typ': '' ,
            'module_type': '',
        }
        # 2. 定义各类型差异化字段
        HESS_ET_FIELD = {
            'module_height_value': '',
            'owner_code': '',
        }
        HESS_AT_FIELD = {
            'controller_name_val': '',
            'pgm_id': '',
            'product_special_handle_value': '',
            'temper_val': '',
            'test_board_id': '',
            'tranmit_bp_list': None,
        }

        for pgm in pgm_main_records:
            draft_id = pgm.get('draft_id')
            pgm_type = pgm.get('pgm_type')

            # 3. 统一赋值逻辑 + 容错兜底
            pgm_schema = {}
            if pgm_type == 'ET':
                pgm_schema = {**HESS_LOT_SCHEMA, **HESS_DEVICE_COMMON, **HESS_ET_FIELD}
            elif pgm_type == 'AT':
                pgm_schema = {**HESS_LOT_SCHEMA, **HESS_DEVICE_COMMON, **HESS_AT_FIELD}
            else:
                pgm_schema = {}
            # 获取PGM HESS
            bdName = f"pgm_{pgm_type.lower()}_hess"
            # 获取HESS
            hess_records = db.select_records(bdName, condition='draft_id = :draft_id', params={'draft_id': draft_id})
            for hess in hess_records:
                # 检索hess的内容，赋值给pgm_schema
                for key, value in hess.items():
                    if key in pgm_schema:
                        pgm_schema[key] = value
                pgm_filtered_dict = {
                    key: value
                    for key, value in pgm_schema.items()
                    if value not in ['', '*'] and value is not None
                }
                print(pgm_filtered_dict)
                pgm_main_records = db.select_records('modulemte.db_deviceinfo', condition='Device = :device', params={'device': 'HMAG56DXNSX051N-1C101'})
                print(pgm_main_records)



if __name__ == '__main__':
    main()