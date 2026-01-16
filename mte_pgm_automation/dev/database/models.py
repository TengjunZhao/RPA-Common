"""
数据库模型定义 - 统一管理所有表结构的字段映射
"""
from typing import Dict, Any

# pgm_main 表结构定义
pgm_main_schema = {
    'draft_id': '',
    'process_id': '',
    'pgm_type': '',
    'status': '',
    'pgm_name': '',
    'tat': '',
    'create_time': '',
    'current_step': '',
    'sk_user': '',
    'hitech_user': '',
}

# pgm_oms_history 表结构定义
pgm_oms_history_schema = {
    'process_id': '',  # varchar(50)
    'process_type': '',  # varchar(100)
    'process_type_desc': '',  # varchar(200)
    'fac_id': '',  # varchar(50)
    'process_name': '',  # varchar(255)
    'draft_id': '',  # varchar(50), 主键
    'complete_yn': '',  # varchar(20)
    'process_status_code': '',  # varchar(50)
    'work_type_desc': '',  # varchar(100), 主键
    'work_prgs_mag_cd': '',  # varchar(20)
    'work_sequence': 0,  # int(11)
    'prev_work_sequence': None,  # int(11)
    'work_type': '',  # varchar(50)
    'work_status': '',  # varchar(50)
    'organ_name': '',  # varchar(100)
    'user_name': '',  # varchar(50)
    'user_id': '',  # varchar(50)
    'work_start_tm': '',  # varchar(20)
    'bp_id': '',  # varchar(100)
    'linked_bp_id': '',  # varchar(100)
    'work_type_no': None  # int(4)
}

# pgm_at_hess 表结构定义
pgm_at_hess_schema = {
    'change_date_time': '',  # datetime
    'controller_name_val': '',  # varchar(20)
    'del_yn': 'N',  # char(1)
    'den_typ': '',  # varchar(20)
    'draft_id': '',  # varchar(20), 联合主键
    'draft_seq': 0,  # int(10) unsigned, 联合主键
    'drafted': 0,  # tinyint(1)
    'fab_id': '',  # varchar(20)
    'factory_id': '',  # varchar(20)
    'fmw_ver_val': '',  # varchar(20)
    'grade_code': '',  # varchar(20)
    'hdiag_dir': '',  # varchar(255)
    'mask_cd1': '',  # varchar(20)
    'mod_section_typ': '',  # varchar(20)
    'module_type': '',  # varchar(20)
    'operation_id': '',  # varchar(20)
    'organiz_cd': '',  # varchar(20)
    'pgm_dir': '',  # varchar(255)
    'pgm_id': '',  # varchar(20)
    'pgm_rev_ver': '',  # varchar(20)
    'pkg_den_typ': '',  # varchar(20)
    'pkg_typ2': '',  # varchar(20)
    'product_special_handle_value': '',  # varchar(20)
    'product_type': '',  # varchar(50), 普通索引
    'qual_opt_cd2': '',  # varchar(20)
    'sap_history_code': '',  # varchar(50)
    'special_cd': None,  # varchar(20), 允许为空
    'tech_nm': '',  # varchar(20)
    'temper_val': '',  # varchar(20)
    'test_board_id': '',  # varchar(50), 普通索引
    'timekey': '',  # varchar(20), 普通索引
    'tranmit_bp_list': None,  # varchar(255), 允许为空
    'tsv_die_typ': '',  # varchar(20)
    'ver_typ': ''  # varchar(20)
}

# pgm_et_hess 表结构定义
pgm_et_hess_schema = {
    'change_date_time': '',  # datetime
    'controller_name_val': '',  # varchar(20)
    'del_yn': 'N',  # char(1)
    'den_typ': '',  # varchar(20)
    'draft_id': '',  # varchar(50), 主键
    'draft_seq': '',
    'drafted': False,  # tinyint(1)
    'equipment_model_code': '',  # varchar(20)
    'fab_id': '',  # varchar(20)
    'factory_id': '',  # varchar(20)
    'fmw_ver_val': '',  # varchar(20)
    'grade_code': '',  # varchar(20)
    'mask_cd1': '',  # varchar(20)
    'mod_section_typ': '',  # varchar(20)
    'module_height_value': '',  # varchar(20)
    'module_type': '',  # varchar(20)
    'operation_id': '',  # varchar(20)
    'organiz_cd': '',  # varchar(20)
    'owner_code': '',  # varchar(20)
    'pgm_dir': '',  # varchar(255)
    'pgm_dir2': '',  # varchar(255)
    'pgm_dir3': '',  # varchar(255)
    'pgm_dir4': '',  # varchar(255)
    'pgm_dir5': None,  # varchar(255)
    'pgm_id': '',  # varchar(50)
    'pgm_rev_ver': '',  # varchar(20)
    'pkg_den_typ': '',  # varchar(20)
    'pkg_typ2': '',  # varchar(20)
    'product_type': '',  # varchar(50)
    'qual_opt_cd2': '',  # varchar(20)
    'sap_history_code': '',  # varchar(50)
    'special_cd': None,  # varchar(20)
    'tech_nm': '',  # varchar(20)
    'timekey': '',  # varchar(50)
    'tranmit_bp_list': None,  # varchar(255)
    'tsv_die_typ': '',  # varchar(20)
    'ver_typ': ''  # varchar(20)
}
# 数据库表结构映射字典
TABLE_SCHEMAS = {
    'pgm_main': pgm_main_schema,
    'pgm_oms_history': pgm_oms_history_schema,
    'pgm_at_hess': pgm_at_hess_schema,
    'pgm_et_hess': pgm_et_hess_schema,
}

def get_table_schema(table_name: str) -> Dict[str, Any]:
    """
    获取指定表的结构定义
    
    Args:
        table_name: 表名
        
    Returns:
        表结构定义字典，如果表不存在则返回空字典
    """
    return TABLE_SCHEMAS.get(table_name, {})

def create_empty_record(table_name: str) -> Dict[str, Any]:
    """
    创建指定表的空记录（使用默认值）
    
    Args:
        table_name: 表名
        
    Returns:
        带有默认值的空记录字典
    """
    schema = get_table_schema(table_name)
    return {field: value if isinstance(value, (str, int, float, bool)) and value != {} else (
        {} if isinstance(value, dict) else None if value != 0 and value != 0.0 else value
    ) for field, value in schema.items()}

def get_required_fields(table_name: str) -> list:
    """
    获取指定表的必填字段列表
    
    Args:
        table_name: 表名
        
    Returns:
        必填字段列表
    """
    # 这里可以根据需要定义各表的必填字段
    required_fields_map = {
        'pgm_main': ['pgm_id', 'pgm_type'],
        'db_oms_pgm': ['draft_id', 'work_type_desc']
    }
    return required_fields_map.get(table_name, [])