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
    # 获取config文件中的Alarm条件
    alarm_conditions = get_config().get_alarm_settings()
    logger = get_pgm_logger()
    logger.info(f"✅ 当前是{env}环境")

    # pgm_main表中查询current_step<4的记录，计算TAT并判断Alarm标志
    with DBManager() as db:
        # 获取PGM_main表中current_step<4的的记录
        pgm_main_records = db.select_records('PGM_main', condition='current_step < :step', params={'step': 4})
        for pgm in pgm_main_records:
            create_time = pgm.get('create_time')
            # 计算当前时间与create_time的差值，以小时计算，精确到小数点后2位
            tat = round((datetime.now() - create_time).total_seconds() / 3600, 2)
            logger.info(f"✅ PGM {pgm.get('draft_id')} 创建时间：{create_time}，已运行时间：{tat}小时")
            # 与config对比是否满足报警条件
            alarm = 3 if tat >= alarm_conditions.get('alarm_hours', 72) else \
                    2 if tat >= alarm_conditions.get('warning_hours', 48) else \
                    1 if tat >= alarm_conditions.get('notice_hours', 24) else 0
            # 将TAT，Alarm回写进数据库中
            db.update_records('PGM_main',
                             {'tat': tat, 'alarm': alarm},
                             condition='draft_id = :draft_id', params={'draft_id': pgm.get('draft_id')})


if __name__ == '__main__':
    main()