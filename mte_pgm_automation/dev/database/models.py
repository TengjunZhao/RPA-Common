"""
数据库模型定义 - 使用SQLAlchemy ORM
"""
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Text, Boolean, Float, JSON, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
import enum
from datetime import datetime
from utils.config_loader import get_config

Base = declarative_base()


# 枚举类型定义
class PGMType(enum.Enum):
    AT = "AT"
    ET = "ET"
    BOTH = "BOTH"


class PGMStatus(enum.Enum):
    NEW = "NEW"
    DOWNLOADED = "DOWNLOADED"
    VERIFY_FAILED = "VERIFY_FAILED"
    VERIFIED = "VERIFIED"
    APPLY_READY = "APPLY_READY"
    APPLIED = "APPLIED"
    MONITORED = "MONITORED"


class TATMarking(enum.Enum):
    Normal = "Normal"
    Notice = "Notice"
    Warning = "Warning"
    Alarm = "Alarm"


class NextTask(enum.Enum):
    DOWNLOAD = "DOWNLOAD"
    VERIFY = "VERIFY"
    APPLY = "APPLY"
    MONITOR = "MONITOR"
    NONE = "NONE"


class AlarmType(enum.Enum):
    TAT_TIMEOUT = "TAT_TIMEOUT"


class PGMMain(Base):
    """PGM主表"""
    __tablename__ = 'pgm_main'

    pgm_id = Column(String(50), primary_key=True, comment='PGM ID')
    pgm_type = Column(Enum(PGMType), nullable=False, comment='PGM类型: AT/ET/BOTH')
    status = Column(Enum(PGMStatus), default=PGMStatus.NEW, comment='PGM状态')
    server_path = Column(String(500), comment='服务器路径')
    ftp_target_path = Column(String(500), comment='FTP目标路径')
    path_details = Column(JSON, comment='路径详情')
    verify_result_code = Column(String(20), comment='验证结果代码')
    verify_result_desc = Column(String(200), comment='验证结果描述')
    verify_time = Column(DateTime, comment='验证时间')
    verify_user = Column(String(50), comment='验证用户')
    apply_flag = Column(Boolean, default=False, comment='适用标志')
    apply_time = Column(DateTime, comment='适用时间')
    apply_user = Column(String(50), comment='适用用户')
    ftp_success = Column(Boolean, default=False, comment='FTP上传成功标志')
    monitor_flag = Column(Boolean, default=False, comment='监控标志')
    monitor_time = Column(DateTime, comment='监控时间')
    monitor_yield = Column(Float(5, 2), comment='监控良率')
    monitor_test_time = Column(Float(8, 2), comment='监控测试时间')
    step1_time = Column(DateTime, comment='步骤1时间')
    step2_time = Column(DateTime, comment='步骤2时间')
    step3_time = Column(DateTime, comment='步骤3时间')
    step4_time = Column(DateTime, comment='步骤4时间')
    tat_marking = Column(Enum(TATMarking), default=TATMarking.Normal, comment='TAT标记')
    fab = Column(String(20), comment='工厂')
    tech = Column(String(20), comment='技术')
    mod_type = Column(String(50), comment='模块类型')
    grade = Column(String(20), comment='等级')
    pkg = Column(String(50), comment='封装')
    density = Column(String(20), comment='密度')
    next_task = Column(Enum(NextTask), default=NextTask.DOWNLOAD, comment='下一个任务')
    created_at = Column(DateTime, default=func.now(), comment='创建时间')
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), comment='更新时间')

    def to_dict(self):
        """转换为字典"""
        return {
            'pgm_id': self.pgm_id,
            'pgm_type': self.pgm_type.value if self.pgm_type else None,
            'status': self.status.value if self.status else None,
            'server_path': self.server_path,
            'ftp_target_path': self.ftp_target_path,
            'path_details': self.path_details,
            'verify_result_code': self.verify_result_code,
            'verify_result_desc': self.verify_result_desc,
            'verify_time': self.verify_time.isoformat() if self.verify_time else None,
            'verify_user': self.verify_user,
            'apply_flag': self.apply_flag,
            'apply_time': self.apply_time.isoformat() if self.apply_time else None,
            'apply_user': self.apply_user,
            'ftp_success': self.ftp_success,
            'monitor_flag': self.monitor_flag,
            'monitor_time': self.monitor_time.isoformat() if self.monitor_time else None,
            'monitor_yield': self.monitor_yield,
            'monitor_test_time': self.monitor_test_time,
            'step1_time': self.step1_time.isoformat() if self.step1_time else None,
            'step2_time': self.step2_time.isoformat() if self.step2_time else None,
            'step3_time': self.step3_time.isoformat() if self.step3_time else None,
            'step4_time': self.step4_time.isoformat() if self.step4_time else None,
            'tat_marking': self.tat_marking.value if self.tat_marking else None,
            'fab': self.fab,
            'tech': self.tech,
            'mod_type': self.mod_type,
            'grade': self.grade,
            'pkg': self.pkg,
            'density': self.density,
            'next_task': self.next_task.value if self.next_task else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class PGMAlarmHistory(Base):
    """PGM报警历史表"""
    __tablename__ = 'pgm_alarm_history'

    alarm_id = Column(Integer, primary_key=True, autoincrement=True, comment='报警ID')
    pgm_id = Column(String(50), nullable=False, comment='PGM ID')
    alarm_type = Column(Enum(AlarmType), nullable=False, comment='报警类型')
    alarm_time = Column(DateTime, default=func.now(), comment='报警时间')
    alarm_message = Column(Text, comment='报警消息')
    resolved = Column(Boolean, default=False, comment='是否解决')
    resolved_time = Column(DateTime, comment='解决时间')
    resolved_by = Column(String(50), comment='解决人')

    def to_dict(self):
        """转换为字典"""
        return {
            'alarm_id': self.alarm_id,
            'pgm_id': self.pgm_id,
            'alarm_type': self.alarm_type.value if self.alarm_type else None,
            'alarm_time': self.alarm_time.isoformat() if self.alarm_time else None,
            'alarm_message': self.alarm_message,
            'resolved': self.resolved,
            'resolved_time': self.resolved_time.isoformat() if self.resolved_time else None,
            'resolved_by': self.resolved_by
        }


class PGMOmsHistory(Base):
    """PGM OMS历史表"""
    __tablename__ = 'pgm_oms_history'

    draft_id = Column(String(50), primary_key=True, comment='草稿ID')
    work_type_desc = Column(String(100), primary_key=True, comment='工作类型描述')
    process_id = Column(String(50), comment='流程ID')
    work_type_no = Column(Integer, comment='工作类型编号')
    work_status = Column(String(50), comment='工作状态')
    work_start_tm = Column(String(20), comment='工作开始时间')
    complete_yn = Column(String(20), comment='完成与否')
    user_id = Column(String(50), comment='用户ID')
    user_name = Column(String(50), comment='用户名')
    fac_id = Column(String(50), comment='工厂ID')
    process_name = Column(String(255), comment='流程名称')
    process_status_code = Column(String(50), comment='流程状态代码')
    fetched_at = Column(DateTime, default=func.now(), comment='获取时间')

    def to_dict(self):
        """转换为字典"""
        return {
            'draft_id': self.draft_id,
            'work_type_desc': self.work_type_desc,
            'process_id': self.process_id,
            'work_type_no': self.work_type_no,
            'work_status': self.work_status,
            'work_start_tm': self.work_start_tm,
            'complete_yn': self.complete_yn,
            'user_id': self.user_id,
            'user_name': self.user_name,
            'fac_id': self.fac_id,
            'process_name': self.process_name,
            'process_status_code': self.process_status_code,
            'fetched_at': self.fetched_at.isoformat() if self.fetched_at else None
        }


def create_engine_from_config():
    """
    从配置创建SQLAlchemy引擎

    Returns:
        SQLAlchemy引擎实例
    """
    config = get_config().get_database_config()

    # 构建数据库URL，特别注意字符集参数
    db_url = f"mysql+pymysql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}?charset=utf8mb4"

    # 创建引擎，确保使用正确的字符集和排序规则
    engine = create_engine(
        db_url,
        pool_size=5,
        max_overflow=10,
        pool_recycle=3600,
        pool_pre_ping=True,
        echo=False,  # 设置为True可查看SQL语句（调试用）
        # 关键：在connect_args中设置排序规则
        connect_args={
            'charset': 'utf8mb4',
            'init_command': "SET NAMES utf8mb4 COLLATE utf8mb4_0900_ai_ci"
        },
        pool_use_lifo=True,
        # 设置连接参数
    )

    return engine


def create_tables(engine=None):
    """
    创建数据库表

    Args:
        engine: SQLAlchemy引擎，如果为None则从配置创建
    """
    if engine is None:
        engine = create_engine_from_config()

    try:
        Base.metadata.create_all(engine)
        print("✅ 数据库表创建成功")
    except Exception as e:
        print(f"❌ 数据库表创建失败: {str(e)}")
        raise


def drop_tables(engine=None):
    """
    删除数据库表

    Args:
        engine: SQLAlchemy引擎，如果为None则从配置创建
    """
    if engine is None:
        engine = create_engine_from_config()

    try:
        Base.metadata.drop_all(engine)
        print("✅ 数据库表删除成功")
    except Exception as e:
        print(f"❌ 数据库表删除失败: {str(e)}")
        raise