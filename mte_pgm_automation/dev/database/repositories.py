"""
数据库仓库类 - 数据访问层
"""
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import create_engine, and_, or_, not_
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from database.sqlalchemy_manager import get_db_session, execute_sql
from database.models import (
    Base, PGMMain, PGMAlarmHistory, PGMOmsHistory,
    PGMType, PGMStatus, NextTask, TATMarking, AlarmType
)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger


class BaseRepository:
    """基础仓库类"""

    def __init__(self, session: Session = None):
        """
        初始化仓库

        Args:
            session: SQLAlchemy会话，如果为None则创建新会话
        """
        self.logger = get_pgm_logger().get_logger('database')

        if session is None:
            self.session = get_db_session()
            self.own_session = True
        else:
            self.session = session
            self.own_session = False

        # 设置会话过期策略
        self.session.expire_on_commit = False

    def close(self):
        """关闭会话"""
        if self.own_session and self.session:
            try:
                self.session.close()
            except:
                pass

    def commit(self):
        """提交事务"""
        try:
            self.session.commit()
        except SQLAlchemyError as e:
            self.session.rollback()
            self.logger.error(f"数据库提交失败: {str(e)}")
            raise

    def rollback(self):
        """回滚事务"""
        try:
            self.session.rollback()
        except:
            pass

    def execute_safe_update(self, pgm_id: str, update_data: Dict[str, Any]) -> bool:
        """
        安全更新方法（解决字符集问题）

        Args:
            pgm_id: PGM ID
            update_data: 更新数据

        Returns:
            是否成功
        """
        try:
            # 使用原始SQL更新，避免字符集问题
            set_clauses = []
            params = {}

            for key, value in update_data.items():
                if value is not None:
                    set_clauses.append(f"{key} = :{key}")
                    params[key] = value
                else:
                    set_clauses.append(f"{key} = NULL")

            if not set_clauses:
                return False

            # 添加更新时间
            set_clauses.append("updated_at = NOW()")

            # 构建SQL
            sql = f"""
            UPDATE pgm_main 
            SET {', '.join(set_clauses)}
            WHERE pgm_id = :pgm_id
            """

            params['pgm_id'] = pgm_id

            # 执行SQL
            execute_sql(sql, params)

            self.logger.info(f"✅ 安全更新PGM记录: {pgm_id}")
            return True

        except Exception as e:
            self.logger.error(f"安全更新失败 ({pgm_id}): {str(e)}")
            return False


class PGMMainRepository(BaseRepository):
    """PGM主表仓库类"""

    def get_by_id(self, pgm_id: str) -> Optional[PGMMain]:
        """根据ID获取PGM"""
        try:
            return self.session.query(PGMMain).filter(PGMMain.pgm_id == pgm_id).first()
        except SQLAlchemyError as e:
            self.logger.error(f"获取PGM失败 (ID: {pgm_id}): {str(e)}")
            return None

    def get_by_status(self, status: PGMStatus) -> List[PGMMain]:
        """根据状态获取PGM列表"""
        try:
            return self.session.query(PGMMain).filter(PGMMain.status == status).all()
        except SQLAlchemyError as e:
            self.logger.error(f"根据状态获取PGM失败 ({status}): {str(e)}")
            return []

    def get_by_next_task(self, next_task: NextTask) -> List[PGMMain]:
        """根据下一个任务获取PGM列表"""
        try:
            return self.session.query(PGMMain).filter(PGMMain.next_task == next_task).all()
        except SQLAlchemyError as e:
            self.logger.error(f"根据下一个任务获取PGM失败 ({next_task}): {str(e)}")
            return []

    def get_ready_for_download(self) -> List[PGMMain]:
        """获取待下载的PGM"""
        return self.get_by_next_task(NextTask.DOWNLOAD)

    def get_ready_for_verify(self) -> List[PGMMain]:
        """获取待验证的PGM"""
        try:
            return self.session.query(PGMMain).filter(
                and_(
                    PGMMain.next_task == NextTask.VERIFY,
                    PGMMain.status == PGMStatus.DOWNLOADED
                )
            ).all()
        except SQLAlchemyError as e:
            self.logger.error(f"获取待验证PGM失败: {str(e)}")
            return []

    def get_ready_for_apply(self) -> List[PGMMain]:
        """获取待适用的PGM"""
        try:
            return self.session.query(PGMMain).filter(
                and_(
                    PGMMain.next_task == NextTask.APPLY,
                    PGMMain.apply_flag == True,
                    PGMMain.status.in_([PGMStatus.VERIFIED, PGMStatus.APPLY_READY])
                )
            ).all()
        except SQLAlchemyError as e:
            self.logger.error(f"获取待适用PGM失败: {str(e)}")
            return []

    def get_ready_for_monitor(self) -> List[PGMMain]:
        """获取待监控的PGM"""
        try:
            return self.session.query(PGMMain).filter(
                and_(
                    PGMMain.next_task == NextTask.MONITOR,
                    PGMMain.status == PGMStatus.APPLIED,
                    PGMMain.monitor_flag == False
                )
            ).all()
        except SQLAlchemyError as e:
            self.logger.error(f"获取待监控PGM失败: {str(e)}")
            return []

    def create(self, pgm_data: Dict[str, Any]) -> Optional[PGMMain]:
        """创建PGM记录（使用原始SQL避免字符集问题）"""
        try:
            # 使用原始SQL插入，避免字符集问题
            columns = []
            values = []
            params = {}

            for key, value in pgm_data.items():
                if value is not None:
                    columns.append(key)
                    values.append(f":{key}")
                    params[key] = value

            if not columns:
                return None

            columns_str = ', '.join(columns)
            values_str = ', '.join(values)

            # 移除可能重复的created_at和updated_at列
            if 'created_at' not in columns:
                columns.append('created_at')
                values.append('NOW()')
            
            if 'updated_at' not in columns:
                columns.append('updated_at')
                values.append('NOW()')
            
            columns_str = ', '.join(columns)
            values_str = ', '.join(values)
            
            sql = f"""
            INSERT INTO pgm_main ({columns_str})
            VALUES ({values_str})
            """

            # 执行插入
            execute_sql(sql, params)

            # 获取插入的记录
            pgm_id = pgm_data.get('pgm_id')
            if pgm_id:
                return self.get_by_id(pgm_id)
            else:
                return None

        except Exception as e:
            self.logger.error(f"创建PGM记录失败: {str(e)}")
            return None

    def update(self, pgm_id: str, update_data: Dict[str, Any]) -> bool:
        """更新PGM记录 - 使用安全更新方法"""
        return self.execute_safe_update(pgm_id, update_data)

    def update_status(self, pgm_id: str, status: PGMStatus, next_task: NextTask = None) -> bool:
        """更新PGM状态"""
        update_data = {'status': status.value}

        if next_task:
            update_data['next_task'] = next_task.value

        # 根据状态设置时间戳
        if status == PGMStatus.VERIFIED:
            update_data['verify_time'] = datetime.now()
        elif status == PGMStatus.APPLIED:
            update_data['apply_time'] = datetime.now()
        elif status == PGMStatus.MONITORED:
            update_data['monitor_time'] = datetime.now()

        return self.update(pgm_id, update_data)

    def set_verify_result(self, pgm_id: str, result_code: str, result_desc: str, user: str = None) -> bool:
        """设置验证结果"""
        update_data = {
            'verify_result_code': result_code,
            'verify_result_desc': result_desc,
            'verify_time': datetime.now()
        }

        if user:
            update_data['verify_user'] = user

        return self.update(pgm_id, update_data)

    def set_apply_flag(self, pgm_id: str, apply_flag: bool, user: str = None) -> bool:
        """设置适用标志"""
        update_data = {'apply_flag': apply_flag}

        if apply_flag:
            update_data['next_task'] = NextTask.APPLY.value
            if user:
                update_data['apply_user'] = user

        return self.update(pgm_id, update_data)

    def set_ftp_success(self, pgm_id: str, success: bool) -> bool:
        """设置FTP上传结果"""
        update_data = {
            'ftp_success': success,
            'apply_time': datetime.now() if success else None
        }

        if success:
            update_data['status'] = PGMStatus.APPLIED.value
            update_data['next_task'] = NextTask.MONITOR.value

        return self.update(pgm_id, update_data)

    def get_tat_timeout_pgms(self, threshold_hours: int = 72) -> List[PGMMain]:
        """获取TAT超时的PGM（使用原始SQL）"""
        try:
            sql = """
                  SELECT * \
                  FROM pgm_main
                  WHERE status != 'MONITORED' 
            AND created_at < DATE_SUB(NOW(), INTERVAL :threshold_hours HOUR)
            AND tat_marking != 'Alarm' \
                  """

            results = execute_sql(sql, {'threshold_hours': threshold_hours})

            # 将结果转换为PGMMain对象
            pgm_list = []
            for row in results:
                pgm = PGMMain()
                for key, value in row.items():
                    setattr(pgm, key, value)
                pgm_list.append(pgm)

            return pgm_list

        except Exception as e:
            self.logger.error(f"获取TAT超时PGM失败: {str(e)}")
            return []

    def update_tat_marking(self, pgm_id: str, marking: TATMarking) -> bool:
        """更新TAT标记"""
        return self.update(pgm_id, {'tat_marking': marking.value})

    def delete(self, pgm_id: str) -> bool:
        """删除PGM记录"""
        try:
            result = self.session.query(PGMMain).filter(PGMMain.pgm_id == pgm_id).delete()
            self.commit()

            if result > 0:
                self.logger.info(f"🗑️ 删除PGM记录: {pgm_id}")
                return True
            else:
                self.logger.warning(f"⚠️ PGM记录不存在，无法删除: {pgm_id}")
                return False

        except SQLAlchemyError as e:
            self.rollback()
            self.logger.error(f"❌ 删除PGM记录失败 ({pgm_id}): {str(e)}")
            return False


class PGMAlarmHistoryRepository(BaseRepository):
    """PGM报警历史仓库类"""

    def create_alarm(self, pgm_id: str, alarm_type: AlarmType, message: str) -> Optional[PGMAlarmHistory]:
        """创建报警记录"""
        try:
            alarm = PGMAlarmHistory(
                pgm_id=pgm_id,
                alarm_type=alarm_type,
                alarm_message=message
            )

            self.session.add(alarm)
            self.commit()

            self.logger.info(f"⚠️ 创建报警记录: {pgm_id} - {alarm_type}")
            return alarm

        except SQLAlchemyError as e:
            self.rollback()
            self.logger.error(f"创建报警记录失败: {str(e)}")
            return None

    def get_unresolved_alarms(self) -> List[PGMAlarmHistory]:
        """获取未解决的报警"""
        try:
            return self.session.query(PGMAlarmHistory).filter(
                PGMAlarmHistory.resolved == False
            ).order_by(PGMAlarmHistory.alarm_time.desc()).all()
        except SQLAlchemyError as e:
            self.logger.error(f"获取未解决报警失败: {str(e)}")
            return []

    def resolve_alarm(self, alarm_id: int, resolved_by: str) -> bool:
        """解决报警"""
        try:
            result = self.session.query(PGMAlarmHistory).filter(
                PGMAlarmHistory.alarm_id == alarm_id
            ).update({
                'resolved': True,
                'resolved_time': datetime.now(),
                'resolved_by': resolved_by
            })

            self.commit()
            return result > 0

        except SQLAlchemyError as e:
            self.rollback()
            self.logger.error(f"解决报警失败 ({alarm_id}): {str(e)}")
            return False


class PGMOmsHistoryRepository(BaseRepository):
    """PGM OMS历史仓库类"""

    def upsert_oms_record(self, oms_data: Dict[str, Any]) -> bool:
        """插入或更新OMS记录"""
        try:
            # 检查是否已存在
            existing = self.session.query(PGMOmsHistory).filter(
                and_(
                    PGMOmsHistory.draft_id == oms_data['draft_id'],
                    PGMOmsHistory.work_type_desc == oms_data['work_type_desc']
                )
            ).first()

            if existing:
                # 更新现有记录
                for key, value in oms_data.items():
                    setattr(existing, key, value)
                existing.fetched_at = datetime.now()
            else:
                # 插入新记录
                oms_record = PGMOmsHistory(**oms_data)
                self.session.add(oms_record)

            self.commit()
            return True

        except SQLAlchemyError as e:
            self.rollback()
            self.logger.error(f"UPSERT OMS记录失败: {str(e)}")
            return False

    def get_recent_drafts(self, days: int = 30) -> List[PGMOmsHistory]:
        """获取最近的草稿"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)

            return self.session.query(PGMOmsHistory).filter(
                PGMOmsHistory.fetched_at >= cutoff_date
            ).order_by(PGMOmsHistory.fetched_at.desc()).all()

        except SQLAlchemyError as e:
            self.logger.error(f"获取最近草稿失败: {str(e)}")
            return []