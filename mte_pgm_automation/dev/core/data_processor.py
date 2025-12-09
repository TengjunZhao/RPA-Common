"""
数据处理器 - 完整版本（包含文件下载）
"""
import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import time
import shutil
import requests

from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.models import PGMMain, PGMStatus, NextTask, PGMOmsHistory
from database.repositories import PGMMainRepository, PGMOmsHistoryRepository
from core.oms_client import OMSClient, OMSDataProcessor

"""
数据处理器 - 负责从OMS数据创建PGM记录并下载文件
"""
import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
import time
import shutil

from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.models import PGMMain, PGMStatus, NextTask, PGMOmsHistory
from database.repositories import PGMMainRepository, PGMOmsHistoryRepository
from core.oms_client import OMSClient


class DataProcessor:
    """OMS数据处理和PGM记录创建器"""

    def __init__(self, oms_client: OMSClient = None):
        """初始化数据处理器"""
        self.logger = get_pgm_logger().get_logger('data_processor')
        self.config = get_config()
        self.file_paths = self.config.get_file_paths()
        self.oms_client = oms_client or OMSClient()
        self.pgm_repo = PGMMainRepository()
        self.oms_repo = PGMOmsHistoryRepository()

        self.logger.info("🔧 数据处理器初始化完成")

    def fetch_and_process_new_pgms(self) -> Dict[str, any]:
        """
        获取并处理新PGM（完整流程）

        Returns:
            处理结果统计
        """
        try:
            self.logger.info("🔄 开始完整PGM处理流程")

            results = {
                'total_drafts': 0,
                'new_pgms_created': 0,
                'existing_pgms_updated': 0,
                'files_downloaded': 0,
                'download_errors': 0,
                'process_errors': 0,
                'pgm_details': []
            }

            # 1. 从OMS获取所有草稿ID
            all_drafts = self._get_all_draft_ids_from_oms()
            results['total_drafts'] = len(all_drafts)

            if not all_drafts:
                self.logger.warning("⚠️ 未从OMS获取到任何草稿ID")
                return results

            self.logger.info(f"📊 从OMS获取到 {len(all_drafts)} 个草稿ID")

            # 2. 处理每个草稿ID
            for draft_id in all_drafts:
                try:
                    draft_result = self._process_single_draft(draft_id)
                    results['pgm_details'].append(draft_result)

                    if draft_result['success']:
                        if draft_result['action'] == 'created':
                            results['new_pgms_created'] += 1
                        elif draft_result['action'] == 'updated':
                            results['existing_pgms_updated'] += 1

                        results['files_downloaded'] += draft_result['files_downloaded']
                        results['download_errors'] += draft_result['download_errors']
                    else:
                        results['process_errors'] += 1

                except Exception as e:
                    self.logger.error(f"❌ 处理草稿失败 ({draft_id}): {str(e)}")
                    results['process_errors'] += 1
                    results['pgm_details'].append({
                        'draft_id': draft_id,
                        'success': False,
                        'error': str(e)
                    })

            # 3. 汇总结果
            self.logger.info(
                f"📊 完整处理完成 - 新建PGM: {results['new_pgms_created']}, 文件下载: {results['files_downloaded']}, 错误: {results['process_errors']}")

            return results

        except Exception as e:
            self.logger.error(f"❌ 完整处理流程失败: {str(e)}")
            return {'total_drafts': 0, 'new_pgms_created': 0, 'process_errors': 1, 'error': str(e)}

    def _get_all_draft_ids_from_oms(self) -> List[str]:
        """从OMS获取所有草稿ID"""
        try:
            # 使用现有的OMS客户端获取分发状态
            oms_data = self.oms_client.get_pgm_distribution_status()

            if not oms_data:
                return []

            # 提取唯一的draft_id
            draft_ids = set()
            for item in oms_data:
                draft_id = item.get('draftId')
                if draft_id:
                    draft_ids.add(str(draft_id))

            # 转换为列表并排序
            sorted_drafts = sorted(list(draft_ids))

            self.logger.info(f"📋 提取到 {len(sorted_drafts)} 个唯一草稿ID")
            return sorted_drafts

        except Exception as e:
            self.logger.error(f"❌ 获取草稿ID失败: {str(e)}")
            return []

    def _process_single_draft(self, draft_id: str) -> Dict[str, any]:
        """
        处理单个草稿

        Args:
            draft_id: 草稿ID

        Returns:
            处理结果
        """
        self.logger.info(f"🔍 处理草稿: {draft_id}")

        result = {
            'draft_id': draft_id,
            'success': False,
            'action': 'skipped',
            'pgm_id': None,
            'pgm_type': None,
            'files_downloaded': 0,
            'download_errors': 0,
            'server_path': None,
            'message': ''
        }

        try:
            # 1. 检查服务器是否已存在该PGM
            server_path = self._get_server_path_for_draft(draft_id)
            result['server_path'] = server_path

            if os.path.exists(server_path) and os.listdir(server_path):
                # 目录已存在且非空，跳过
                result['success'] = True
                result['action'] = 'skipped'
                result['message'] = '服务器已存在该PGM文件'
                self.logger.info(f"⏭️ 跳过已存在的PGM: {draft_id}")
                return result

            # 2. 确定PGM类型（需要从OMS获取详细信息）
            pgm_type = self._determine_pgm_type_for_draft(draft_id)
            result['pgm_type'] = pgm_type

            # 3. 生成PGM ID
            pgm_id = self._generate_pgm_id(draft_id, pgm_type)
            result['pgm_id'] = pgm_id

            # 4. 更新服务器路径
            server_path = self._create_server_path(pgm_id)
            result['server_path'] = server_path

            # 5. 创建或更新PGM记录
            existing_pgm = self.pgm_repo.get_by_id(pgm_id)

            if existing_pgm:
                # 更新现有记录
                updated = self._update_existing_pgm(existing_pgm, draft_id, pgm_type)
                result['action'] = 'updated' if updated else 'update_failed'
            else:
                # 创建新记录
                created = self._create_new_pgm(pgm_id, pgm_type, draft_id, server_path)
                result['action'] = 'created' if created else 'create_failed'

            if result['action'] in ['created', 'updated']:
                result['success'] = True

                # 6. 下载文件
                download_result = self._download_files_for_draft(draft_id, server_path)
                result['files_downloaded'] = download_result['downloaded']
                result['download_errors'] = download_result['errors']

                if download_result['downloaded'] > 0:
                    result['message'] = f'成功处理，下载{download_result["downloaded"]}个文件'
                else:
                    result['message'] = '成功处理，但未下载到文件'

            return result

        except Exception as e:
            self.logger.error(f"❌ 处理草稿失败 ({draft_id}): {str(e)}")
            result['message'] = f'处理失败: {str(e)}'
            return result

    def _get_server_path_for_draft(self, draft_id: str) -> str:
        """获取草稿的服务器路径"""
        base_path = self.file_paths.get('local_verify', '')
        if not base_path:
            raise ValueError("未配置本地验证路径")

        # 使用日期子目录
        date_str = datetime.now().strftime('%Y%m%d')
        draft_path = os.path.join(base_path, date_str, draft_id)

        return draft_path

    def _determine_pgm_type_for_draft(self, draft_id: str) -> str:
        """为草稿确定PGM类型"""
        # 这里需要调用OMS API获取详细信息来确定类型
        # 暂时返回默认值，实际需要实现

        # 尝试从数据库查找历史记录
        try:
            # 从pgm_oms_history查找该draft_id的记录
            session = self.oms_repo.session
            records = session.query(PGMOmsHistory).filter(
                PGMOmsHistory.draft_id == draft_id
            ).all()

            if records:
                for record in records:
                    process_name = record.process_name or ''
                    if 'ET' in process_name.upper():
                        return 'ET'
                    elif 'AT' in process_name.upper():
                        return 'AT'
        except:
            pass

        # 默认返回ET
        return 'ET'

    def _process_single_oms_record(self, oms_record: PGMOmsHistory) -> Dict[str, any]:
        """
        处理单个OMS记录

        Args:
            oms_record: OMS历史记录

        Returns:
            处理结果
        """
        draft_id = oms_record.draft_id
        work_type_desc = oms_record.work_type_desc

        self.logger.info(f"🔍 处理OMS记录: {draft_id} - {work_type_desc}")

        result = {
            'draft_id': draft_id,
            'work_type_desc': work_type_desc,
            'success': False,
            'action': 'skipped',
            'pgm_id': None,
            'pgm_type': None,
            'files_downloaded': 0,
            'message': ''
        }

        try:
            # 1. 检查是否应该处理此记录
            if not self._should_process_record(oms_record):
                result['message'] = '记录不符合处理条件'
                self.logger.info(f"⏭️ 跳过记录: {draft_id} - {result['message']}")
                return result

            # 2. 确定PGM类型（ET/AT）
            pgm_type = self._determine_pgm_type(oms_record)
            result['pgm_type'] = pgm_type

            # 3. 生成PGM ID
            pgm_id = self._generate_pgm_id(draft_id, pgm_type, oms_record)
            result['pgm_id'] = pgm_id

            # 4. 检查PGM是否已存在
            existing_pgm = self.pgm_repo.get_by_id(pgm_id)

            if existing_pgm:
                # 更新现有记录
                updated = self._update_existing_pgm(existing_pgm, oms_record)
                result['action'] = 'updated' if updated else 'update_failed'
            else:
                # 创建新记录
                created = self._create_new_pgm(pgm_id, pgm_type, oms_record)
                result['action'] = 'created' if created else 'create_failed'

            if result['action'] in ['created', 'updated']:
                result['success'] = True

                # 5. 下载文件（如果适用）
                if self._should_download_files(oms_record):
                    files_downloaded = self._download_attached_files(oms_record, pgm_id)
                    result['files_downloaded'] = files_downloaded
                    result['message'] = f'成功处理，下载{files_downloaded}个文件'
                else:
                    result['message'] = '成功处理，无需下载文件'

            return result

        except Exception as e:
            self.logger.error(f"❌ 处理单个记录失败 ({draft_id}): {str(e)}")
            result['message'] = f'处理失败: {str(e)}'
            return result

    def _get_new_oms_records(self, processed_draft_ids: set) -> List[PGMOmsHistory]:
        """获取新的OMS记录"""
        try:
            # 获取今天的所有记录
            today = datetime.now().strftime('%Y-%m-%d')
            all_records = self.oms_repo.get_recent_drafts(days=1)

            # 过滤掉已处理的
            new_records = [r for r in all_records if r.draft_id not in processed_draft_ids]

            # 按工作类型排序，优先处理起草阶段的记录
            new_records.sort(key=lambda x: x.work_type_no or 0)

            return new_records

        except Exception as e:
            self.logger.error(f"❌ 获取新OMS记录失败: {str(e)}")
            return []

    def _process_single_oms_record(self, oms_record: PGMOmsHistory) -> Dict[str, any]:
        """
        处理单个OMS记录

        Args:
            oms_record: OMS历史记录

        Returns:
            处理结果
        """
        draft_id = oms_record.draft_id
        work_type_desc = oms_record.work_type_desc

        self.logger.info(f"🔍 处理OMS记录: {draft_id} - {work_type_desc}")

        result = {
            'draft_id': draft_id,
            'work_type_desc': work_type_desc,
            'success': False,
            'action': 'skipped',
            'pgm_id': None,
            'pgm_type': None,
            'files_downloaded': 0,
            'message': ''
        }

        try:
            # 1. 检查是否应该处理此记录
            if not self._should_process_record(oms_record):
                result['message'] = '记录不符合处理条件'
                self.logger.info(f"⏭️ 跳过记录: {draft_id} - {result['message']}")
                return result

            # 2. 确定PGM类型（ET/AT）
            pgm_type = self._determine_pgm_type(oms_record)
            result['pgm_type'] = pgm_type

            # 3. 生成PGM ID
            pgm_id = self._generate_pgm_id(draft_id, pgm_type, oms_record)
            result['pgm_id'] = pgm_id

            # 4. 检查PGM是否已存在
            existing_pgm = self.pgm_repo.get_by_id(pgm_id)

            if existing_pgm:
                # 更新现有记录
                updated = self._update_existing_pgm(existing_pgm, oms_record)
                result['action'] = 'updated' if updated else 'update_failed'
            else:
                # 创建新记录
                created = self._create_new_pgm(pgm_id, pgm_type, oms_record)
                result['action'] = 'created' if created else 'create_failed'

            if result['action'] in ['created', 'updated']:
                result['success'] = True

                # 5. 下载文件（如果适用）
                if self._should_download_files(oms_record):
                    files_downloaded = self._download_attached_files(oms_record, pgm_id)
                    result['files_downloaded'] = files_downloaded
                    result['message'] = f'成功处理，下载{files_downloaded}个文件'
                else:
                    result['message'] = '成功处理，无需下载文件'

            return result

        except Exception as e:
            self.logger.error(f"❌ 处理单个记录失败 ({draft_id}): {str(e)}")
            result['message'] = f'处理失败: {str(e)}'
            return result

    def _should_process_record(self, oms_record: PGMOmsHistory) -> bool:
        """检查是否应该处理此记录"""
        try:
            # 检查必要字段
            if not oms_record.draft_id:
                return False

            # 检查工作类型（只处理特定类型）
            work_type_desc = oms_record.work_type_desc or ''
            process_name = oms_record.process_name or ''

            # 只处理包含"PGM"或"Dram"关键字的记录
            keywords = ['PGM', 'DRAM', 'Dram']
            has_keyword = any(keyword in process_name.upper() for keyword in keywords)

            if not has_keyword:
                self.logger.debug(f"⏭️ 跳过非PGM记录: {process_name}")
                return False

            # 检查完成状态（根据业务需求调整）
            complete_yn = oms_record.complete_yn or ''
            if complete_yn == '완료':  # 韩语的"完成"
                return True
            elif complete_yn == '진행 중':  # 韩语的"进行中"
                return True

            return False

        except Exception as e:
            self.logger.error(f"❌ 检查记录处理条件失败: {str(e)}")
            return False

    def _determine_pgm_type(self, oms_record: PGMOmsHistory) -> str:
        """确定PGM类型"""
        try:
            process_name = oms_record.process_name or ''
            process_name_upper = process_name.upper()

            if 'ET' in process_name_upper:
                return 'ET'
            elif 'AT' in process_name_upper:
                return 'AT'
            elif 'DRAM' in process_name_upper:
                # 默认返回ET，实际需要更精确的判断
                return 'ET'
            else:
                # 通过其他方式判断
                return self._guess_pgm_type_by_context(oms_record)

        except Exception as e:
            self.logger.error(f"❌ 确定PGM类型失败: {str(e)}")
            return 'ET'  # 默认返回ET

    def _guess_pgm_type_by_context(self, oms_record: PGMOmsHistory) -> str:
        """通过上下文猜测PGM类型"""
        # 这里可以根据更多信息进行判断
        # 例如：用户部门、工厂ID等

        fac_id = oms_record.fac_id or ''
        user_id = oms_record.user_id or ''

        # 简单的启发式规则
        if 'ET' in user_id.upper() or 'ET' in fac_id.upper():
            return 'ET'
        elif 'AT' in user_id.upper() or 'AT' in fac_id.upper():
            return 'AT'
        else:
            return 'ET'  # 默认

    def _generate_pgm_id(self, draft_id: str, pgm_type: str) -> str:
        """生成PGM ID"""
        # 使用draft_id和类型组合
        return f"{pgm_type}_{draft_id}"

    def _create_new_pgm(self, pgm_id: str, pgm_type: str, draft_id: str, server_path: str) -> bool:
        """创建新的PGM记录"""
        try:
            # 创建PGM数据
            pgm_data = {
                'pgm_id': pgm_id,
                'pgm_type': pgm_type,
                'status': PGMStatus.NEW.value,
                'server_path': server_path,
                'next_task': NextTask.VERIFY.value,  # 下载后直接验证
                'fab': '*',
                'tech': '*',
                'mod_type': '*',
                'grade': '*',
                'pkg': '*',
                'density': '*'
            }

            # 添加路径详情
            path_details = {
                'draft_id': draft_id,
                'source': 'OMS',
                'created_at': datetime.now().isoformat()
            }
            pgm_data['path_details'] = json.dumps(path_details, ensure_ascii=False)

            # 保存到数据库
            pgm = self.pgm_repo.create(pgm_data)

            if pgm:
                self.logger.info(f"✅ 创建新PGM记录: {pgm_id}")
                return True
            else:
                self.logger.error(f"❌ 创建PGM记录失败: {pgm_id}")
                return False

        except Exception as e:
            self.logger.error(f"❌ 创建新PGM失败 ({pgm_id}): {str(e)}")
            return False

    def _update_existing_pgm(self, existing_pgm: PGMMain, draft_id: str, pgm_type: str) -> bool:
        """更新现有PGM记录"""
        try:
            # 更新状态为需要验证
            update_data = {
                'status': PGMStatus.DOWNLOADED.value,
                'next_task': NextTask.VERIFY.value,
                'updated_at': datetime.now()
            }

            # 如果服务器路径为空，更新它
            if not existing_pgm.server_path:
                server_path = self._create_server_path(existing_pgm.pgm_id)
                update_data['server_path'] = server_path

            # 执行更新
            updated = self.pgm_repo.update(existing_pgm.pgm_id, update_data)

            if updated:
                self.logger.info(f"✅ 更新PGM记录: {existing_pgm.pgm_id}")
            else:
                self.logger.warning(f"⚠️ 更新PGM记录无变化: {existing_pgm.pgm_id}")

            return updated

        except Exception as e:
            self.logger.error(f"❌ 更新PGM记录失败 ({existing_pgm.pgm_id}): {str(e)}")
            return False

    def _create_server_path(self, pgm_id: str) -> str:
        """创建服务器路径"""
        try:
            base_path = self.file_paths.get('local_verify', '')
            if not base_path:
                raise ValueError("未配置本地验证路径")

            # 使用日期子目录
            date_str = datetime.now().strftime('%Y%m%d')
            pgm_path = os.path.join(base_path, date_str, pgm_id)

            # 创建目录
            os.makedirs(pgm_path, exist_ok=True)

            self.logger.debug(f"📁 创建服务器路径: {pgm_path}")
            return pgm_path

        except Exception as e:
            self.logger.error(f"❌ 创建服务器路径失败: {str(e)}")
            return ''

    def _extract_product_info(self, oms_record: PGMOmsHistory) -> Dict[str, str]:
        """从OMS记录中提取产品信息"""
        product_info = {}

        try:
            process_name = oms_record.process_name or ''

            # 从流程名称中提取信息（根据实际命名规则）
            # 示例: "CP16G RD ET PGM Release"
            patterns = [
                (r'(\w+)G', 'density'),  # 密度: 16G, 32G
                (r'(\w+)\s+(RD|QD)', 'mod_type'),  # 模块类型: RD, QD
                (r'(CP|LC|HC|DP)', 'tech'),  # 技术: CP, LC
                (r'(\w+)\s+DIMM', 'product_type'),  # 产品类型
            ]

            for pattern, field in patterns:
                match = re.search(pattern, process_name, re.IGNORECASE)
                if match:
                    product_info[field] = match.group(1).upper()

            # 设置默认值
            product_info.setdefault('fab', '*')
            product_info.setdefault('tech', '*')
            product_info.setdefault('mod_type', '*')
            product_info.setdefault('grade', '*')
            product_info.setdefault('pkg', '*')
            product_info.setdefault('density', '*')

            self.logger.debug(f"📋 提取产品信息: {product_info}")

        except Exception as e:
            self.logger.error(f"❌ 提取产品信息失败: {str(e)}")
            product_info = {
                'fab': '*', 'tech': '*', 'mod_type': '*',
                'grade': '*', 'pkg': '*', 'density': '*'
            }

        return product_info

    def _should_download_files(self, oms_record: PGMOmsHistory) -> bool:
        """检查是否应该下载文件"""
        # 这里可以根据工作类型、状态等决定
        work_type_desc = oms_record.work_type_desc or ''

        # 只在前几个步骤下载文件
        download_stages = ['[1Step] 기안', '[2Step] 외주사 결과']

        return work_type_desc in download_stages

    def _download_attached_files(self, oms_record: PGMOmsHistory, pgm_id: str) -> int:
        """下载附件文件"""
        # 注意：这里需要实际的process_id和file_download_id
        # 当前只能模拟下载

        self.logger.info(f"📥 需要下载文件，但缺少具体文件信息: {pgm_id}")
        return 0  # 模拟下载0个文件

    def cleanup_old_data(self, days_to_keep: int = 30) -> Dict[str, int]:
        """清理旧数据"""
        try:
            self.logger.info(f"🧹 开始清理 {days_to_keep} 天前的数据")

            cutoff_date = datetime.now() - timedelta(days=days_to_keep)

            # 清理旧的PGM记录（根据状态）
            old_pgms = self.pgm_repo.session.query(PGMMain).filter(
                PGMMain.created_at < cutoff_date,
                PGMMain.status.in_([PGMStatus.MONITORED, PGMStatus.VERIFY_FAILED])
            ).all()

            deleted_pgms = 0
            for pgm in old_pgms:
                # 删除对应的文件
                if pgm.server_path and os.path.exists(pgm.server_path):
                    try:
                        shutil.rmtree(pgm.server_path)
                        self.logger.debug(f"🗑️ 删除文件目录: {pgm.server_path}")
                    except Exception as e:
                        self.logger.error(f"❌ 删除文件目录失败: {str(e)}")

                # 删除数据库记录
                self.pgm_repo.delete(pgm.pgm_id)
                deleted_pgms += 1

            # 清理旧的OMS记录
            old_oms_records = self.oms_repo.session.query(PGMOmsHistory).filter(
                PGMOmsHistory.fetched_at < cutoff_date
            ).delete()

            self.pgm_repo.commit()

            self.logger.info(f"✅ 数据清理完成 - 删除PGM记录: {deleted_pgms}, OMS记录: {old_oms_records}")

            return {
                'deleted_pgms': deleted_pgms,
                'deleted_oms_records': old_oms_records
            }

        except Exception as e:
            self.logger.error(f"❌ 数据清理失败: {str(e)}")
            return {'deleted_pgms': 0, 'deleted_oms_records': 0}

    def _download_files_for_draft(self, draft_id: str, server_path: str) -> Dict[str, int]:
        """为草稿下载文件"""
        result = {
            'downloaded': 0,
            'errors': 0,
            'files': []
        }

        try:
            self.logger.info(f"📥 开始为草稿 {draft_id} 下载文件")

            # 1. 获取process_id和work_sequence
            process_info = self._get_process_info_for_draft(draft_id)
            if not process_info:
                self.logger.warning(f"⚠️ 无法获取process信息: {draft_id}")
                return result

            process_id = process_info['process_id']
            work_sequence = process_info['work_sequence']

            if not process_id:
                self.logger.warning(f"⚠️ process_id为空: {draft_id}")
                return result

            # 2. 获取文件下载信息
            file_info_list = self._get_file_info_for_process(process_id, work_sequence)

            if not file_info_list:
                self.logger.warning(f"⚠️ 未找到文件下载信息: {draft_id}")
                return result

            # 3. 下载每个文件
            for file_info in file_info_list:
                try:
                    file_download_id = file_info.get('file_download_id')
                    file_name = file_info.get('file_name')
                    file_size = file_info.get('size', 0)

                    if not file_download_id or not file_name:
                        self.logger.warning(f"⚠️ 文件信息不完整: {file_info}")
                        continue

                    # 构建保存路径
                    save_path = os.path.join(server_path, file_name)

                    # 下载文件 - 使用OMSClient的download_file方法
                    self.logger.info(f"📥 下载文件: {file_name} (ID: {file_download_id})")

                    download_success = self.oms_client.download_file(
                        file_download_id=file_download_id,
                        bpms_process_id=process_id,
                        bpms_work_sequence=work_sequence,
                        save_path=save_path
                    )

                    if download_success:
                        # 验证文件是否成功下载
                        if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                            actual_size = os.path.getsize(save_path)
                            result['downloaded'] += 1
                            result['files'].append({
                                'name': file_name,
                                'expected_size': file_size,
                                'actual_size': actual_size,
                                'path': save_path
                            })
                            self.logger.info(f"✅ 下载文件成功: {file_name} ({actual_size}/{file_size} bytes)")
                        else:
                            result['errors'] += 1
                            self.logger.error(f"❌ 文件下载后验证失败: {save_path}")
                    else:
                        result['errors'] += 1
                        self.logger.error(f"❌ 下载文件失败: {file_name}")

                except Exception as e:
                    result['errors'] += 1
                    self.logger.error(f"❌ 处理文件失败: {str(e)}")

            self.logger.info(f"📊 文件下载完成: {draft_id} - 成功{result['downloaded']}个, 失败{result['errors']}个")

            return result

        except Exception as e:
            self.logger.error(f"❌ 下载文件失败 ({draft_id}): {str(e)}")
            result['errors'] += 1
            return result

    def _get_file_info_for_process(self, process_id: str, work_sequence: int) -> List[Dict[str, any]]:
        """获取流程的文件信息"""
        file_info_list = []

        try:
            # 1. 尝试获取ET文件信息
            et_details = self.oms_client.get_et_pgm_details(process_id, work_sequence)
            if et_details and 'file_info' in et_details:
                for file_info in et_details['file_info']:
                    file_info['pgm_type'] = 'ET'
                    file_info['process_id'] = process_id
                    file_info['work_sequence'] = work_sequence
                    file_info_list.append(file_info)

            # 2. 尝试获取AT文件信息
            at_details = self.oms_client.get_at_pgm_details(process_id, work_sequence)
            if at_details and 'file_info' in at_details:
                for file_info in at_details['file_info']:
                    file_info['pgm_type'] = 'AT'
                    file_info['process_id'] = process_id
                    file_info['work_sequence'] = work_sequence
                    file_info_list.append(file_info)

            self.logger.debug(f"📋 获取到 {len(file_info_list)} 个文件下载信息")

        except Exception as e:
            self.logger.error(f"❌ 获取文件信息失败: {str(e)}")

        return file_info_list

    def _get_process_info_for_draft(self, draft_id: str) -> Optional[Dict[str, any]]:
        """获取草稿的process信息"""
        try:
            # 从数据库查询
            session = self.oms_repo.session
            records = session.query(PGMOmsHistory).filter(
                PGMOmsHistory.draft_id == draft_id
            ).order_by(PGMOmsHistory.fetched_at.desc()).all()

            if records:
                # 取最新的记录
                latest_record = records[0]
                return {
                    'process_id': latest_record.process_id,
                    'work_sequence': self._extract_work_sequence(latest_record),
                    'work_type_desc': latest_record.work_type_desc
                }

            self.logger.warning(f"⚠️ 未找到草稿记录: {draft_id}")
            return None

        except Exception as e:
            self.logger.error(f"❌ 获取process信息失败: {str(e)}")
            return None

    def _extract_work_sequence(self, oms_record: PGMOmsHistory) -> int:
        """提取工作序列号"""
        # 这里需要根据实际数据结构提取work_sequence
        # 暂时返回默认值1，实际需要根据数据调整
        return 1

# 更新OMS客户端以支持文件下载
class EnhancedOMSClient(OMSClient):
    """增强的OMS客户端（支持文件下载）"""

    def get_file_download_info(self, draft_id: str) -> List[Dict[str, any]]:
        """获取文件下载信息"""
        # 这里需要调用OMS API获取文件信息
        # 暂时返回模拟数据

        return [
            {
                'file_download_id': f'mock_{draft_id}_1',
                'file_name': f'{draft_id}.zip',
                'size': 1024000,
                'bpms_process_id': 'mock_process_id',
                'bpms_work_sequence': 1
            },
            {
                'file_download_id': f'mock_{draft_id}_2',
                'file_name': 'HESS.xlsx',
                'size': 512000,
                'bpms_process_id': 'mock_process_id',
                'bpms_work_sequence': 1
            }
        ]