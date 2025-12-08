"""
PGM验证器模块 - 基于API获取的HESS信息进行验证
"""
import os
import re
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime

from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.models import PGMMain, PGMStatus, NextTask
from database.repositories import PGMMainRepository
from core.oms_client import OMSClient


class PGMVerifier:
    """PGM验证器（基于API获取的HESS信息）"""

    def __init__(self, oms_client: OMSClient = None):
        """初始化PGM验证器"""
        self.logger = get_pgm_logger().get_logger('pgm_verifier')
        self.config = get_config()
        self.oms_client = oms_client or OMSClient()
        self.pgm_verification_settings = self.config.get_pgm_verification_settings()

        self.logger.info("🔧 PGM验证器初始化完成")

    def verify_pgm_by_draft_id(self, draft_id: str) -> Tuple[bool, str, Dict[str, any]]:
        """
        通过draft_id验证PGM

        Args:
            draft_id: 草稿ID

        Returns:
            (是否成功, 验证结果代码, 详细结果)
        """
        try:
            self.logger.info(f"🔍 开始验证PGM: draft_id={draft_id}")

            # 1. 获取PGM主记录
            repo = PGMMainRepository()
            pgm_record = repo.get_by_id(draft_id)
            if not pgm_record:
                self.logger.error(f"❌ 未找到PGM记录: {draft_id}")
                return False, "PGM_NOT_FOUND", {"message": "未找到PGM记录"}

            repo.close()

            # 2. 获取本地文件信息
            local_path = pgm_record.server_path
            if not local_path or not os.path.exists(local_path):
                self.logger.error(f"❌ 本地路径不存在: {local_path}")
                return False, "LOCAL_PATH_NOT_EXIST", {"message": "本地路径不存在"}

            # 3. 根据PGM类型获取详细信息
            pgm_type = pgm_record.pgm_type
            process_id = self._extract_process_id(pgm_record)  # 需要从其他字段获取

            if not process_id:
                self.logger.error(f"❌ 无法获取process_id")
                return False, "NO_PROCESS_ID", {"message": "无法获取process_id"}

            # 4. 从OMS获取详细信息
            if pgm_type in ["ET", "BOTH"]:
                et_details = self._get_et_details(process_id, pgm_record)
                if et_details:
                    self.logger.info(f"📊 获取到ET详情: {len(et_details.get('pgm_records', []))}条记录")

            if pgm_type in ["AT", "BOTH"]:
                at_details = self._get_at_details(process_id, pgm_record)
                if at_details:
                    self.logger.info(f"📊 获取到AT详情: {len(at_details.get('pgm_records', []))}条记录")

            # 5. 分析本地文件
            local_files = self._analyze_local_files(local_path)

            # 6. 比较API信息与本地文件
            verification_result = self._compare_with_api_info(
                pgm_type, local_files, et_details, at_details
            )

            # 7. 更新数据库
            self._update_verification_result(draft_id, verification_result)

            self.logger.info(f"✅ PGM验证完成: {draft_id}")
            return True, verification_result['code'], verification_result

        except Exception as e:
            self.logger.error(f"❌ PGM验证失败 ({draft_id}): {str(e)}")
            return False, "VERIFICATION_ERROR", {"message": str(e)}

    def _extract_process_id(self, pgm_record: PGMMain) -> Optional[str]:
        """从PGM记录中提取process_id"""
        # 这里需要根据实际数据结构来提取process_id
        # 可能需要从其他字段或关联表中获取
        return None

    def _get_et_details(self, process_id: str, pgm_record: PGMMain) -> Optional[Dict[str, any]]:
        """获取ET详情"""
        try:
            # 这里需要获取work_sequence，可能从其他字段获取
            work_sequence = 1  # 默认值，实际需要获取

            return self.oms_client.get_et_pgm_details(process_id, work_sequence)
        except Exception as e:
            self.logger.error(f"❌ 获取ET详情失败: {str(e)}")
            return None

    def _get_at_details(self, process_id: str, pgm_record: PGMMain) -> Optional[Dict[str, any]]:
        """获取AT详情"""
        try:
            # 这里需要获取work_sequence，可能从其他字段获取
            work_sequence = 1  # 默认值，实际需要获取

            return self.oms_client.get_at_pgm_details(process_id, work_sequence)
        except Exception as e:
            self.logger.error(f"❌ 获取AT详情失败: {str(e)}")
            return None

    def _analyze_local_files(self, local_path: str) -> Dict[str, any]:
        """分析本地文件"""
        result = {
            'at_files': [],
            'et_files': [],
            'zip_files': [],
            'other_files': [],
            'total_count': 0
        }

        try:
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, local_path)

                    # 检查文件类型
                    if file.endswith('.xml'):
                        result['at_files'].append(rel_path)
                    elif file.endswith('.obj'):
                        result['et_files'].append(rel_path)
                    elif file.endswith('.zip'):
                        result['zip_files'].append(rel_path)
                    else:
                        result['other_files'].append(rel_path)

                    result['total_count'] += 1

            self.logger.debug(
                f"📁 本地文件分析: AT={len(result['at_files'])}, ET={len(result['et_files'])}, ZIP={len(result['zip_files'])}")

        except Exception as e:
            self.logger.error(f"❌ 分析本地文件失败: {str(e)}")

        return result

    def _compare_with_api_info(self, pgm_type: str, local_files: Dict[str, any],
                               et_details: Optional[Dict[str, any]],
                               at_details: Optional[Dict[str, any]]) -> Dict[str, any]:
        """与API信息比较"""
        result = {
            'code': 'UNKNOWN',
            'message': '未知验证结果',
            'details': {},
            'matched': False
        }

        try:
            # 提取API中的PGM路径
            api_pgm_paths = set()

            if et_details and 'pgm_records' in et_details:
                for record in et_details['pgm_records']:
                    pgm_dir = record.get('pgm_dir')
                    if pgm_dir:
                        api_pgm_paths.add(pgm_dir)

            if at_details and 'pgm_records' in at_details:
                for record in at_details['pgm_records']:
                    pgm_dir = record.get('pgm_dir')
                    if pgm_dir:
                        api_pgm_paths.add(pgm_dir)
                    hdiag_dir = record.get('hdiag_dir')
                    if hdiag_dir:
                        api_pgm_paths.add(hdiag_dir)

            # 提取本地文件路径（简化处理）
            local_paths = set()
            for file_list in [local_files['at_files'], local_files['et_files']]:
                for file_path in file_list:
                    # 标准化路径
                    normalized = self._normalize_path(file_path)
                    local_paths.add(normalized)

            # 比较路径
            matched_paths = api_pgm_paths.intersection(local_paths)
            missing_in_local = api_pgm_paths - local_paths
            extra_in_local = local_paths - api_pgm_paths

            # 生成结果
            result['details'] = {
                'api_paths_count': len(api_pgm_paths),
                'local_paths_count': len(local_paths),
                'matched_paths_count': len(matched_paths),
                'missing_paths': list(missing_in_local),
                'extra_paths': list(extra_in_local)
            }

            # 确定验证结果代码
            result_code = self._determine_verification_code(
                len(api_pgm_paths), len(local_paths), len(matched_paths),
                pgm_type, local_files
            )

            result['code'] = result_code
            result['message'] = self.pgm_verification_settings['comparison_codes'].get(
                result_code, '未知验证结果'
            )
            result['matched'] = (len(missing_in_local) == 0 and len(api_pgm_paths) > 0)

            self.logger.info(
                f"📊 验证比较结果: API路径={len(api_pgm_paths)}, 本地路径={len(local_paths)}, 匹配={len(matched_paths)}")

        except Exception as e:
            self.logger.error(f"❌ 比较API信息失败: {str(e)}")
            result['code'] = 'COMPARISON_ERROR'
            result['message'] = f'比较过程中发生错误: {str(e)}'

        return result

    def _normalize_path(self, path: str) -> str:
        """标准化路径"""
        # 移除多余的分隔符，转换为小写等
        normalized = path.replace('\\', '/').strip('/').lower()
        return normalized

    def _determine_verification_code(self, api_count: int, local_count: int,
                                     matched_count: int, pgm_type: str,
                                     local_files: Dict[str, any]) -> str:
        """确定验证结果代码"""
        # 简化的验证逻辑，实际需要根据完整的compare_dir_and_hess逻辑

        at_files_count = len(local_files['at_files'])
        et_files_count = len(local_files['et_files'])

        # 根据不同的组合返回不同的代码
        if api_count == 0:
            return 'da0de0ha0he0'  # 无有效的PGM以及HESS

        if pgm_type == 'ET':
            if et_files_count > 0 and matched_count == api_count:
                return 'da0de1ha0he1'  # ET 匹配
            elif et_files_count > 0:
                return 'da0de3ha0he1'  # ET 不匹配
            else:
                return 'da0de0ha0he1'  # 仅有ET HESS

        elif pgm_type == 'AT':
            if at_files_count > 0 and matched_count == api_count:
                return 'da1de0ha1he0'  # AT 匹配
            elif at_files_count > 0:
                return 'da3de0ha1he0'  # AT 不匹配
            else:
                return 'da0de0ha1he0'  # 仅有AT HESS

        elif pgm_type == 'BOTH':
            if at_files_count > 0 and et_files_count > 0 and matched_count == api_count:
                return 'da1de1ha1he1'  # AT 匹配，ET 匹配
            elif at_files_count > 0 and et_files_count > 0:
                return 'da3de3ha1he1'  # AT 不匹配，ET 不匹配
            else:
                return 'da1de1ha0he0'  # 缺AT HESS，缺ET HESS

        return 'UNKNOWN'

    def _update_verification_result(self, pgm_id: str, result: Dict[str, any]):
        """更新验证结果到数据库"""
        try:
            repo = PGMMainRepository()

            update_data = {
                'verify_result_code': result['code'],
                'verify_result_desc': result['message'],
                'verify_time': datetime.now()
            }

            if result.get('matched', False):
                update_data['status'] = PGMStatus.VERIFIED
                update_data['next_task'] = NextTask.APPLY
            else:
                update_data['status'] = PGMStatus.VERIFY_FAILED
                update_data['next_task'] = NextTask.NONE

            repo.update(pgm_id, update_data)
            repo.close()

            self.logger.info(f"📝 更新验证结果: {pgm_id} - {result['code']}")

        except Exception as e:
            self.logger.error(f"❌ 更新验证结果失败 ({pgm_id}): {str(e)}")

    def verify_all_pending(self) -> Dict[str, any]:
        """验证所有待验证的PGM"""
        try:
            self.logger.info("🔄 开始批量验证待验证的PGM")

            repo = PGMMainRepository()
            pending_pgms = repo.get_ready_for_verify()
            repo.close()

            results = {
                'total': len(pending_pgms),
                'success': 0,
                'failed': 0,
                'details': []
            }

            for pgm in pending_pgms:
                success, code, details = self.verify_pgm_by_draft_id(pgm.pgm_id)

                result_item = {
                    'pgm_id': pgm.pgm_id,
                    'success': success,
                    'code': code,
                    'message': details.get('message', '')
                }

                results['details'].append(result_item)

                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1

            self.logger.info(
                f"📊 批量验证完成: 总计={results['total']}, 成功={results['success']}, 失败={results['failed']}")

            return results

        except Exception as e:
            self.logger.error(f"❌ 批量验证失败: {str(e)}")
            return {'total': 0, 'success': 0, 'failed': 0, 'error': str(e)}