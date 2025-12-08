"""
OMS客户端模块 - 负责与SK Hynix OMS系统交互
"""
import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
import time
import hashlib
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger
from database.models import PGMOmsHistory
from database.repositories import PGMOmsHistoryRepository


class OMSClient:
    """OMS系统客户端"""

    def __init__(self):
        """初始化OMS客户端"""
        self.logger = get_pgm_logger().get_logger('oms')
        self.config = get_config().get_oms_config()
        self.token = None
        self.token_expiry = None

        # ET和AT API端点
        self.et_detail_endpoint = "/bpms/test-pgm/module/dram-et/process-id"
        self.at_detail_endpoint = "/bpms/test-pgm/module/dram-at/process-id"

        # 缓存最近查询的PGM数据（可选）
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存

        self.logger.info("🔧 OMS客户端初始化完成")

    def _get_headers(self, additional_headers: Dict[str, str] = None) -> Dict[str, str]:
        headers = self.config['headers'].copy()
        """获取请求头"""
        headers.update({
            "uiId": "ModuleTestPgmDistributeStatus",
            "uiName": "BPM%20%3E%20MOD%20Test%20PGM%20Distribute%20Status",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"",
            "sec-ch-ua-mobile": "?0",
            "Accept": "application/json, text/plain, */*",
            "Sec-Fetch-Site": "same-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty"
        })

        if self.token:
            headers['Authorization'] = self.token

        if additional_headers:
            headers.update(additional_headers)

        return headers

    def login(self) -> bool:
        """
        登录OMS系统获取token

        Returns:
            是否登录成功
        """
        try:
            # 如果token仍然有效，直接使用
            if self.token and self.token_expiry and datetime.now() < self.token_expiry:
                self.logger.debug("✅ 使用缓存的OMS token")
                return True

            login_url = urljoin(self.config['api_base'], self.config['auth_endpoint'])

            login_data = {
                "id": self.config['user_id'],
                "password": self.config['password']
            }

            self.logger.info(f"🔑 正在登录OMS系统: {self.config['user_id']}")

            response = requests.post(
                login_url,
                json=login_data,
                headers=self._get_headers(),
                timeout=30
            )

            response.raise_for_status()

            result = response.json()
            token = result.get("token")

            if not token:
                self.logger.error("❌ OMS登录失败: 响应中未找到token")
                return False

            self.token = f"Bearer {token}"

            # 设置token过期时间（假设token有效期为1小时）
            self.token_expiry = datetime.now() + timedelta(minutes=55)

            self.logger.info("✅ OMS登录成功")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ OMS登录失败 (网络错误): {str(e)}")
            return False
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ OMS登录失败 (JSON解析错误): {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"❌ OMS登录失败 (未知错误): {str(e)}")
            return False

    def _ensure_login(self) -> bool:
        """确保已登录"""
        if not self.token or (self.token_expiry and datetime.now() >= self.token_expiry):
            return self.login()
        return True

    def get_et_pgm_details(self, process_id: str, work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        获取ET PGM详细信息

        Args:
            process_id: 流程ID (UUID格式)
            work_sequence: 工作序列号

        Returns:
            ET PGM详细信息
        """
        try:
            if not self._ensure_login():
                self.logger.error("❌ 获取ET详情失败: 登录无效")
                return None

            url = urljoin(self.config['api_base'], self.et_detail_endpoint)

            params = {
                "processId": process_id,
                "workSequence": work_sequence
            }

            self.logger.info(f"📋 获取ET PGM详情: process_id={process_id}, work_sequence={work_sequence}")

            start_time = time.time()
            response = requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=60
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            self.logger.info(f"✅ 成功获取ET PGM详情，响应时间: {response_time:.2f}秒")

            # 提取关键信息
            extracted_data = self._extract_et_info(data, process_id, work_sequence)

            return extracted_data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取ET PGM详情失败 (网络错误): {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取ET PGM详情失败 (JSON解析错误): {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 获取ET PGM详情失败 (未知错误): {str(e)}")
            return None

    def get_at_pgm_details(self, process_id: str, work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        获取AT PGM详细信息

        Args:
            process_id: 流程ID (UUID格式)
            work_sequence: 工作序列号

        Returns:
            AT PGM详细信息
        """
        try:
            if not self._ensure_login():
                self.logger.error("❌ 获取AT详情失败: 登录无效")
                return None

            url = urljoin(self.config['api_base'], self.at_detail_endpoint)

            params = {
                "processId": process_id,
                "workSequence": work_sequence
            }

            self.logger.info(f"📋 获取AT PGM详情: process_id={process_id}, work_sequence={work_sequence}")

            start_time = time.time()
            response = requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=60
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            self.logger.info(f"✅ 成功获取AT PGM详情，响应时间: {response_time:.2f}秒")

            # 提取关键信息
            extracted_data = self._extract_at_info(data, process_id, work_sequence)

            return extracted_data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取AT PGM详情失败 (网络错误): {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取AT PGM详情失败 (JSON解析错误): {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 获取AT PGM详情失败 (未知错误): {str(e)}")
            return None

    def _extract_et_info(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
        """提取ET信息"""
        try:
            extracted = {
                'process_id': process_id,
                'work_sequence': work_sequence,
                'type': 'ET',
                'extracted_at': datetime.now().isoformat(),
                'pgm_records': [],
                'file_info': [],
                'work_info': {}
            }

            # 提取工作详情
            if 'workDetailViews' in data and data['workDetailViews']:
                work_detail = data['workDetailViews'][0]
                extracted['work_info'] = {
                    'process_name': work_detail.get('processName'),
                    'work_name': work_detail.get('workName'),
                    'status': work_detail.get('status'),
                    'process_start_time': work_detail.get('processStartTime'),
                    'process_end_time': work_detail.get('processEndTime'),
                    'work_start_time': work_detail.get('workStartTime'),
                    'work_end_time': work_detail.get('workEndTime'),
                    'factory': work_detail.get('factory'),
                    'specified_work_user': work_detail.get('specifiedWorkUserName')
                }

                # 提取文件信息
                if 'file' in work_detail and work_detail['file']:
                    for file_item in work_detail['file']:
                        file_info = {
                            'file_download_id': file_item.get('fileDownloadId'),
                            'file_name': file_item.get('fileName'),
                            'size': file_item.get('size')
                        }
                        extracted['file_info'].append(file_info)

            # 提取PGM记录
            if 'testProgramModuleDramEtViews' in data:
                for pgm_record in data['testProgramModuleDramEtViews']:
                    pgm_info = {
                        'draft_id': pgm_record.get('draftId'),
                        'pgm_id': pgm_record.get('pgmId'),
                        'pgm_rev_ver': pgm_record.get('pgmRevVer'),
                        'pgm_dir': pgm_record.get('pgmDir'),
                        'pgm_dir2': pgm_record.get('pgmDir2'),
                        'pgm_dir3': pgm_record.get('pgmDir3'),
                        'pgm_dir4': pgm_record.get('pgmDir4'),
                        'equipment_model_code': pgm_record.get('equipmentModelCode'),
                        'operation_id': pgm_record.get('operationId'),
                        'module_type': pgm_record.get('moduleType'),
                        'product_type': pgm_record.get('productType'),
                        'tech_nm': pgm_record.get('techNm'),
                        'pkg_den_typ': pgm_record.get('pkgDenTyp'),
                        'organiz_cd': pgm_record.get('organizCd'),
                        'den_typ': pgm_record.get('denTyp'),
                        'change_date_time': pgm_record.get('changeDateTime'),
                        'factory_id': pgm_record.get('factoryId')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取ET信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取ET信息失败: {str(e)}")
            return {}

    def _extract_at_info(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
        """提取AT信息"""
        try:
            extracted = {
                'process_id': process_id,
                'work_sequence': work_sequence,
                'type': 'AT',
                'extracted_at': datetime.now().isoformat(),
                'pgm_records': [],
                'file_info': [],
                'work_info': {}
            }

            # 提取工作详情
            if 'workDetailViews' in data and data['workDetailViews']:
                work_detail = data['workDetailViews'][0]
                extracted['work_info'] = {
                    'process_name': work_detail.get('processName'),
                    'work_name': work_detail.get('workName'),
                    'status': work_detail.get('status'),
                    'process_start_time': work_detail.get('processStartTime'),
                    'process_end_time': work_detail.get('processEndTime'),
                    'work_start_time': work_detail.get('workStartTime'),
                    'work_end_time': work_detail.get('workEndTime'),
                    'factory': work_detail.get('factory'),
                    'specified_work_user': work_detail.get('specifiedWorkUserName')
                }

                # 提取文件信息
                if 'file' in work_detail and work_detail['file']:
                    for file_item in work_detail['file']:
                        file_info = {
                            'file_download_id': file_item.get('fileDownloadId'),
                            'file_name': file_item.get('fileName'),
                            'size': file_item.get('size')
                        }
                        extracted['file_info'].append(file_info)

            # 提取PGM记录
            if 'testProgramModuleDramAtViews' in data:
                for pgm_record in data['testProgramModuleDramAtViews']:
                    pgm_info = {
                        'draft_id': pgm_record.get('draftId'),
                        'pgm_id': pgm_record.get('pgmId'),
                        'pgm_rev_ver': pgm_record.get('pgmRevVer'),
                        'pgm_dir': pgm_record.get('pgmDir'),
                        'hdiag_dir': pgm_record.get('hdiagDir'),
                        'test_board_id': pgm_record.get('testBoardId'),
                        'operation_id': pgm_record.get('operationId'),
                        'module_type': pgm_record.get('moduleType'),
                        'product_type': pgm_record.get('productType'),
                        'tech_nm': pgm_record.get('techNm'),
                        'pkg_den_typ': pgm_record.get('pkgDenTyp'),
                        'sap_history_code': pgm_record.get('sapHistoryCode'),
                        'temper_val': pgm_record.get('temperVal'),
                        'change_date_time': pgm_record.get('changeDateTime'),
                        'factory_id': pgm_record.get('factoryId')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取AT信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取AT信息失败: {str(e)}")
            return {}

    def download_file(self, file_download_id: str, save_path: str) -> bool:
        """
        下载文件

        Args:
            file_download_id: 文件下载ID
            save_path: 保存路径

        Returns:
            是否下载成功
        """
        try:
            if not self._ensure_login():
                self.logger.error("❌ 下载文件失败: 登录无效")
                return False

            # 假设下载文件的API端点（需要确认实际API）
            download_url = f"{self.config['api_base']}/files/download/{file_download_id}"

            self.logger.info(f"📥 下载文件: {file_download_id} -> {save_path}")

            response = requests.get(
                download_url,
                headers=self._get_headers(),
                stream=True,
                timeout=120
            )

            response.raise_for_status()

            # 确保目录存在
            os.makedirs(os.path.dirname(save_path), exist_ok=True)

            # 下载文件
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(save_path)
            self.logger.info(f"✅ 文件下载完成: {save_path} ({file_size} bytes)")
            return True

        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 下载文件失败 (网络错误): {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"❌ 下载文件失败 (未知错误): {str(e)}")
            return False

class OMSDataProcessor:
    """OMS数据处理器"""

    # work_type_desc 到 work_type_no 的映射
    WORK_TYPE_MAP = {
        '[1Step] 기안': 1,
        '[2Step] 외주사 결과': 2,
        '[3Step] 최종승인': 3,
        '[4Step] 양산적용': 4
    }

    def __init__(self):
        """初始化数据处理器"""
        self.logger = get_pgm_logger().get_logger('oms_processor')
        self.oms_client = OMSClient()
        self.repository = PGMOmsHistoryRepository()

    def fetch_and_process_latest_data(self) -> bool:
        """
        获取并处理最新的OMS数据

        Returns:
            是否成功
        """
        try:
            self.logger.info("🔄 开始获取最新OMS数据")

            # 1. 获取最新数据（默认获取11天内的数据）
            pgm_data = self.oms_client.get_pgm_distribution_status()

            if not pgm_data:
                self.logger.warning("⚠️ 未获取到OMS数据")
                return False

            self.logger.info(f"📊 获取到 {len(pgm_data)} 条OMS记录")

            # 2. 处理每条记录
            success_count = 0
            error_count = 0

            for item in pgm_data:
                try:
                    processed_data = self._process_single_record(item)

                    if processed_data:
                        # 保存到数据库
                        if self.repository.upsert_oms_record(processed_data):
                            success_count += 1
                        else:
                            error_count += 1
                    else:
                        error_count += 1

                except Exception as e:
                    self.logger.error(f"❌ 处理OMS记录失败: {str(e)}")
                    error_count += 1

            # 3. 记录结果
            self.logger.info(f"✅ OMS数据处理完成 - 成功: {success_count}, 失败: {error_count}")

            return success_count > 0

        except Exception as e:
            self.logger.error(f"❌ OMS数据处理失败: {str(e)}")
            return False

    def _process_single_record(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        处理单条OMS记录

        Args:
            raw_data: 原始数据

        Returns:
            处理后的数据
        """
        try:
            # 提取关键字段
            draft_id = raw_data.get('draftId')
            work_type_desc = raw_data.get('workTypeDesc')

            if not draft_id or not work_type_desc:
                self.logger.warning(f"⚠️ OMS记录缺少关键字段: {raw_data}")
                return None

            # 映射 work_type_no
            work_type_no = self.WORK_TYPE_MAP.get(work_type_desc)

            # 构建处理后的数据
            processed_data = {
                'draft_id': draft_id,
                'work_type_desc': work_type_desc,
                'process_id': raw_data.get('processId'),
                'work_type_no': work_type_no,
                'work_status': raw_data.get('workStatus'),
                'work_start_tm': raw_data.get('workStartTm'),
                'complete_yn': raw_data.get('completeYn'),
                'user_id': raw_data.get('userId'),
                'user_name': raw_data.get('userName'),
                'fac_id': raw_data.get('facId'),
                'process_name': raw_data.get('processName'),
                'process_status_code': raw_data.get('processStatusCode')
            }

            # 计算TAT相关信息
            if work_type_no and raw_data.get('workStartTm'):
                tat_info = self._calculate_tat_info(
                    raw_data['workStartTm'],
                    work_type_no
                )
                processed_data.update(tat_info)

            return processed_data

        except Exception as e:
            self.logger.error(f"❌ 处理单条OMS记录失败: {str(e)}")
            return None

    def _calculate_tat_info(self, work_start_tm: str, work_type_no: int) -> Dict[str, Any]:
        """
        计算TAT相关信息

        Args:
            work_start_tm: 工作开始时间
            work_type_no: 工作类型编号

        Returns:
            TAT信息字典
        """
        try:
            # 解析时间字符串（格式可能为 "MM/DD HH:MM" 或 "YYYY/MM/DD HH:MM"）
            try:
                # 尝试格式 "YYYY/MM/DD HH:MM"
                if '/' in work_start_tm and ':' in work_start_tm:
                    parts = work_start_tm.split()
                    if len(parts) == 2:
                        date_part, time_part = parts
                        date_parts = date_part.split('/')

                        if len(date_parts) == 2:  # "MM/DD"
                            # 添加当前年份
                            current_year = datetime.now().year
                            start_time = datetime.strptime(
                                f"{current_year}/{date_part} {time_part}",
                                "%Y/%m/%d %H:%M"
                            )
                        elif len(date_parts) == 3:  # "YYYY/MM/DD"
                            start_time = datetime.strptime(work_start_tm, "%Y/%m/%d %H:%M")
                        else:
                            start_time = datetime.now()
                    else:
                        start_time = datetime.now()
                else:
                    start_time = datetime.now()
            except:
                start_time = datetime.now()

            # 计算TAT（天数）
            tat_days = (datetime.now() - start_time).total_seconds() / (24 * 3600)
            tat_days = round(tat_days, 2)

            # 计算marking
            tat_marking = self._calculate_tat_marking(tat_days)

            # 计算info_object
            info_object = "Hitech" if work_type_no in [2, 4] else "All"

            return {
                'tat_days': tat_days,
                'tat_marking': tat_marking,
                'info_object': info_object
            }

        except Exception as e:
            self.logger.error(f"❌ 计算TAT信息失败 ({work_start_tm}): {str(e)}")
            return {}

    def _calculate_tat_marking(self, tat_days: float) -> str:
        """
        根据TAT计算marking

        Args:
            tat_days: TAT天数

        Returns:
            marking字符串
        """
        try:
            config = get_config().get_email_config()
            thresholds = config.get('tat_thresholds', {})

            alarm_hours = thresholds.get('alarm_hours', 72)
            warning_hours = thresholds.get('warning_hours', 48)
            notice_hours = thresholds.get('notice_hours', 24)

            tat_hours = tat_days * 24

            if tat_hours > alarm_hours:
                return "Alarm"
            elif tat_hours > warning_hours:
                return "Warning"
            elif tat_hours > notice_hours:
                return "Notice"
            else:
                return "Normal"

        except:
            # 默认阈值
            if tat_days > 3:
                return "Alarm"
            elif tat_days > 2:
                return "Warning"
            elif tat_days > 1:
                return "Notice"
            else:
                return "Normal"

    def check_tat_alarms(self) -> List[Dict[str, Any]]:
        """
        检查TAT超时并返回报警信息

        Returns:
            报警信息列表
        """
        try:
            self.logger.info("🔍 检查TAT超时报警")

            # 获取最近的OMS记录
            recent_records = self.repository.get_recent_drafts(days=30)

            alarms = []
            for record in recent_records:
                try:
                    # 计算TAT
                    if record.work_start_tm and record.work_type_no:
                        tat_info = self._calculate_tat_info(
                            record.work_start_tm,
                            record.work_type_no
                        )

                        tat_marking = tat_info.get('tat_marking', 'Normal')

                        # 如果是Alarm级别，生成报警信息
                        if tat_marking == 'Alarm':
                            alarm_info = {
                                'draft_id': record.draft_id,
                                'work_type_desc': record.work_type_desc,
                                'work_start_tm': record.work_start_tm,
                                'tat_days': tat_info.get('tat_days'),
                                'tat_marking': tat_marking,
                                'info_object': tat_info.get('info_object'),
                                'user_name': record.user_name,
                                'process_name': record.process_name
                            }
                            alarms.append(alarm_info)

                except Exception as e:
                    self.logger.error(f"❌ 检查单条记录TAT失败: {str(e)}")
                    continue

            self.logger.info(f"⚠️ 发现 {len(alarms)} 个TAT超时报警")

            return alarms

        except Exception as e:
            self.logger.error(f"❌ 检查TAT报警失败: {str(e)}")
            return []


def test_oms_client():
    """测试OMS客户端"""
    print("=" * 60)
    print("OMS客户端测试")
    print("=" * 60)

    logger = get_pgm_logger()
    logger.log_execution_start("OMS客户端测试")

    try:
        # 1. 初始化客户端
        oms_client = OMSClient()

        # 2. 测试登录
        print("🔑 测试OMS登录...")
        if oms_client.login():
            print("✅ OMS登录成功")
        else:
            print("❌ OMS登录失败")
            return False

        # 3. 测试获取数据
        print("📋 测试获取PGM分发状态...")
        pgm_data = oms_client.get_pgm_distribution_status()

        if pgm_data:
            print(f"✅ 成功获取 {len(pgm_data)} 条PGM数据")

            # 显示前几条数据
            for i, item in enumerate(pgm_data[:3]):
                print(f"  {i + 1}. {item.get('draftId', 'N/A')} - {item.get('processName', 'N/A')}")
        else:
            print("❌ 获取PGM数据失败")
            return False

        # 4. 测试数据处理
        print("🔄 测试数据处理...")
        processor = OMSDataProcessor()

        if processor.fetch_and_process_latest_data():
            print("✅ OMS数据处理成功")
        else:
            print("❌ OMS数据处理失败")

        # 5. 测试TAT报警检查
        print("🔍 测试TAT报警检查...")
        alarms = processor.check_tat_alarms()

        if alarms:
            print(f"⚠️ 发现 {len(alarms)} 个TAT报警:")
            for alarm in alarms[:3]:  # 显示前3个
                print(f"  - {alarm['draft_id']}: TAT {alarm['tat_days']}天")
        else:
            print("✅ 未发现TAT报警")

        logger.log_execution_end("OMS客户端测试", success=True)
        return True

    except Exception as e:
        logger.log_execution_end("OMS客户端测试", success=False, message=str(e))
        print(f"❌ OMS客户端测试失败: {str(e)}")
        return False


if __name__ == "__main__":
    test_oms_client()