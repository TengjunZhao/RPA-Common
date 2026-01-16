"""
OMS客户端模块 - 负责与SK Hynix OMS系统交互
"""
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
current_dir = os.path.dirname(os.path.abspath(__file__))  # core目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)
from utils.config_loader import get_config
from utils.logger import get_pgm_logger


class OMSClient:
    """OMS系统客户端"""

    def __init__(self):
        """初始化OMS客户端"""
        self.logger = get_pgm_logger().get_logger('oms')
        self.config = get_config().get_oms_config()

        # 获取端点配置
        self.endpoints = self.config['endpoints']
        self.api_base = self.config['api_base']

        # 认证信息
        config_loader = get_config()
        env = config_loader.get_current_environment()
        if env in self.config.get('credentials', {}):
            self.credentials = self.config['credentials'][env]
        else:
            # 回退到默认凭据结构（兼容旧配置）
            self.credentials = self.config.get('credentials', {})

        # Token管理
        self.token = None
        self.token_expiry = None
        self.token_refresh_buffer = timedelta(minutes=5)  # 提前5分钟刷新token

        # 请求超时配置
        self.timeouts = self.config.get('request_timeout', {
            'login': 30,
            'list': 60,
            'detail': 60,
            'download': 300
        })

        # 重试配置
        self.retry_settings = self.config.get('retry_settings', {
            'max_retries': 3,
            'retry_delay_seconds': 5,
            'retry_on_status_codes': [408, 429, 500, 502, 503, 504]
        })

        # 缓存配置
        self.cache = {}
        self.cache_ttl = 300  # 5分钟缓存

        # 创建带重试机制的Session
        self.session = self._create_retry_session()

        self.logger.info(f"🔧 OMS客户端初始化完成，环境: {env}")

        self.file_paths = get_config().get_file_paths()

    def _create_retry_session(self) -> requests.Session:
        """创建带重试机制的Session"""
        session = requests.Session()

        retry_strategy = Retry(
            total=self.retry_settings['max_retries'],
            backoff_factor=self.retry_settings['retry_delay_seconds'],
            status_forcelist=self.retry_settings['retry_on_status_codes']
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def _get_headers(self, endpoint_name: str = None) -> Dict[str, str]:
        """
        获取请求头

        Args:
            endpoint_name: 端点名称，用于获取特定端点的headers

        Returns:
            请求头字典
        """
        headers = self.config['headers']['common'].copy()

        # 添加特定端点的headers
        if endpoint_name and 'endpoint_specific' in self.config['headers']:
            specific_headers = self.config['headers']['endpoint_specific'].get(endpoint_name, {})
            headers.update(specific_headers)

        # 添加Authorization头
        if self.token and self._is_token_valid():
            headers['Authorization'] = self.token

        return headers

    def login(self, force: bool = False) -> Tuple[bool, str]:
        """
        登录OMS系统获取token

        Args:
            force: 是否强制重新登录

        Returns:
            (是否成功, 错误信息)
        """
        try:
            # 检查token是否仍然有效
            if not force and self._is_token_valid():
                self.logger.debug("✅ 使用缓存的OMS token")
                return True, "使用缓存的token"

            login_url = urljoin(self.api_base, self.endpoints['auth'])

            # 准备登录数据
            login_data = {
                "id": self.credentials['user_id'],
                "password": self.credentials['password']
            }

            self.logger.info(f"🔑 正在登录OMS系统: {self.credentials['user_id']}")
            self.logger.debug(f"登录URL: {login_url}")

            # 发送登录请求
            response = self.session.post(
                login_url,
                json=login_data,
                headers=self._get_headers(),
                timeout=self.timeouts['login']
            )

            response.raise_for_status()

            result = response.json()

            # 检查响应结构
            if not isinstance(result, dict):
                return False, "登录响应格式错误"

            token = result.get("token")
            if not token:
                error_msg = result.get("message", "未知错误")
                self.logger.error(f"❌ OMS登录失败: {error_msg}")
                return False, f"响应中未找到token: {error_msg}"

            # 更新token
            self.token = f"Bearer {token}"

            # 解析token过期时间（从响应中获取或使用默认值）
            token_duration = result.get("expires_in", 3600)  # 默认1小时
            self.token_expiry = datetime.now() + timedelta(seconds=token_duration)

            self.logger.info(f"✅ OMS登录成功，token有效期至: {self.token_expiry}")
            return True, "登录成功"

        except requests.exceptions.Timeout:
            self.logger.error("❌ OMS登录失败: 请求超时")
            return False, "请求超时"
        except requests.exceptions.ConnectionError:
            self.logger.error("❌ OMS登录失败: 连接错误")
            return False, "连接错误"
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            self.logger.error(f"❌ OMS登录失败 (HTTP {status_code}): {str(e)}")
            return False, f"HTTP错误: {status_code}"
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ OMS登录失败 (JSON解析错误): {str(e)}")
            return False, "JSON解析错误"
        except Exception as e:
            self.logger.error(f"❌ OMS登录失败 (未知错误): {str(e)}")
            return False, f"未知错误: {str(e)}"

    def _is_token_valid(self) -> bool:
        """检查token是否有效"""
        if not self.token or not self.token_expiry:
            return False

        # 考虑刷新缓冲时间
        return datetime.now() < (self.token_expiry - self.token_refresh_buffer)

    def _ensure_login(self) -> bool:
        """确保已登录，自动处理token刷新"""
        if not self._is_token_valid():
            success, message = self.login()
            if not success:
                self.logger.error(f"❌ 自动登录失败: {message}")
                return False
        return True

    # 获取新的PGM List，如果begin_date，end_date为空则为11天前至当前时间
    def get_pgm_distribution_status(self,
                                    begin_date: Optional[str] = None,
                                    end_date: Optional[str] = None,
                                    factory_id: str = "OSMOD",
                                    company_id: str = "HITECH",
                                    force_refresh: bool = False) -> List[Dict[str, Any]]:
        """
        获取PGM分发状态

        Args:
            begin_date: 开始日期 (格式: "YYYY-MM-DD HH:MM:SS")
            end_date: 结束日期 (格式: "YYYY-MM-DD HH:MM:SS")
            factory_id: 工厂ID
            company_id: 公司ID
            force_refresh: 是否强制刷新缓存

        Returns:
            PGM分发状态列表
        """
        # 生成缓存键
        cache_key = hashlib.md5(
            f"distribution_{factory_id}_{company_id}_{begin_date}_{end_date}".encode()
        ).hexdigest()

        # 检查缓存
        if not force_refresh and cache_key in self.cache:
            cache_data, cache_time = self.cache[cache_key]
            if time.time() - cache_time < self.cache_ttl:
                self.logger.debug(f"📦 使用缓存的PGM分发状态数据")
                return cache_data

        try:
            # 确保已登录
            if not self._ensure_login():
                self.logger.error("❌ 获取PGM分发状态失败: 登录无效")
                return []

            # 设置默认日期范围
            today = datetime.now()
            if not begin_date:
                begin_date = (today - timedelta(days=11)).strftime("%Y-%m-%d 07:00:00")
            if not end_date:
                end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d 07:00:00")
            self.logger.info(f"📅 日期参数: beginDate={begin_date}, endDate={end_date}")
            # 构建URL和参数
            url = urljoin(self.api_base, self.endpoints['distribute_status'])

            params = {
                "factoryId": factory_id,
                "companyId": company_id,
                "beginDate": begin_date,
                "endDate": end_date
            }

            self.logger.info(f"📋 获取PGM分发状态: {begin_date} 到 {end_date}")
            self.logger.debug(f"请求URL: {url}")
            self.logger.debug(f"请求参数: {params}")

            # 发送请求
            start_time = time.time()
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers('distribute_status'),
                timeout=self.timeouts['list']
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            if not isinstance(data, list):
                self.logger.error(f"❌ 响应格式错误，期望列表，得到: {type(data)}")
                return []

            self.logger.info(f"✅ 成功获取PGM分发状态，共{len(data)}条记录")
            self.logger.debug(f"响应时间: {response_time:.2f}秒")

            # 缓存结果
            self.cache[cache_key] = (data, time.time())

            return data

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"

            if status_code in [401, 403]:
                self.logger.info("🔄 Token过期或无效，尝试重新登录...")
                self.token = None
                self.token_expiry = None

                # 重新登录并重试
                if self._ensure_login():
                    return self.get_pgm_distribution_status(
                        begin_date, end_date, factory_id, company_id, True
                    )

            self.logger.error(f"❌ 获取PGM分发状态失败 (HTTP {status_code}): {str(e)}")
            return []
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (网络错误): {str(e)}")
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (JSON解析错误): {str(e)}")
            return []
        except Exception as e:
            self.logger.error(f"❌ 获取PGM分发状态失败 (未知错误): {str(e)}")
            return []

    # 根据pgm_record.processName确定是ET还是AT PGM
    def _determine_pgm_type(self, pgm_record: Dict[str, Any]) -> str:
        """确定PGM类型"""
        process_type = str(pgm_record.get('processType', '')).upper()

        if 'ET' in process_type:
            return 'ET'
        elif 'AT' in process_type:
            return 'AT'
        elif 'HESS' in process_type:
            return 'HESS'
        else:
            return 'UNKNOWN'

    # 根据PGM类型获取HESS详情
    def _get_pgm_detial(self,
                        pgm_type: str,
                        process_id: str,
                        work_sequence: int) -> Optional[Dict[str, Any]]:
        """
        通用方法获取PGM详情

        Args:
            pgm_type: 'et' 或 'at'
            process_id: 流程ID
            work_sequence: 工作序列号

        Returns:
            PGM详细信息
        """
        try:
            if not self._ensure_login():
                self.logger.error(f"❌ 获取{pgm_type.upper()}详情失败: 登录无效")
                return None

            # 构建URL
            endpoint_key = f"{pgm_type.lower()}_hess"
            url = urljoin(self.api_base, self.endpoints[endpoint_key])

            params = {
                "processId": process_id,
                "workSequence": work_sequence
            }

            self.logger.info(
                f"📋 获取{pgm_type.upper()} PGM详情: process_id={process_id}, work_sequence={work_sequence}")

            # 发送请求
            start_time = time.time()
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(endpoint_key),
                timeout=self.timeouts['detail']
            )
            response_time = time.time() - start_time

            response.raise_for_status()

            data = response.json()

            self.logger.info(f"✅ 成功获取{pgm_type.upper()} PGM详情，响应时间: {response_time:.2f}秒")

            # 提取关键信息
            if pgm_type == 'ET':
                extracted_data = self._extract_et_hess(data, process_id, work_sequence)
            else:
                extracted_data = self._extract_at_hess(data, process_id, work_sequence)

            return extracted_data

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (HTTP {status_code}): {str(e)}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (网络错误): {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (JSON解析错误): {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ 获取{pgm_type.upper()} PGM详情失败 (未知错误): {str(e)}")
            return None

    # 以ET模式抽取HESS信息（不涉及OMS操作）
    def _extract_et_hess(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
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
                        'pgm_dir5': pgm_record.get('pgmDir5'),
                        'operation_id': pgm_record.get('operationId'),
                        'module_type': pgm_record.get('moduleType'),
                        'product_type': pgm_record.get('productType'),
                        'tech_nm': pgm_record.get('techNm'),
                        'pkg_den_typ': pgm_record.get('pkgDenTyp'),
                        'den_typ': pgm_record.get('denTyp'),
                        'change_date_time': pgm_record.get('changeDateTime'),
                        'factory_id': pgm_record.get('factoryId'),
                        'del_yn': pgm_record.get('delYn'),
                        'organiz_cd': pgm_record.get('organizCd'),
                        'special_cd': pgm_record.get('specialCd'),
                        'timekey': pgm_record.get('timekey'),
                        'controller_name_val': pgm_record.get('controllerNameVal'),
                        'fab_id': pgm_record.get('fabId'),
                        'fmw_ver_val': pgm_record.get('fmwVerVal'),
                        'grade_code': pgm_record.get('gradeCode'),
                        'mask_cd1': pgm_record.get('maskCd1'),
                        'mod_section_typ': pgm_record.get('modSectionTyp'),
                        'pkg_typ2': pgm_record.get('pkgTyp2'),
                        'qual_opt_cd2': pgm_record.get('qualOptCd2'),
                        'sap_history_code': pgm_record.get('sapHistoryCode'),
                        'tranmit_bp_list': pgm_record.get('tranmitBpList'),
                        'tsv_die_typ': pgm_record.get('tsvDieTyp'),
                        'ver_typ': pgm_record.get('verTyp'),
                        'drafted': pgm_record.get('drafted'),
                        'equipment_model_code': pgm_record.get('equipmentModelCode'),
                        'module_height_value': pgm_record.get('moduleHeightValue'),
                        'owner_code': pgm_record.get('ownerCode')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取ET信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取ET信息失败: {str(e)}")
            return {}

    # 以AT模式抽取HESS信息（不涉及OMS操作）
    def _extract_at_hess(self, data: Dict[str, Any], process_id: str, work_sequence: int) -> Dict[str, Any]:
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
                        'draft_seq': pgm_record.get('draftSeq'),
                        'drafted': pgm_record.get('drafted'),
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
                        'factory_id': pgm_record.get('factoryId'),
                        'del_yn': pgm_record.get('delYn'),
                        'organiz_cd': pgm_record.get('organizCd'),
                        'qual_opt_cd2': pgm_record.get('qualOptCd2'),
                        'special_cd': pgm_record.get('specialCd'),
                        'timekey': pgm_record.get('timekey'),
                        'controller_name_val': pgm_record.get('controllerNameVal'),
                        'den_typ': pgm_record.get('denTyp'),
                        'fab_id': pgm_record.get('fabId'),
                        'fmw_ver_val': pgm_record.get('fmwVerVal'),
                        'grade_code': pgm_record.get('gradeCode'),
                        'mask_cd1': pgm_record.get('maskCd1'),
                        'mod_section_typ': pgm_record.get('modSectionTyp'),
                        'pkg_typ2': pgm_record.get('pkgTyp2'),
                        'product_special_handle_value': pgm_record.get('productSpecialHandleValue'),
                        'tranmit_bp_list': pgm_record.get('tranmitBpList'),
                        'tsv_die_typ': pgm_record.get('tsvDieTyp'),
                        'ver_typ': pgm_record.get('verTyp')
                    }
                    extracted['pgm_records'].append(pgm_info)

            self.logger.info(f"📊 提取AT信息: {len(extracted['pgm_records'])}条PGM记录")
            return extracted

        except Exception as e:
            self.logger.error(f"❌ 提取AT信息失败: {str(e)}")
            return {}

    # 下载单个文件
    def _download_sigle_file(self,
                             file_download_id: str,
                             file_name: str,
                             process_id: str,
                             work_sequence: int,
                             save_dir: str = None,
                             custom_filename: str = None) -> Tuple[bool, str]:
        """
        下载单个文件

        Args:
            file_download_id: 文件下载ID
            file_name: 原始文件名
            process_id: 流程ID
            work_sequence: 工作序列
            save_dir: 保存目录（如果为None则使用配置路径）
            custom_filename: 自定义文件名

        Returns:
            (是否成功, 保存的文件路径或错误信息)
        """
        try:
            if not self._ensure_login():
                return False, "下载文件失败: 登录无效"

            # 确定保存目录
            if save_dir is None:
                save_dir = self.file_paths['local_verify']

            # 创建目录（如果不存在）
            save_path_obj = Path(save_dir)
            save_path_obj.mkdir(parents=True, exist_ok=True)

            # 确定文件名
            if custom_filename:
                filename = custom_filename
            else:
                # 清理文件名，移除非法字符
                filename = self._sanitize_filename(file_name)


            # 构建完整保存路径
            save_path = save_path_obj / filename

            # 构建下载URL和参数
            download_url = urljoin(self.api_base, self.endpoints['file_download'])

            params = {
                "id": file_download_id,
                "userId": self.credentials.get('user_id', ''),
                "bpmsProcessId": process_id,
                "bpmsWorkSequence": work_sequence,
                "uiId": "ModuleTestPgmDistributeStatus"
            }

            self.logger.info(f"📥 开始下载文件: {filename}")
            self.logger.debug(f"下载URL: {download_url}")
            self.logger.debug(f"参数: {params}")

            # 发送下载请求
            response = self.session.get(
                download_url,
                params=params,
                headers=self._get_headers('file_download'),
                stream=True,
                timeout=self.timeouts['download']
            )

            response.raise_for_status()

            # 尝试从Content-Disposition获取文件名
            content_disposition = response.headers.get('content-disposition', '')
            if 'filename=' in content_disposition:
                match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if match:
                    server_filename = match.group(1)
                    
                    # 修复可能的编码问题：服务器返回的文件名可能是UTF-8被错误解释为Latin-1的情况
                    # 例如：韩文 "적용.zip" 可能变成 "ì\xa0\x81ì\x9a.zip"
                    try:
                        # 将可能被错误解释的Latin-1字符串转换回正确的UTF-8
                        server_filename_bytes = server_filename.encode('latin-1')
                        server_filename = server_filename_bytes.decode('utf-8')
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        # 如果转换失败，保持原样
                        pass
                    
                    filename = self._sanitize_filename(server_filename)
                    save_path = save_path_obj / filename
                    self.logger.info(f"📝 使用服务器提供的文件名: {filename}")
            # 获取文件大小
            total_size = int(response.headers.get('content-length', 0))
            content_type = response.headers.get('content-type', 'application/octet-stream')

            self.logger.info(f"📄 文件类型: {content_type}, 大小: {self._format_size(total_size)}")

            # 下载文件
            downloaded = 0
            start_time = time.time()

            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)

                        # 显示下载进度
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            current_time = time.time()
                            elapsed = current_time - start_time

                            # 每下载5MB或5秒显示一次进度
                            if downloaded % (5 * 1024 * 1024) == 0 or elapsed > 5:
                                speed = downloaded / elapsed / 1024 if elapsed > 0 else 0
                                self.logger.info(
                                    f"📥 下载进度: {percent:.1f}% ({self._format_size(downloaded)}/{self._format_size(total_size)}) "
                                    f"速度: {speed:.1f} KB/s"
                                )

            # 验证文件
            actual_size = os.path.getsize(save_path)
            download_time = time.time() - start_time

            if total_size > 0 and actual_size != total_size:
                self.logger.warning(
                    f"⚠️ 文件大小不匹配: 期望={self._format_size(total_size)}, 实际={self._format_size(actual_size)}")
            else:
                self.logger.info(f"✅ 文件大小验证通过")

            # 计算下载速度
            speed = actual_size / download_time / 1024 if download_time > 0 else 0

            self.logger.info(f"✅ 文件下载完成: {save_path}")
            self.logger.info(f"📊 下载统计 - 大小: {self._format_size(actual_size)}, "
                             f"时间: {download_time:.2f}秒, 速度: {speed:.1f} KB/s")

            # 计算文件哈希（可选）
            file_hash = self._calculate_file_hash(str(save_path))
            if file_hash:
                self.logger.debug(f"🔢 文件MD5: {file_hash}")

            return True, str(save_path)

        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "未知"
            error_msg = f"下载失败 (HTTP {status_code}): {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
        except Exception as e:
            error_msg = f"下载失败: {str(e)}"
            self.logger.error(f"❌ {error_msg}")
            return False, error_msg
    # 真实下载文件
    def download_pgm(self, pgm, save_dir: str = None):
        pgm_type = self._determine_pgm_type(pgm)
        process_id = pgm.get('processId')
        draft_id = pgm.get('draftId')
        self.logger.info(f"📊 开始下载PGM: {pgm_type} draft_id= {draft_id}, process_id= {process_id}")
        work_squence = pgm.get('workSequence')
        detail = self._get_pgm_detial(pgm_type, process_id, work_squence)
        file_info_list = detail.get('file_info')
        for file in file_info_list:
            file_download_id = file.get('file_download_id')
            file_name = file.get('file_name')
            self._download_sigle_file(file_download_id, file_name, process_id,1, save_dir)
        return detail

    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除非法字符，同时保持韩文等Unicode字符"""
        if not filename:
            return "unknown_file"

        # 移除非法字符，保留Unicode字符（如韩文）
        invalid_chars = '<>:"/\\|?*\t\n\r'  # 包括控制字符
        for char in invalid_chars:
            filename = filename.replace(char, '_')

        # 处理可能的问题字符，但保留韩文字母
        # 使用正则表达式替换掉控制字符（除常见空白字符外）
        filename = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '_', filename)

        # 确保字符串是有效的UTF-8编码，这对韩文等Unicode字符很重要
        try:
            # 先编码再解码以确保字符串的有效性
            filename = filename.encode('utf-8').decode('utf-8')
        except UnicodeDecodeError:
            # 如果遇到解码错误，使用错误处理策略
            filename = filename.encode('utf-8', errors='replace').decode('utf-8')

        # 限制长度，考虑长Unicode字符可能的影响
        if len(filename) > 255:
            name, ext = os.path.splitext(filename)
            filename = name[:250 - len(ext)] + ext

        return filename

    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小"""
        if size_bytes == 0:
            return "0B"

        units = ['B', 'KB', 'MB', 'GB']
        i = 0
        size = float(size_bytes)

        while size >= 1024 and i < len(units) - 1:
            size /= 1024
            i += 1

        return f"{size:.2f} {units[i]}"

    def _calculate_file_hash(self, file_path: str, algorithm: str = 'md5') -> str:
        """计算文件哈希值"""
        if not os.path.exists(file_path):
            return ""

        hash_func = hashlib.new(algorithm)

        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    hash_func.update(chunk)
            return hash_func.hexdigest()
        except:
            return ""

    def _create_download_summary(self, results: Dict[str, Any], target_dir: str):
        """创建下载摘要文件"""
        try:
            summary_path = Path(target_dir) / "download_summary.json"

            summary = {
                'timestamp': datetime.now().isoformat(),
                'pgm_info': results.get('pgm_info', {}),
                'download_stats': {
                    'successful': len(results.get('downloaded_files', [])),
                    'failed': len(results.get('failed_files', [])),
                    'total_size': results.get('total_size', 0),
                    'total_time': results.get('total_time', 0)
                },
                'downloaded_files': results.get('downloaded_files', []),
                'failed_files': results.get('failed_files', [])
            }

            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, ensure_ascii=False, indent=2)

            self.logger.info(f"📝 创建下载摘要: {summary_path}")

        except Exception as e:
            self.logger.warning(f"⚠️ 创建下载摘要失败: {str(e)}")
