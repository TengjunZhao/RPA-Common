"""
RPA脚本构建工具 - 将模块化代码合并为独立脚本
"""
import os
import re
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Set


class ScriptBuilder:
    def __init__(self, dev_root: str, prod_root: str):
        self.dev_root = Path(dev_root)
        self.prod_root = Path(prod_root)

        # 脚本依赖映射表（手动定义）
        self.dependency_map = {
            '01_fetch_pgm.py': [
                'core/oms_client.py',
                'core/file_downloader.py',
                'database/repositories.py',
                'utils/config_loader.py',
                'utils/logger.py',
                'utils/db_connection.py'
            ],
            '02_verify_pgm.py': [
                'core/file_processor.py',
                'core/hess_analyzer.py',
                'core/pgm_verifier.py',
                'database/repositories.py',
                'utils/config_loader.py',
                'utils/logger.py',
                'utils/db_connection.py'
            ],
            '03_apply_pgm.py': [
                'core/ftp_uploader.py',
                'core/file_processor.py',
                'database/repositories.py',
                'utils/config_loader.py',
                'utils/logger.py',
                'utils/db_connection.py'
            ],
            '04_alarm_check.py': [
                'core/tat_calculator.py',
                'core/email_sender.py',
                'database/repositories.py',
                'utils/config_loader.py',
                'utils/logger.py',
                'utils/db_connection.py'
            ],
            '05_monitor_lot.py': [
                'core/yield_monitor.py',
                'core/testtime_analyzer.py',
                'database/repositories.py',
                'utils/config_loader.py',
                'utils/logger.py',
                'utils/db_connection.py'
            ]
        }

    def extract_imports(self, filepath: Path) -> tuple:
        """提取文件中的导入语句"""
        std_imports = []
        third_party_imports = []
        local_imports = []

        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # 匹配导入语句
        import_pattern = r'^(import\s+[\w\s,\.]+|from\s+[\w\.]+\s+import\s+[\w\s,\.]+)'

        for line in content.split('\n'):
            line = line.strip()
            if re.match(import_pattern, line):
                # 分类导入
                if 'from utils' in line or 'from core' in line or 'from database' in line:
                    local_imports.append(line)
                elif any(pkg in line for pkg in ['pymysql', 'requests', 'openpyxl', 'ttkbootstrap']):
                    third_party_imports.append(line)
                else:
                    std_imports.append(line)

        return std_imports, third_party_imports, local_imports

    def remove_local_imports(self, content: str) -> str:
        """移除本地模块导入语句"""
        lines = content.split('\n')
        filtered_lines = []

        for line in lines:
            if not any(pattern in line for pattern in [
                'from utils import', 'from core import',
                'from database import', 'import utils.',
                'import core.', 'import database.'
            ]):
                filtered_lines.append(line)

        return '\n'.join(filtered_lines)

    def get_file_content(self, filepath: Path) -> str:
        """获取文件内容并移除本地导入"""
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # 移除本地模块导入
        content = self.remove_local_imports(content)

        # 移除可能的重复函数定义（如果有）
        return content

    def generate_header(self) -> str:
        """生成脚本头部信息"""
        return f'''"""
MTE PGM Automation - 生产环境独立脚本
======================================
生成时间: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
注意: 此文件为自动生成，请勿手动修改
======================================
"""

'''

    def build_script(self, script_name: str) -> Path:
        """构建单个生产脚本"""
        print(f"📦 开始构建脚本: {script_name}")

        # 1. 创建输出目录
        output_dir = self.prod_root / 'config'
        output_dir.mkdir(exist_ok=True)

        # 2. 准备输出文件
        output_file = self.prod_root / script_name

        # 3. 开始构建内容
        content_parts = []

        # 3.1 添加头部信息
        content_parts.append(self.generate_header())

        # 3.2 收集所有导入（去重）
        all_std_imports = set()
        all_third_party_imports = set()

        # 添加主脚本的导入
        main_script = self.dev_root / 'scripts' / script_name
        if main_script.exists():
            std_imp, third_imp, _ = self.extract_imports(main_script)
            all_std_imports.update(std_imp)
            all_third_party_imports.update(third_imp)

        # 添加依赖模块的导入
        for dep in self.dependency_map.get(script_name, []):
            dep_path = self.dev_root / dep
            if dep_path.exists():
                std_imp, third_imp, _ = self.extract_imports(dep_path)
                all_std_imports.update(std_imp)
                all_third_party_imports.update(third_imp)

        # 添加导入部分
        if all_std_imports:
            content_parts.append('# ===== 标准库导入 =====\n')
            content_parts.extend(sorted(all_std_imports))
            content_parts.append('\n')

        if all_third_party_imports:
            content_parts.append('# ===== 第三方库导入 =====\n')
            content_parts.extend(sorted(all_third_party_imports))
            content_parts.append('\n')

        # 3.3 添加依赖模块内容
        content_parts.append('# ===== 核心功能模块（自动内联） =====\n')
        for dep in self.dependency_map.get(script_name, []):
            dep_path = self.dev_root / dep
            if dep_path.exists():
                content_parts.append(f'\n# --- {dep} ---\n')
                content_parts.append(self.get_file_content(dep_path))

        # 3.4 添加主脚本内容（移除导入）
        if main_script.exists():
            content_parts.append(f'\n# --- 主程序: {script_name} ---\n')
            main_content = self.get_file_content(main_script)
            content_parts.append(main_content)

        # 4. 写入文件
        full_content = '\n'.join(content_parts)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_content)

        print(f"✅ 脚本构建完成: {output_file}")

        # 5. 复制配置文件模板
        config_template = self.dev_root / 'config' / 'config_template.json'
        if config_template.exists():
            target_config = output_dir / 'config.json'
            with open(config_template, 'r', encoding='utf-8') as src:
                with open(target_config, 'w', encoding='utf-8') as dst:
                    dst.write(src.read())
            print(f"✅ 配置文件复制完成: {target_config}")

        return output_file

    def build_all(self):
        """构建所有生产脚本"""
        print("🚀 开始构建所有生产脚本...")

        # 确保生产目录存在
        self.prod_root.mkdir(exist_ok=True)

        # 构建每个脚本
        for script_name in self.dependency_map.keys():
            self.build_script(script_name)

        print("🎉 所有脚本构建完成！")

        # 生成批处理文件（Windows）
        self.generate_batch_file()

    def generate_batch_file(self):
        """生成Windows批处理执行文件"""
        batch_content = '''@echo off
echo ========================================
echo    MTE PGM Automation - RPA执行器
echo ========================================
echo.
echo 请选择要执行的脚本:
echo 1. 获取PGM (01_fetch_pgm.py)
echo 2. 验证PGM (02_verify_pgm.py)
echo 3. 适用PGM (03_apply_pgm.py)
echo 4. 报警检查 (04_alarm_check.py)
echo 5. 首Lot监控 (05_monitor_lot.py)
echo 6. 执行全部（按顺序）
echo.
set /p choice="请输入选项 (1-6): "

if "%choice%"=="1" (
    python 01_fetch_pgm.py
) else if "%choice%"=="2" (
    python 02_verify_pgm.py
) else if "%choice%"=="3" (
    python 03_apply_pgm.py
) else if "%choice%"=="4" (
    python 04_alarm_check.py
) else if "%choice%"=="5" (
    python 05_monitor_lot.py
) else if "%choice%"=="6" (
    echo 开始执行所有脚本...
    python 01_fetch_pgm.py
    timeout /t 5
    python 02_verify_pgm.py
    timeout /t 5
    python 03_apply_pgm.py
    timeout /t 5
    python 04_alarm_check.py
    timeout /t 5
    python 05_monitor_lot.py
) else (
    echo 无效选项
)

pause
'''

        batch_file = self.prod_root / 'run_pgm.bat'
        with open(batch_file, 'w', encoding='utf-8') as f:
            f.write(batch_content)

        print(f"✅ 批处理文件生成完成: {batch_file}")


if __name__ == "__main__":
    # 使用示例
    builder = ScriptBuilder(
        dev_root="D:/Python/RPA Common/MTE_PGM_Automation/dev",
        prod_root="D:/Python/RPA Common/MTE_PGM_Automation/prod"
    )
    builder.build_all()