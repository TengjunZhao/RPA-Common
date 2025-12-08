"""
运行脚本 - 正确设置环境并运行file_processor
"""
import sys
import os
from pathlib import Path

# 添加项目路径到Python路径
project_dir = Path(__file__)
sys.path.insert(0, str(project_dir))
sys.path.insert(0, str(project_dir / "dev"))

print("Python搜索路径:")
for i, path in enumerate(sys.path):
    print(f"  {i}: {path}")

# 现在可以正常导入core.file_processor
try:
    from core.file_processor import FileProcessor
    print("✅ 成功导入 FileProcessor")
except Exception as e:
    print(f"❌ 导入失败: {e}")
    import traceback
    traceback.print_exc()