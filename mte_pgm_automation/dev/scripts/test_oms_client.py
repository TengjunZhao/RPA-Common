"""
OMS客户端测试脚本
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

# 添加dev目录到Python路径
sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)

from core.oms_client import test_oms_client

if __name__ == "__main__":
    success = test_oms_client()

    print("\n" + "=" * 60)
    if success:
        print("🎉 OMS客户端测试完成!")
    else:
        print("❌ OMS客户端测试失败!")
    print("=" * 60)

    sys.exit(0 if success else 1)