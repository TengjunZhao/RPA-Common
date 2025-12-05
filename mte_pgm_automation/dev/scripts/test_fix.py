"""
测试修复后的基础框架
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)

from utils.config_loader import get_config
from utils.logger import get_pgm_logger, get_module_logger
from utils.db_connection import test_database_connection


def test_config():
    """测试配置系统"""
    print("测试配置系统...")
    config = get_config()

    print(f"✅ 当前环境: {config.get_current_environment()}")
    print(f"✅ 数据库配置: {config.get_database_config().get('host')}")
    print(f"✅ 数据库名称: {config.get_database_config().get('database')}")

    return True


def test_logging():
    """测试日志系统"""
    print("\n测试日志系统...")

    # 测试 PGMLogger
    pgm_logger = get_pgm_logger()
    pgm_logger.info("这是一条PGMLogger.info消息")
    pgm_logger.error("这是一条PGMLogger.error消息")

    # 测试模块日志器
    test_logger = get_module_logger("test_module")
    test_logger.info("这是一条模块日志器消息")
    test_logger.warning("这是一条警告消息")

    # 测试专用日志方法
    pgm_logger.log_execution_start("test_script", param1="value1")
    pgm_logger.log_execution_end("test_script", success=True, duration=1.5)

    return True


def test_database():
    """测试数据库连接"""
    print("\n测试数据库连接...")

    if test_database_connection():
        print("✅ 数据库连接测试成功")
        return True
    else:
        print("❌ 数据库连接测试失败")
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("基础框架修复测试")
    print("=" * 60)

    tests_passed = 0
    total_tests = 3

    try:
        if test_config():
            tests_passed += 1

        if test_logging():
            tests_passed += 1

        if test_database():
            tests_passed += 1

        print("\n" + "=" * 60)
        print(f"测试结果: {tests_passed}/{total_tests} 通过")

        if tests_passed == total_tests:
            print("🎉 所有测试通过！")
            return 0
        else:
            print("⚠️  部分测试失败")
            return 1

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())