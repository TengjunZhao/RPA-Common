"""
修复后的示例主程序 - 解决主键冲突问题
"""
import sys
import os
from datetime import datetime
import time

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)

from utils.config_loader import get_config, reload_config
from utils.logger import get_pgm_logger, get_module_logger
from utils.db_connection import get_db_pool, test_database_connection, execute_query
from database.models import create_tables, PGMMain, PGMStatus, NextTask, PGMType
from database.repositories import PGMMainRepository


def cleanup_test_data():
    """清理测试数据"""
    try:
        # 删除测试数据
        sql1 = "DELETE FROM pgm_main WHERE pgm_id LIKE 'TEST%' OR pgm_id LIKE 'DEMO%'"
        execute_query(sql1, fetch=False)
        print("✅ 清理测试数据完成")
    except Exception as e:
        print(f"⚠️  清理测试数据时出错: {str(e)}")


def demonstrate_config_usage():
    """演示配置使用"""
    print("=" * 60)
    print("配置使用演示")
    print("=" * 60)

    # 获取配置实例
    config = get_config()

    # 获取当前环境
    env = config.get_current_environment()
    print(f"当前环境: {env}")

    # 获取数据库配置
    db_config = config.get_database_config()
    print(f"数据库主机: {db_config.get('host')}")
    print(f"数据库名称: {db_config.get('database')}")

    # 获取文件路径
    paths = config.get_file_paths()
    print(f"验证文件路径: {paths.get('local_verify')}")

    return True


def demonstrate_logging_usage():
    """演示日志使用"""
    print("\n" + "=" * 60)
    print("日志使用演示")
    print("=" * 60)

    # 获取日志器
    pgm_logger = get_pgm_logger()
    module_logger = get_module_logger("demonstrate_logging")

    # 测试不同级别的日志
    module_logger.debug("调试信息 - 通常不会显示")
    module_logger.info("信息级别日志")
    module_logger.warning("警告级别日志")
    module_logger.error("错误级别日志")

    # 测试专用日志方法
    pgm_logger.log_execution_start("日志演示", stage="testing")
    pgm_logger.log_file_operation("COPY", src="/tmp/source.txt", dest="/tmp/dest.txt", size=1024)
    pgm_logger.log_database_operation("SELECT", "pgm_main", 10, "查询待处理PGM")
    pgm_logger.log_execution_end("日志演示", success=True, duration=0.5)

    return True


def demonstrate_database_usage():
    """演示数据库使用 - 使用唯一ID避免冲突"""
    print("\n" + "=" * 60)
    print("数据库使用演示")
    print("=" * 60)

    # 测试数据库连接
    if test_database_connection():
        print("✅ 数据库连接测试成功")
    else:
        print("❌ 数据库连接测试失败")
        return False

    # 使用仓库类
    repo = PGMMainRepository()

    try:
        # 1. 获取所有待下载的PGM
        pending_pgms = repo.get_ready_for_download()
        print(f"📊 找到 {len(pending_pgms)} 个待下载的PGM")

        # 2. 创建新的PGM记录（使用时间戳确保唯一）
        timestamp = int(time.time())
        test_id = f"TEST_{timestamp}"

        new_pgm = {
            'pgm_id': test_id,
            'pgm_type': PGMType.ET,
            'status': PGMStatus.NEW,
            'next_task': NextTask.DOWNLOAD,
            'server_path': '/path/to/pgm',
            'fab': 'TEST',
            'tech': 'TEST',
            'mod_type': 'TEST_MODEL'
        }

        created = repo.create(new_pgm)
        if created:
            print(f"✅ 成功创建PGM记录: {created.pgm_id}")
        else:
            print(f"❌ 创建PGM记录失败: {test_id}")
            return False

        # 3. 更新PGM状态
        if repo.update_status(test_id, PGMStatus.DOWNLOADED, NextTask.VERIFY):
            print(f"✅ 更新PGM状态为: DOWNLOADED")
        else:
            print(f"⚠️  更新PGM状态失败")

        # 4. 设置验证结果
        if repo.set_verify_result(test_id, "SUCCESS", "自动验证通过", "system"):
            print(f"✅ 设置验证结果成功")

        # 5. 获取TAT超时的PGM
        timeout_pgms = repo.get_tat_timeout_pgms(168)  # 7天阈值
        print(f"📊 找到 {len(timeout_pgms)} 个TAT超时的PGM")

        if timeout_pgms:
            for pgm in timeout_pgms[:3]:  # 只显示前3个
                print(f"  - {pgm.pgm_id}: {pgm.status}")

        return True

    except Exception as e:
        print(f"❌ 数据库演示失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        repo.close()


def demonstrate_full_workflow():
    """演示完整工作流程"""
    print("\n" + "=" * 60)
    print("完整工作流程演示")
    print("=" * 60)

    # 获取日志器
    pgm_logger = get_pgm_logger()
    workflow_logger = get_module_logger("workflow_demo")

    pgm_logger.log_execution_start("完整工作流程演示", environment="test")
    start_time = datetime.now()

    try:
        # 1. 加载配置
        config = get_config()
        workflow_logger.info(f"配置加载完成 - 环境: {config.get_current_environment()}")

        # 2. 测试数据库连接
        if not test_database_connection():
            workflow_logger.error("数据库连接失败，流程终止")
            return False

        workflow_logger.info("✅ 数据库连接成功")

        # 3. 模拟PGM处理流程
        repo = PGMMainRepository()

        # 使用时间戳确保ID唯一
        timestamp = int(time.time())
        demo_id = f"DEMO_{timestamp}"

        # 模拟新PGM到达
        new_pgm = {
            'pgm_id': demo_id,
            'pgm_type': PGMType.AT,
            'status': PGMStatus.NEW,
            'next_task': NextTask.DOWNLOAD,
            'fab': 'DEMO_FAB',
            'tech': 'DEMO_TECH',
            'mod_type': 'DEMO_MODULE',
            'grade': 'A',
            'pkg': 'DEMO_PACKAGE'
        }

        pgm = repo.create(new_pgm)
        if not pgm:
            workflow_logger.error("创建PGM记录失败")
            return False

        workflow_logger.info(f"✅ PGM记录创建: {pgm.pgm_id}")

        # 模拟下载完成
        if repo.update_status(pgm.pgm_id, PGMStatus.DOWNLOADED, NextTask.VERIFY):
            workflow_logger.info(f"✅ PGM下载完成: {pgm.pgm_id}")

        # 模拟验证完成
        if repo.set_verify_result(pgm.pgm_id, "SUCCESS", "验证通过，HESS文件匹配", "auto_system"):
            workflow_logger.info(f"✅ PGM验证完成: {pgm.pgm_id}")

        if repo.update_status(pgm.pgm_id, PGMStatus.VERIFIED, NextTask.APPLY):
            workflow_logger.info(f"✅ PGM状态更新为已验证: {pgm.pgm_id}")

        # 模拟用户触发适用
        if repo.set_apply_flag(pgm.pgm_id, True, "test_user"):
            workflow_logger.info(f"✅ 用户触发PGM适用: {pgm.pgm_id}")

        # 获取待适用的PGM
        ready_to_apply = repo.get_ready_for_apply()
        workflow_logger.info(f"📊 找到 {len(ready_to_apply)} 个待适用的PGM")

        # 模拟FTP上传成功
        if repo.set_ftp_success(pgm.pgm_id, True):
            workflow_logger.info(f"✅ FTP上传成功: {pgm.pgm_id}")

        # 显示PGM最终状态
        updated_pgm = repo.get_by_id(pgm.pgm_id)
        if updated_pgm:
            workflow_logger.info(f"📋 PGM最终状态: {updated_pgm.status}, 下一个任务: {updated_pgm.next_task}")

        # 关闭仓库
        repo.close()

        duration = (datetime.now() - start_time).total_seconds()
        pgm_logger.log_execution_end("完整工作流程演示", success=True, duration=duration)

        print(f"\n🎉 完整工作流程演示完成!")
        print(f"📊 总耗时: {duration:.2f}秒")
        print(f"📋 演示PGM ID: {demo_id}")

        return True

    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        pgm_logger.log_execution_end("完整工作流程演示", success=False, duration=duration, message=str(e))
        workflow_logger.error(f"工作流程执行失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def demonstrate_repository_methods():
    """演示仓库类的各种方法"""
    print("\n" + "=" * 60)
    print("仓库类方法演示")
    print("=" * 60)

    repo = PGMMainRepository()

    try:
        # 1. 按状态查询
        print("1. 按状态查询PGM:")
        for status in [PGMStatus.NEW, PGMStatus.VERIFIED, PGMStatus.APPLIED]:
            pgms = repo.get_by_status(status)
            print(f"  - {status.value}: {len(pgms)} 个")

        # 2. 按任务查询
        print("\n2. 按下一个任务查询PGM:")
        for task in [NextTask.DOWNLOAD, NextTask.VERIFY, NextTask.APPLY, NextTask.MONITOR]:
            pgms = repo.get_by_next_task(task)
            print(f"  - {task.value}: {len(pgms)} 个")

        # 3. 专用查询方法
        print("\n3. 专用查询方法:")
        print(f"  - 待下载: {len(repo.get_ready_for_download())} 个")
        print(f"  - 待验证: {len(repo.get_ready_for_verify())} 个")
        print(f"  - 待适用: {len(repo.get_ready_for_apply())} 个")
        print(f"  - 待监控: {len(repo.get_ready_for_monitor())} 个")

        return True

    except Exception as e:
        print(f"❌ 仓库方法演示失败: {str(e)}")
        return False
    finally:
        repo.close()


def main():
    """主函数"""
    print("\n" + "=" * 60)
    print("MTE PGM Automation 基础框架演示 (修复版)")
    print("=" * 60)

    # 清理旧的测试数据
    cleanup_test_data()

    # 演示计数
    passed = 0
    total = 4

    try:
        # 演示各个模块的使用
        if demonstrate_config_usage():
            passed += 1

        if demonstrate_logging_usage():
            passed += 1

        if demonstrate_database_usage():
            passed += 1

        if demonstrate_repository_methods():
            passed += 1

        # 完整工作流程演示
        print("\n" + "=" * 60)
        print("完整工作流程演示")
        print("=" * 60)

        if demonstrate_full_workflow():
            passed += 1
            total += 1

        # 汇总结果
        print("\n" + "=" * 60)
        print("演示结果汇总")
        print("=" * 60)
        print(f"✅ 通过: {passed}/{total}")

        if passed == total:
            print("🎉 所有演示成功完成！")
        else:
            print("⚠️  部分演示失败")

        return 0

    except Exception as e:
        print(f"\n❌ 演示过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())