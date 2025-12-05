"""
测试字符集修复方案
"""
import sys
import os

# 添加项目根目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # scripts目录
dev_dir = os.path.dirname(current_dir)  # dev目录
project_root = os.path.dirname(dev_dir)  # 项目根目录

sys.path.insert(0, dev_dir)
sys.path.insert(0, project_root)

from database.sqlalchemy_manager import get_sqlalchemy_manager, execute_sql
from database.repositories import PGMMainRepository
from database.models import PGMStatus, NextTask, PGMType
import time


def test_sqlalchemy_connection():
    """测试SQLAlchemy连接"""
    print("=" * 60)
    print("测试SQLAlchemy连接")
    print("=" * 60)

    manager = get_sqlalchemy_manager()

    # 测试连接
    if manager.test_connection():
        print("✅ SQLAlchemy连接测试成功")
    else:
        print("❌ SQLAlchemy连接测试失败")
        return False

    # 检查字符集
    sql = "SELECT @@character_set_connection, @@collation_connection"
    result = execute_sql(sql)

    if result:
        charset, collation = result[0]
        print(f"📊 当前连接字符集: {charset}")
        print(f"📊 当前连接排序规则: {collation}")

        if collation == 'utf8mb4_0900_ai_ci':
            print("✅ 排序规则匹配成功")
            return True
        else:
            print("⚠️  排序规则不匹配")
            return False

    return True


def test_repository_operations():
    """测试仓库类操作"""
    print("\n" + "=" * 60)
    print("测试仓库类操作")
    print("=" * 60)

    repo = PGMMainRepository()

    try:
        # 1. 创建测试记录
        timestamp = int(time.time())
        test_id = f"FIX_TEST_{timestamp}"

        new_pgm = {
            'pgm_id': test_id,
            'pgm_type': PGMType.ET.value,
            'status': PGMStatus.NEW.value,
            'next_task': NextTask.DOWNLOAD.value,
            'fab': 'TEST',
            'tech': 'TEST',
            'mod_type': 'FIX_TEST'
        }

        created = repo.create(new_pgm)
        if created:
            print(f"✅ 创建记录成功: {test_id}")
        else:
            print(f"❌ 创建记录失败")
            return False

        # 2. 更新状态
        if repo.update_status(test_id, PGMStatus.DOWNLOADED, NextTask.VERIFY):
            print(f"✅ 更新状态成功: DOWNLOADED")
        else:
            print(f"❌ 更新状态失败")
            return False

        # 3. 设置验证结果
        if repo.set_verify_result(test_id, "SUCCESS", "修复测试通过", "system"):
            print(f"✅ 设置验证结果成功")
        else:
            print(f"❌ 设置验证结果失败")
            return False

        # 4. 再次更新状态
        if repo.update_status(test_id, PGMStatus.VERIFIED, NextTask.APPLY):
            print(f"✅ 再次更新状态成功: VERIFIED")
        else:
            print(f"❌ 再次更新状态失败")
            return False

        # 5. 验证记录存在
        pgm = repo.get_by_id(test_id)
        if pgm:
            print(f"✅ 查询记录成功: 状态={pgm.status}, 任务={pgm.next_task}")
            return True
        else:
            print(f"❌ 查询记录失败")
            return False

    except Exception as e:
        print(f"❌ 仓库操作测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        repo.close()


def test_raw_sql_operations():
    """测试原始SQL操作"""
    print("\n" + "=" * 60)
    print("测试原始SQL操作")
    print("=" * 60)

    try:
        # 1. 创建测试记录
        timestamp = int(time.time())
        test_id = f"RAW_TEST_{timestamp}"

        sql = """
        INSERT INTO pgm_main 
        (pgm_id, pgm_type, status, next_task, fab, tech, mod_type, created_at, updated_at)
        VALUES 
        (:pgm_id, 'ET', 'NEW', 'DOWNLOAD', 'TEST', 'TEST', 'RAW_TEST', NOW(), NOW())
        """

        result = execute_sql(sql, {'pgm_id': test_id})
        print(f"✅ 原始SQL插入成功: {test_id}")

        # 2. 更新记录
        update_sql = """
        UPDATE pgm_main 
        SET status = 'DOWNLOADED', next_task = 'VERIFY', updated_at = NOW()
        WHERE pgm_id = :pgm_id
        """

        result = execute_sql(update_sql, {'pgm_id': test_id})
        print(f"✅ 原始SQL更新成功: 影响 {result} 行")

        # 3. 查询记录
        select_sql = "SELECT * FROM pgm_main WHERE pgm_id = :pgm_id"
        result = execute_sql(select_sql, {'pgm_id': test_id})

        if result:
            print(f"✅ 原始SQL查询成功: 找到 {len(result)} 条记录")
            return True
        else:
            print(f"❌ 原始SQL查询失败")
            return False

    except Exception as e:
        print(f"❌ 原始SQL操作测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("=" * 60)
    print("字符集修复方案测试")
    print("=" * 60)

    tests_passed = 0
    total_tests = 3

    try:
        if test_sqlalchemy_connection():
            tests_passed += 1

        if test_repository_operations():
            tests_passed += 1

        if test_raw_sql_operations():
            tests_passed += 1

        print("\n" + "=" * 60)
        print(f"测试结果: {tests_passed}/{total_tests} 通过")

        if tests_passed == total_tests:
            print("🎉 所有测试通过！字符集问题已解决")
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