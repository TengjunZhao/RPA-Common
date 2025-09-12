import json
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote

# 数据库模型基类
Base = declarative_base()


# OMS PGM数据模型（映射db_oms_pgm表）
class OmsPgm(Base):
    __tablename__ = 'db_oms_pgm'

    draft_id = Column(String(50), primary_key=True)
    work_type_desc = Column(String(100), primary_key=True)
    process_id = Column(String(50))
    process_type = Column(String(100))
    process_type_desc = Column(String(200))
    fac_id = Column(String(50))
    process_name = Column(String(255))
    complete_yn = Column(String(20))
    process_status_code = Column(String(50))
    work_type_no = Column(Integer)
    work_prgs_mag_cd = Column(String(20))
    work_sequence = Column(Integer)
    prev_work_sequence = Column(Integer)
    work_type = Column(String(50))
    work_status = Column(String(50))
    organ_name = Column(String(100))
    user_name = Column(String(50))
    user_id = Column(String(50))
    work_start_tm = Column(String(20))
    bp_id = Column(String(100))
    linked_bp_id = Column(String(100))


# work_type_desc与work_type_no映射表
WORK_TYPE_MAP = {
    '[1Step] 기안': 1,
    '[2Step] 외주사 결과': 2,
    '[3Step] 최종승인': 3,
    '[4Step] 양산적용': 4
}


def load_config(config_path):
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        print(f"✅ 配置文件加载成功: {config_path}")
        return config
    except Exception as e:
        print(f"❌ 配置文件加载失败: {str(e)}")
        raise


def login_oms(config):
    """登录OMS系统获取Bearer Token"""
    login_url = "https://apihtts.skhynix.com/auth/sign/in"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0"
    }
    body = {
        "id": config["user_id"],
        "password": config["password"]
    }

    try:
        response = requests.post(login_url, json=body, headers=headers)
        response.raise_for_status()
        token = response.json().get("token")  # 假设返回的token字段为token
        if not token:
            raise ValueError("登录响应中未找到token")
        print(f"✅ OMS登录成功，获取到token")
        return f"Bearer {token}"
    except Exception as e:
        print(f"❌ OMS登录失败: {str(e)}")
        raise


def get_date_params():
    """生成日期参数（beginDate: 当日11天前, endDate: 当日后一天）"""
    today = datetime.now()
    # 计算日期部分（包含时间，格式为标准的yyyy-MM-dd HH:mm:ss）
    begin_date = (today - timedelta(days=11)).strftime("%Y-%m-%d 07:00:00")  # 11天前的日期
    end_date = (today + timedelta(days=1)).strftime("%Y-%m-%d 07:00:00")  # 后一天的日期

    print(f"📅 日期参数: beginDate={begin_date}, endDate={end_date}")
    return begin_date, end_date  # 返回未编码的原始格式


def get_data_list(token):
    """从OMS获取数据列表（修复日期参数格式）"""
    base_url = "https://apihtts.skhynix.com/bpms/test-pgm/module/distribute-status"
    begin_str, end_str = get_date_params()  # 获取未编码的日期字符串

    params = {
        "factoryId": "OSMOD",
        "companyId": "HITECH",
        "beginDate": begin_str,  # 直接使用原始格式，requests会自动处理URL编码
        "endDate": end_str
    }

    headers = {
        "sec-ch-ua-platform": "\"Windows\"",
        "Authorization": token,
        "uiId": "ModuleTestPgmDistributeStatus",
        "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "uiName": "BPM%20%3E%20MOD%20Test%20PGM%20Distribute%20Status",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "host": "apihtts.skhynix.com"
    }

    try:
        # requests会自动对params中的特殊字符（如空格、冒号）进行URL编码
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(f"✅ 成功获取数据列表，共{len(data)}条记录")
        return data
    except Exception as e:
        print(f"❌ 获取数据列表失败: {str(e)}")
        # 打印响应内容帮助调试
        print(f"响应内容: {response.text if 'response' in locals() else '无响应'}")
        raise

def process_data(raw_data):
    """处理原始数据，映射到数据库模型"""
    processed = []
    for item in raw_data:
        # 转换work_type_desc为work_type_no
        work_type_no = WORK_TYPE_MAP.get(item.get("workTypeDesc"), None)

        pgm = OmsPgm(
            process_id=item.get("processId"),
            process_type=item.get("processType"),
            process_type_desc=item.get("processTypeDesc"),
            fac_id=item.get("facId"),
            process_name=item.get("processName"),
            draft_id=item.get("draftId"),
            complete_yn=item.get("completeYn"),
            process_status_code=item.get("processStatusCode"),
            work_type_no=work_type_no,
            work_type_desc=item.get("workTypeDesc"),
            work_prgs_mag_cd=item.get("workPrgsMagCd"),
            work_sequence=item.get("workSequence"),
            prev_work_sequence=item.get("prevWorkSequence"),
            work_type=item.get("workType"),
            work_status=item.get("workStatus"),
            organ_name=item.get("organName"),
            user_name=item.get("userName"),
            user_id=item.get("userId"),
            work_start_tm=item.get("workStartTm"),
            bp_id=item.get("bpId"),
            linked_bp_id=item.get("linkedBpId")
        )
        processed.append(pgm)
    print(f"🔧 数据处理完成，生成{len(processed)}条模型数据")
    return processed


def save_to_db(data, db_config):
    """将数据存入数据库（on duplicate key update）"""
    # 创建数据库引擎
    db_url = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}/cmsalpha"
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for item in data:
            # 先查询是否存在，存在则更新，不存在则插入
            existing = session.query(OmsPgm).filter(
                OmsPgm.draft_id == item.draft_id,
                OmsPgm.work_type_desc == item.work_type_desc
            ).first()

            if existing:
                # 更新现有记录
                for key, value in item.__dict__.items():
                    if key != '_sa_instance_state' and value is not None:
                        setattr(existing, key, value)
            else:
                # 插入新记录
                session.add(item)

        session.commit()
        print(f"✅ 数据成功存入数据库，共处理{len(data)}条记录")
    except Exception as e:
        session.rollback()
        print(f"❌ 数据库操作失败: {str(e)}")
        raise
    finally:
        session.close()


def calculate_tat(work_start_tm):
    """计算TAT（当前时间 - 开始时间，单位：天）"""
    try:
        # 解析work_start_tm（格式：MM/DD HH:MM），暂用当前年份
        current_year = datetime.now().year
        start_str = f"{current_year}-{work_start_tm}"
        start_time = datetime.strptime(start_str, "%Y-%m/%d %H:%M")
        tat = (datetime.now() - start_time).total_seconds() / (24 * 3600)  # 转换为天
        return round(tat, 2)
    except Exception as e:
        print(f"⚠️ TAT计算失败（{work_start_tm}）: {str(e)}")
        return None


def calculate_marking(tat):
    """根据TAT计算marking"""
    if tat is None:
        return "Unknown"
    if tat > 3:
        return "Alarm"
    elif tat > 2:
        return "Warning"
    elif tat > 1:
        return "Notice"
    else:
        return "Normal"


def calculate_info_object(work_type_no):
    """根据work_type_no计算info_object"""
    if work_type_no in [2, 4]:
        return "Hitech"
    else:
        return "All"


def analyze_data(db_config):
    """分析数据，计算TAT、marking和info_object并输出调试信息"""
    db_url = f"mysql+pymysql://{db_config['username']}:{db_config['password']}@{db_config['host']}/cmsalpha"
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # 获取进行中的draft_id
        # 获取进行中的draft_id（适配数据库中"MM/DD HH:MM"格式）
        today = datetime.now()
        # 计算11天前的日期，格式化为"MM/DD HH:MM"（与数据库格式一致）
        begin_date = (today - timedelta(days=11)).strftime("%m/%d 07:00")

        draft_ids = session.query(OmsPgm.draft_id).distinct().filter(
            OmsPgm.work_start_tm > begin_date,
            OmsPgm.complete_yn == "진행 중"
        ).all()
        draft_ids = [d[0] for d in draft_ids]
        print(f"🔍 找到{len(draft_ids)}个进行中的draft_id")

        # 分析每个draft_id
        analysis_result = []
        for draft_id in draft_ids:
            # 获取最新的work_type_no和work_start_tm
            latest_work = session.query(OmsPgm).filter(
                OmsPgm.draft_id == draft_id
            ).order_by(OmsPgm.work_type_no.desc()).first()

            if not latest_work:
                continue

            tat = calculate_tat(latest_work.work_start_tm)
            marking = calculate_marking(tat)
            info_object = calculate_info_object(latest_work.work_type_no)

            analysis_result.append({
                "draft_id": draft_id,
                "work_type_no": latest_work.work_type_no,
                "work_start_tm": latest_work.work_start_tm,
                "tat_days": tat,
                "marking": marking,
                "info_object": info_object
            })

        # 输出分析结果
        print("\n📊 数据分析结果:")
        for res in analysis_result:
            print(
                f"draft_id: {res['draft_id']}, TAT: {res['tat_days']}天, marking: {res['marking']}, info_object: {res['info_object']}")

        return analysis_result
    except Exception as e:
        print(f"❌ 数据分析失败: {str(e)}")
        raise
    finally:
        session.close()


def main(mode):
    """主函数"""
    print("===== 程序开始运行 =====")

    # 配置初始化
    if mode == "test":
        config = {
            "oms_config_path": "config.json",  # 测试环境配置文件路径
            "db": {
                "host": "localhost",
                "username": "remoteuser",
                "password": "password"
            }
        }
    else:
        config = {
            "oms_config_path": "/path/to/config.json",  # 生产环境配置文件路径
            "db": {
                "host": "CMS DB地址",
                "username": "生产用户名",
                "password": "生产密码"
            }
        }

    try:
        # 1. 加载OMS配置
        oms_config = load_config(config["oms_config_path"])

        # 2. 登录OMS获取token
        token = login_oms(oms_config)

        # 3. 获取数据列表
        raw_data = get_data_list(token)

        # 4. 处理数据
        processed_data = process_data(raw_data)

        # 5. 存入数据库
        save_to_db(processed_data, config["db"])

        # 6. 分析数据（计算TAT、marking等）
        analyze_data(config["db"])

        print("===== 程序运行结束 =====")
    except Exception as e:
        print(f"===== 程序运行失败: {str(e)} =====")


if __name__ == "__main__":
    # 测试模式运行（生产环境请改为mode="prod"）
    main(mode="test")