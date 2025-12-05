-- scripts/init_database.sql
-- MTE PGM Automation 数据库初始化脚本
-- 创建时间: 2024-01-20

USE cmsalpha;

-- 1. PGM_MAIN - PGM主表
CREATE TABLE IF NOT EXISTS PGM_MAIN (
    pgm_id VARCHAR(50) PRIMARY KEY COMMENT 'PGM唯一ID = draft_id',
    pgm_type ENUM('AT', 'ET', 'BOTH') NOT NULL COMMENT 'PGM类型',
    status ENUM('NEW', 'DOWNLOADED', 'VERIFY_FAILED', 'VERIFIED', 'APPLY_READY', 'APPLIED', 'MONITORED') DEFAULT 'NEW' COMMENT '当前状态',

    -- 路径信息
    server_path VARCHAR(500) COMMENT '服务器存储根路径',
    ftp_target_path VARCHAR(500) COMMENT 'FTP目标路径',
    path_details JSON COMMENT '详细路径信息(JSON格式)',

    -- 验证信息
    verify_result_code VARCHAR(20) COMMENT '验证结果编码',
    verify_result_desc VARCHAR(200) COMMENT '验证结果描述',
    verify_time DATETIME COMMENT '验证完成时间',
    verify_user VARCHAR(50) COMMENT '验证操作人(=Step2完成用户)',

    -- 适用信息
    apply_flag BOOLEAN DEFAULT FALSE COMMENT '是否标记为可适用',
    apply_time DATETIME COMMENT '适用完成时间',
    apply_user VARCHAR(50) COMMENT '适用操作人',
    ftp_success BOOLEAN DEFAULT FALSE COMMENT 'FTP上传是否成功',

    -- 监控信息
    monitor_flag BOOLEAN DEFAULT FALSE COMMENT '是否已监控',
    monitor_time DATETIME COMMENT '监控完成时间',
    monitor_yield DECIMAL(5,2) COMMENT '首lot良率',
    monitor_test_time DECIMAL(8,2) COMMENT '首lot测试时间(秒)',

    -- 时间点记录
    step1_time DATETIME COMMENT 'Step1开始时间',
    step2_time DATETIME COMMENT 'Step2开始时间',
    step3_time DATETIME COMMENT 'Step3开始时间',
    step4_time DATETIME COMMENT 'Step4开始时间',
    tat_marking ENUM('Normal', 'Notice', 'Warning', 'Alarm') DEFAULT 'Normal' COMMENT 'TAT标记',

    -- 产品信息
    fab VARCHAR(20),
    tech VARCHAR(20),
    mod_type VARCHAR(50),
    grade VARCHAR(20),
    pkg VARCHAR(50),
    density VARCHAR(20),

    -- 任务状态
    next_task ENUM('DOWNLOAD', 'VERIFY', 'APPLY', 'MONITOR', 'NONE') DEFAULT 'DOWNLOAD' COMMENT '下一个待处理任务',

    -- 系统字段
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',

    -- 索引
    INDEX idx_status (status),
    INDEX idx_pgm_type (pgm_type),
    INDEX idx_apply_flag (apply_flag),
    INDEX idx_next_task (next_task),
    INDEX idx_verify_time (verify_time),
    INDEX idx_apply_time (apply_time),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='PGM主表';

-- 2. PGM_OMS_HISTORY - OMS状态历史表
CREATE TABLE IF NOT EXISTS PGM_OMS_HISTORY (
    draft_id VARCHAR(50) NOT NULL COMMENT 'draft_id',
    work_type_desc VARCHAR(100) NOT NULL COMMENT '工作类型描述',
    process_id VARCHAR(50) COMMENT '流程ID',

    -- OMS工作流信息
    work_type_no INT COMMENT '工作类型编号(1-4)',
    work_status VARCHAR(50) COMMENT '工作状态',
    work_start_tm VARCHAR(20) COMMENT '工作开始时间(OMS格式)',
    complete_yn VARCHAR(20) COMMENT '完成状态',

    -- 用户信息
    user_id VARCHAR(50) COMMENT '用户ID',
    user_name VARCHAR(50) COMMENT '用户姓名',

    -- 其他关键字段
    fac_id VARCHAR(50),
    process_name VARCHAR(255),
    process_status_code VARCHAR(50),

    -- TAT相关
    tat_days FLOAT COMMENT 'TAT天数',
    tat_marking VARCHAR(50) COMMENT 'TAT标记',
    info_object VARCHAR(100) COMMENT '信息对象';

    -- 时间戳
    fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '获取时间',

    -- 复合主键
    PRIMARY KEY (draft_id, work_type_desc),

    -- 索引
    INDEX idx_work_type_no (work_type_no),
    INDEX idx_fetched_at (fetched_at),
    INDEX idx_complete_yn (complete_yn),
    INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='OMS状态历史表';

-- 3. PGM_ALARM_HISTORY - 报警历史表
CREATE TABLE IF NOT EXISTS PGM_ALARM_HISTORY (
    alarm_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '报警ID',
    pgm_id VARCHAR(50) NOT NULL COMMENT '关联的PGM ID',
    alarm_type ENUM('TAT_TIMEOUT') NOT NULL COMMENT '报警类型',
    alarm_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '报警时间',

    -- 报警内容
    alarm_message TEXT COMMENT '报警消息',

    -- 处理状态
    resolved BOOLEAN DEFAULT FALSE COMMENT '是否已处理',
    resolved_time DATETIME COMMENT '处理时间',
    resolved_by VARCHAR(50) COMMENT '处理人',

    -- 索引
    INDEX idx_pgm_id (pgm_id),
    INDEX idx_alarm_time (alarm_time),
    INDEX idx_resolved (resolved)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='报警历史表';

-- 删除现有触发器（如果存在）
DROP TRIGGER IF EXISTS UpdateNextTaskOnStatusChange;
DROP TRIGGER IF EXISTS UpdateVerifyUserFromOMS;

-- 插入示例数据
INSERT IGNORE INTO PGM_MAIN (pgm_id, pgm_type, status, server_path, next_task)
VALUES
('DRAFT_2024001', 'AT', 'NEW', '\\\\172.27.7.188\\Mod_TestE\\23. PGM Automation\\DRAFT_2024001', 'DOWNLOAD'),
('DRAFT_2024002', 'ET', 'DOWNLOADED', '\\\\172.27.7.188\\Mod_TestE\\23. PGM Automation\\DRAFT_2024002', 'VERIFY'),
('DRAFT_2024003', 'BOTH', 'VERIFIED', '\\\\172.27.7.188\\Mod_TestE\\23. PGM Automation\\DRAFT_2024003', 'NONE');

INSERT IGNORE INTO PGM_OMS_HISTORY (draft_id, work_type_desc, work_type_no, user_name, complete_yn)
VALUES
('DRAFT_2024001', '[1Step] 기안', 1, '홍길동', '진행 중'),
('DRAFT_2024002', '[2Step] 외주사 결과', 2, '김철수', '완료'),
('DRAFT_2024003', '[3Step] 최종승인', 3, '이영희', '완료');

SELECT '✅ 数据库初始化完成' as Message;