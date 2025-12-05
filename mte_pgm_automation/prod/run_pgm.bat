@echo off
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
