@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Producer
echo ==========================================

SET PROJECT_DIR=%~dp0
SET TOPIC_NAME=%1

IF "%TOPIC_NAME%"=="" (
    echo [ERROR] No topic name provided.
    echo Usage: producer_start.bat topic_name
    echo Example: producer_start.bat raw_wiki_changes
    pause
    exit /b 1
)

echo.
echo [1/1] Starting Wiki Producer for topic: %TOPIC_NAME%...
start "Wiki Producer - %TOPIC_NAME%" cmd /k "cd /d %PROJECT_DIR% && C:\Users\LENOVO\Desktop\venv\Scripts\python.exe producer.py %TOPIC_NAME%"

echo.
echo ==========================================
echo  Producer started!
echo  - Kafka  : 127.0.0.1:9092
echo  - Topic  : %TOPIC_NAME%
echo  - Producer : running
echo ==========================================
pause