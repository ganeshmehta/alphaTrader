@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Startup
echo ==========================================

SET KAFKA_HOME=C:\kafka
SET PROJECT_DIR=%~dp0
SET VENV_DIR=C:\Users\LENOVO\Desktop\venv\Scripts\activate.bat

echo.
echo [0/5] Cleaning up stale data...
taskkill /F /IM java.exe /T >nul 2>&1
timeout /t 3 /nobreak >nul
if exist "C:\tmp\kafka-logs" rmdir /s /q "C:\tmp\kafka-logs"
if exist "C:\tmp\zookeeper" rmdir /s /q "C:\tmp\zookeeper"
if exist "C:\kafka\logs" rmdir /s /q "C:\kafka\logs"
echo Cleanup done.

echo.
echo [1/5] Starting Zookeeper...
start "Zookeeper" cmd /k "%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties"

echo Waiting for Zookeeper to be ready (15s)...
timeout /t 15 /nobreak >nul

echo.
echo [2/5] Starting Kafka Broker...
start "Kafka Broker" cmd /k "%KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties --override advertised.listeners=PLAINTEXT://127.0.0.1:9092 --override listeners=PLAINTEXT://127.0.0.1:9092"

echo Waiting for Kafka Broker to be ready (60s)...
timeout /t 60 /nobreak >nul

echo.
echo [3/5] Creating Kafka topic if not exists...
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create --topic raw_wiki_changes --bootstrap-server 127.0.0.1:9092 --partitions 1 --replication-factor 1 --if-not-exists
echo Topic ready.

echo.
echo [4/5] Verifying Kafka is up...
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --describe --topic raw_wiki_changes --bootstrap-server 127.0.0.1:9092 >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Kafka is not reachable. Check Kafka window for errors.
    pause
    exit /b 1
)
echo Kafka is reachable, proceeding...

echo.
echo [5/5] Starting Wiki Producer...
start "Wiki Producer" cmd /k "cd /d %PROJECT_DIR% && C:\Users\LENOVO\Desktop\venv\Scripts\python.exe producer.py"

echo.
echo ==========================================
echo  All services started successfully!
echo  - Zookeeper : localhost:2181
echo  - Kafka     : localhost:9092
echo  - Producer  : running
echo ==========================================
pause