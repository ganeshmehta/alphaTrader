@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Infra
echo ==========================================

SET KAFKA_HOME=C:\kafka

echo.
echo [1/3] Cleaning up stale data...
taskkill /F /IM java.exe /T >nul 2>&1
timeout /t 3 /nobreak >nul
if exist "C:\tmp\kafka-logs" rmdir /s /q "C:\tmp\kafka-logs"
if exist "C:\tmp\zookeeper" rmdir /s /q "C:\tmp\zookeeper"
echo Cleanup done.

echo.
echo [2/3] Starting Zookeeper...
start "Zookeeper" cmd /k "%KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties"
echo Waiting for Zookeeper to be ready (15s)...
timeout /t 15 /nobreak >nul

echo.
echo [3/3] Starting Kafka Broker...
start "Kafka Broker" cmd /k "set KAFKA_HEAP_OPTS=-Xmx1G -Xms1G && %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties --override advertised.listeners=PLAINTEXT://127.0.0.1:9092 --override listeners=PLAINTEXT://127.0.0.1:9092"
echo Waiting for Kafka Broker to be ready (60s)...
timeout /t 60 /nobreak >nul

echo.
echo ==========================================
echo  Infrastructure started!
echo  - Zookeeper : 127.0.0.1:2181
echo  - Kafka     : 127.0.0.1:9092
echo ==========================================
pause