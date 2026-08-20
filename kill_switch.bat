@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Shutdown
echo ==========================================

echo.
echo [1/2] Stopping Kafka gracefully...
call C:\kafka\bin\windows\kafka-server-stop.bat
echo Waiting for Kafka to deregister from Zookeeper...
timeout /t 10 /nobreak >nul

echo.
echo [2/2] Stopping Zookeeper...
call C:\kafka\bin\windows\zookeeper-server-stop.bat
timeout /t 5 /nobreak >nul

echo.
echo ==========================================
echo  All services stopped cleanly.
echo ==========================================
pause