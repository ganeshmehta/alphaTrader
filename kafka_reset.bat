@echo off
echo Stopping all Kafka and Zookeeper processes...
taskkill /F /IM java.exe /T
echo All services stopped.
pause