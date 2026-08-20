@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Create Topic
echo ==========================================

SET KAFKA_HOME=C:\kafka
SET TOPIC_NAME=%1
SET PARTITIONS=%2
SET REPLICATION=%3

IF "%TOPIC_NAME%"=="" (
    echo [ERROR] No topic name provided.
    echo Usage: create_topic.bat topic_name partitions replication_factor
    echo Example: create_topic.bat raw_wiki_changes 1 1
    pause
    exit /b 1
)

IF "%PARTITIONS%"=="" SET PARTITIONS=1
IF "%REPLICATION%"=="" SET REPLICATION=1

echo.
echo Creating topic: %TOPIC_NAME% (partitions=%PARTITIONS%, replication=%REPLICATION%)
call %KAFKA_HOME%\bin\windows\kafka-topics.bat --create ^
  --topic %TOPIC_NAME% ^
  --bootstrap-server 127.0.0.1:9092 ^
  --partitions %PARTITIONS% ^
  --replication-factor %REPLICATION% ^
  --if-not-exists
echo Topic %TOPIC_NAME% ready.
pause