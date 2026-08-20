@echo off
echo ==========================================
echo  Wiki Changeset Streaming - Consumer
echo ==========================================

SET PROJECT_DIR=%~dp0
SET SPARK_HOME=C:\Users\LENOVO\Desktop\venv\Lib\site-packages\pyspark
SET PYSPARK_PYTHON=C:\Users\LENOVO\Desktop\venv\Scripts\python.exe
SET PYSPARK_DRIVER_PYTHON=C:\Users\LENOVO\Desktop\venv\Scripts\python.exe
SET HADOOP_HOME=C:\hadoop
SET PATH=%PATH%;C:\hadoop\bin
SET TOPIC_NAME=%1

IF "%TOPIC_NAME%"=="" (
    echo [ERROR] No topic name provided.
    echo Usage: consumer_start.bat topic_name
    echo Example: consumer_start.bat raw_wiki_changes
    pause
    exit /b 1
)

echo.
echo [1/1] Starting Spark Consumer for topic: %TOPIC_NAME%...
start "Spark Consumer - %TOPIC_NAME%" cmd /k "cd /d %PROJECT_DIR% && set KAFKA_TOPIC=%TOPIC_NAME% && spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 consumer.py"

echo.
echo ==========================================
echo  Consumer started!
echo  - Kafka  : 127.0.0.1:9092
echo  - Topic  :