#!/bin/bash

KAFKA_HOME="D:/kafka/kafka_2.13-3.5.0"

ZOOKEEPER_BIN_PATH="$KAFKA_HOME/bin/windows/zookeeper-server-start.bat"
ZOOKEEPER_CONFIG_PATH="$KAFKA_HOME/config/zookeeper.properties"


COMMAND="$ZOOKEEPER_BIN_PATH  $ZOOKEEPER_CONFIG_PATH"
echo $COMMAND
cmd.exe /c $COMMAND

#kafka\kafka_2.13-3.5.0\bin\windows\zookeeper-server-start.bat kafka\kafka_2.13-3.5.0\config\zookeeper.properties
