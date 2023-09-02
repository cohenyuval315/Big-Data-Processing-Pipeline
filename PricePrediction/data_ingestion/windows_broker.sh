#!/bin/bash

KAFKA_HOME="D:/kafka/kafka_2.13-3.5.0"

BROKER_BIN_PATH="$KAFKA_HOME/bin/windows/kafka-server-start.bat"
BROKER_CONFIG_PATH="$KAFKA_HOME/config/server.properties"


COMMAND="$BROKER_BIN_PATH  $BROKER_CONFIG_PATH"
echo $COMMAND
cmd.exe /c $COMMAND

#kafka\kafka_2.13-3.5.0\bin\windows\kafka-server-start.bat kafka\kafka_2.13-3.5.0\config\server.properties

