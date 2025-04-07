#!/bin/bash

# Create jars directory if it doesn't exist
mkdir -p jars

# Download MySQL Connector
echo "Downloading MySQL Connector..."
curl -L -o jars/mysql-connector-java-8.0.28.jar \
  https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar

# Download Hadoop AWS
echo "Downloading Hadoop AWS..."
curl -L -o jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Download AWS Java SDK Bundle
echo "Downloading AWS Java SDK Bundle..."
curl -L -o jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo "All JAR files downloaded successfully!"
echo "JAR files in jars/ directory:"
ls -la jars/ 