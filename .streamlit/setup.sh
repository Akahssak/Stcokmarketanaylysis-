#!/bin/bash
# Streamlit setup script to install Java OpenJDK 11

echo "Updating package lists..."
apt-get update

echo "Installing OpenJDK 11..."
apt-get install -y openjdk-11-jdk

echo "Setting JAVA_HOME environment variable..."
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

echo "Java installation and setup complete."
