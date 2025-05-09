import os
import subprocess

def check_java_home():
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        print(f"JAVA_HOME is set to: {java_home}")
    else:
        print("JAVA_HOME is not set.")

def set_java_home(java_home_path):
    os.environ["JAVA_HOME"] = java_home_path
    print(f"JAVA_HOME set to: {java_home_path}")

def check_java_version():
    try:
        result = subprocess.run(["java", "-version"], capture_output=True, text=True)
        print("Java version info:")
        print(result.stderr)
    except FileNotFoundError:
        print("Java is not installed or not found in PATH.")

def main():
    check_java_home()
    check_java_version()
    # Example path, adjust as needed
    example_java_home = "/usr/lib/jvm/java-11-openjdk-amd64"
    set_java_home(example_java_home)
    check_java_home()

if __name__ == "__main__":
    main()
