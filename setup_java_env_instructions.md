# Setting Up JAVA_HOME Environment Variable for PySpark

The PySpark error you encountered indicates that the Java gateway process could not start because the JAVA_HOME environment variable is not set. To fix this, please follow these steps:

## Step 1: Verify Java Installation

- Ensure you have Java JDK installed on your system.
- You can check by running in your terminal or command prompt:
  ```
  java -version
  ```
- If Java is not installed, download and install the latest JDK from:
  https://www.oracle.com/java/technologies/javase-jdk11-downloads.html

## Step 2: Set JAVA_HOME Environment Variable

### On Windows

1. Find the path to your JDK installation directory, e.g. `C:\Program Files\Java\jdk-11.0.12`
2. Open the Start menu and search for "Environment Variables"
3. Click "Edit the system environment variables"
4. Click "Environment Variables" button
5. Under "System variables", click "New"
6. Enter `JAVA_HOME` as the variable name
7. Enter the JDK path as the variable value
8. Click OK to save

### On Linux/macOS

Add the following line to your shell profile file (`~/.bashrc`, `~/.zshrc`, etc.):

```bash
export JAVA_HOME=/path/to/your/jdk
export PATH=$JAVA_HOME/bin:$PATH
```

Replace `/path/to/your/jdk` with the actual path.

Then reload the profile:

```bash
source ~/.bashrc
```

## Step 3: Verify JAVA_HOME is Set

Run:

```bash
echo $JAVA_HOME
```

(or `echo %JAVA_HOME%` on Windows) to confirm it is set correctly.

## Step 4: Restart Your Development Environment

- Close and reopen your terminal or IDE.
- Restart the Streamlit app.

## Additional Notes

- The error also mentioned `ps: command not found`. On Windows, this is normal as `ps` is a Unix command. PySpark should handle this, but if you run into issues, consider using Windows Subsystem for Linux (WSL) or a Linux environment.
- Ensure your Java version is compatible with your PySpark version.

---

If you want, I can help you add debug logs in your code to verify data loading once the environment is fixed.
