# Environment Setup for PySpark-based Stock Market Analytics on Windows

This document guides you through setting up the environment on Windows 10/11 for running PySpark with Anaconda and Jupyter Notebook.

## 1. Install Java (JDK 8+)

- Download and install Java JDK 8 or higher from [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or [AdoptOpenJDK](https://adoptopenjdk.net/).
- During installation, note the installation path (e.g., `C:\Program Files\Java\jdk-11.0.12`).

## 2. Set JAVA_HOME Environment Variable

- Open **System Properties** > **Advanced** > **Environment Variables**.
- Under **System variables**, click **New**.
- Variable name: `JAVA_HOME`
- Variable value: Path to your JDK installation (e.g., `C:\Program Files\Java\jdk-11.0.12`)
- Click OK and restart your terminal or IDE.

## 3. Install PySpark and Findspark

Open Anaconda Prompt and run:

```bash
pip install pyspark findspark yfinance matplotlib seaborn plotly pandas numpy
```

## 4. Configure PySpark in Jupyter Notebook

In your Python scripts or notebooks, add:

```python
import findspark
findspark.init()
```

This will locate your Spark installation.

## 5. Verify Spark Installation

Create a test script `test_spark.py` with the following content:

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkTest") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version)

spark.stop()
```

Run the script:

```bash
python test_spark.py
```

You should see the Spark version printed, confirming the setup.

---

This completes the environment setup for PySpark on Windows.
