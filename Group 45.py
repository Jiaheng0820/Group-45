import random
import string
import time
import sys
import mysql.connector
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import tkinter as tk
from tkinter import ttk

# Fix random numbers for reproducibility
random.seed(1234)

# Configuration
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "JH1234"
MYSQL_DB = "indexing_test"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "indexing_test"
MONGO_COLLECTION = "records"

NUM_RECORDS = 10000
NUM_RUNS = 5  # Number of times each query is executed

def random_string(length=5):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

# Connect to MySQL
try:
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB}")
    mysql_cursor.execute(f"USE {MYSQL_DB}")
    print("MySQL connected and database ready.")
except Exception as e:
    print("MySQL connection error:", e)
    sys.exit(1)

# Create MySQL table
mysql_cursor.execute("DROP TABLE IF EXISTS records")
mysql_cursor.execute("""
CREATE TABLE records (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(50),
    age INT,
    salary FLOAT,
    join_date DATE
)
""")
print("MySQL table created.")

# Connect to MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    mongo_collection = mongo_db[MONGO_COLLECTION]
    mongo_collection.drop()
    print("MongoDB connected and collection ready.")
except Exception as e:
    print("MongoDB connection error:", e)
    sys.exit(1)

# Generate synthetic data
data = []
for _ in range(NUM_RECORDS):
    record = {
        "name": random_string(5),
        "age": random.randint(20, 60),
        "salary": round(random.uniform(3000, 10000), 2),
        "join_date": f"2020-{random.randint(1,12):02}-{random.randint(1,28):02}"
    }
    data.append(record)

# Insert data into MySQL
mysql_insert = "INSERT INTO records (name, age, salary, join_date) VALUES (%s, %s, %s, %s)"
mysql_values = [(r['name'], r['age'], r['salary'], r['join_date']) for r in data]

start_time = time.time()
mysql_cursor.executemany(mysql_insert, mysql_values)
mysql_conn.commit()
mysql_insert_time = time.time() - start_time
print(f"MySQL: {NUM_RECORDS} records inserted in {mysql_insert_time:.4f} seconds.")

# Insert data into MongoDB
start_time = time.time()
mongo_collection.insert_many(data)
mongo_insert_time = time.time() - start_time
print(f"MongoDB: {NUM_RECORDS} records inserted in {mongo_insert_time:.4f} seconds.")

# Create indexes
mysql_cursor.execute("CREATE INDEX idx_age ON records(age)")
mysql_cursor.execute("CREATE INDEX idx_age_salary ON records(age, salary)")
mongo_collection.create_index("age")
mongo_collection.create_index([("age", 1), ("salary", 1)])
print("Indexes created for all four strategies.")

# Measure storage consumption
mysql_cursor.execute(f"""
SELECT ROUND((data_length + index_length)/1024/1024, 4) AS size_mb
FROM information_schema.tables
WHERE table_schema = '{MYSQL_DB}' AND table_name = 'records';
""")
mysql_storage = float(mysql_cursor.fetchone()[0])

stats = mongo_db.command("collstats", MONGO_COLLECTION)
mongo_storage = stats['storageSize'] / (1024*1024)

# Helper function to run queries multiple times
def run_query_multiple_times_mysql(query):
    times = []
    counts = []
    for _ in range(NUM_RUNS):
        start = time.time()
        mysql_cursor.execute(query)
        rows = mysql_cursor.fetchall()
        end = time.time()
        times.append(end - start)
        counts.append(len(rows))
    avg_time = sum(times) / NUM_RUNS
    avg_throughput = sum([c/t for c,t in zip(counts, times)]) / NUM_RUNS
    return avg_time, avg_throughput

def run_query_multiple_times_mongo(filter_query):
    times = []
    counts = []
    for _ in range(NUM_RUNS):
        start = time.time()
        rows = list(mongo_collection.find(filter_query))
        end = time.time()
        times.append(end - start)
        counts.append(len(rows))
    avg_time = sum(times) / NUM_RUNS
    avg_throughput = sum([c/t for c,t in zip(counts, times)]) / NUM_RUNS
    return avg_time, avg_throughput

# Run queries and collect results
results = []

mysql_queries = [
    ("MySQL Standard (single-field)", "SELECT * FROM records WHERE age > 30"),
    ("MySQL Optimized (composite)", "SELECT * FROM records WHERE age > 30 AND salary > 5000"),
]

for name, query in mysql_queries:
    avg_time, avg_throughput = run_query_multiple_times_mysql(query)
    results.append({
        "Index Type": name,
        "Execution Time (s)": round(avg_time, 4),
        "Throughput (records/s)": round(avg_throughput, 2),
        "Storage Consumption (MB)": round(mysql_storage, 4)
    })

mongo_queries = [
    ("MongoDB Standard (single-field)", {"age": {"$gt": 30}}),
    ("MongoDB Optimized (compound)", {"age": {"$gt": 30}, "salary": {"$gt": 5000}})
]

for name, q in mongo_queries:
    avg_time, avg_throughput = run_query_multiple_times_mongo(q)
    results.append({
        "Index Type": name,
        "Execution Time (s)": round(avg_time, 4),
        "Throughput (records/s)": round(avg_throughput, 2),
        "Storage Consumption (MB)": round(mongo_storage, 4)
    })

# Display comparison table in pop-up
df = pd.DataFrame(results)

def show_table(df):
    root = tk.Tk()
    root.title("Comparison Table")

    tree = ttk.Treeview(root, columns=list(df.columns), show='headings')
    for col in df.columns:
        tree.heading(col, text=col)
        tree.column(col, width=150, anchor='center')

    for row in df.itertuples(index=False):
        tree.insert("", tk.END, values=row)

    tree.pack(expand=True, fill='both')
    scrollbar = ttk.Scrollbar(root, orient="vertical", command=tree.yview)
    tree.configure(yscroll=scrollbar.set)
    scrollbar.pack(side='right', fill='y')
    root.geometry("700x300")
    root.mainloop()

show_table(df)

# Bar charts
plt.figure(figsize=(8,5))
plt.bar(df['Index Type'], df['Execution Time (s)'], color='skyblue')
plt.title("Execution Time by Index Type")
plt.ylabel("Time (s)")
plt.xticks(rotation=30)
plt.tight_layout()
plt.show()

plt.figure(figsize=(8,5))
plt.bar(df['Index Type'], df['Throughput (records/s)'], color='lightgreen')
plt.title("Throughput by Index Type")
plt.ylabel("Records per second")
plt.xticks(rotation=30)
plt.tight_layout()
plt.show()

plt.figure(figsize=(8,5))
plt.bar(df['Index Type'], df['Storage Consumption (MB)'], color='salmon')
plt.title("Storage Consumption by Index Type")
plt.ylabel("MB")
plt.xticks(rotation=30)
plt.tight_layout()
plt.show()

mysql_cursor.close()
mysql_conn.close()
print("Experiment completed successfully!")