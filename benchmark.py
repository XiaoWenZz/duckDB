import duckdb
import time
import os
import subprocess
import pandas as pd
import platform
import glob

# ================= 配置区域 =================
# 数据文件路径模式 (Glob Pattern)
PARQUET_PATH = 'data/yellow_tripdata_*.parquet' 
CSV_PATH = 'data/yellow_tripdata_*.csv'         

# 结果保存路径
RESULT_FILE = 'duckdb_benchmark_results.csv'

# 线程数测试列表 (建议根据机器核心数调整)
THREAD_COUNTS = [1, 2, 4, 8, 16] 

# 每个实验重复次数 (取平均值)
ITERATIONS = 5 

# 是否在每次运行前尝试清除系统缓存 (需要 sudo 权限)
# [cite: 47] 建议 Linux/macOS 开启，Windows 需手动或忽略
CLEAR_CACHE = False 

# ================= 查询定义 (Q1-Q3) =================
# 完全复用 PDF 中的 SQL 定义 [cite: 58-82]
QUERIES = {
    "Q1": {
        "desc": "简单聚合 (I/O Bound)",
        "sql": """
            SELECT count(*), avg(total_amount)
            FROM '{data_path}';
        """
    },
    "Q2": {
        "desc": "过滤+分组聚合 (Filter + Group By)",
        "sql": """
            SELECT passenger_count, avg(trip_distance) AS avg_dist
            FROM '{data_path}'
            WHERE trip_distance > 0 AND total_amount > 0
            GROUP BY passenger_count
            ORDER BY avg_dist DESC;
        """
    },
    "Q3": {
        "desc": "高基数分组+排序 (High Cardinality Group By)",
        "sql": """
            SELECT PULocationID, DOLocationID, count(*) AS trip_count
            FROM '{data_path}'
            GROUP BY PULocationID, DOLocationID
            ORDER BY trip_count DESC
            LIMIT 10;
        """
    }
}

# ================= 辅助函数 =================

def drop_os_caches():
    """
    清理操作系统页缓存，确保 I/O 测试的公平性 (Cold Start)。
    [cite: 48] Linux 命令: sync; echo 3 > /proc/sys/vm/drop_caches
    """
    if not CLEAR_CACHE:
        return

    system = platform.system()
    try:
        if system == "Linux":
            # print("  [System] Dropping caches (requires sudo)...")
            subprocess.run(["sync"], check=True)
            # 注意: 这通常需要 root 权限运行脚本，或者配置 sudo 免密
            subprocess.run(["sudo", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"], check=True)
        elif system == "Darwin": # macOS
            subprocess.run(["sync"], check=True)
            subprocess.run(["sudo", "purge"], check=True)
    except Exception as e:
        print(f"  [Warning] Failed to drop caches: {e}")

def run_query(conn, query_name, sql, threads, iteration):
    """执行单个查询并记录时间"""
    
    # 设置线程数 [cite: 33]
    conn.execute(f"PRAGMA threads={threads};")
    
    # 清理缓存 (如果配置开启)
    if CLEAR_CACHE: # Each run should potentially be cold start if analyzing IO
        drop_os_caches()
    
    start_time = time.time()
    conn.execute(sql)
    end_time = time.time()
    
    duration = end_time - start_time
    return duration

# ================= 主程序 =================

def main():
    results = []
    
    print("=== 开始 DuckDB 并行分析性能评测 ===")
    
    # 初始化 DuckDB 连接 (内存模式)
    conn = duckdb.connect(database=':memory:')
    
    # 检查 Parquet 文件
    if not glob.glob(PARQUET_PATH):
        print(f"Error: No files found for {PARQUET_PATH}. Please populate data/ directory.")
        # Proceeding anyway to show flow, but connection will fail on query.
        return

    # [1. 实验一: 并行度扩展性测试 (Parquet) [cite: 94]]
    print(f"\n--- 实验一: 并行度测试 (Dataset: Parquet) ---")
    file_format = 'Parquet'
    path = PARQUET_PATH
    
    for threads in THREAD_COUNTS:
        print(f"\n[Configuration] Threads: {threads}")
        
        for q_name, q_info in QUERIES.items():
            formatted_sql = q_info['sql'].format(data_path=path)
            
            times = []
            print(f"  Running {q_name} ({q_info['desc']})...", end="", flush=True)
            
            for i in range(ITERATIONS):
                # 执行查询
                t = run_query(conn, q_name, formatted_sql, threads, i)
                times.append(t)
                print(f" {t:.2f}s", end="", flush=True)
            
            avg_time = sum(times) / len(times)
            print(f" | Avg: {avg_time:.4f}s")
            
            results.append({
                "Experiment": "Parallelism",
                "Format": file_format,
                "Query": q_name,
                "Threads": threads,
                "Avg_Time_Sec": avg_time,
                "Raw_Times": times
            })

    # [2. 实验二: 数据格式对比测试 (CSV) [cite: 99]]
    # 仅使用最大线程数进行对比
    max_threads = max(THREAD_COUNTS)
    print(f"\n--- 实验二: 格式对比测试 (Dataset: CSV) ---")
    print(f"[Configuration] Threads: {max_threads} (Fixed)")
    
    file_format = 'CSV'
    path = CSV_PATH
    
    csv_files = glob.glob(CSV_PATH)
    if not csv_files:
         print(f"  [Skip] No CSV files found at {CSV_PATH}. Skipping CSV experiment.")
    else:
        for q_name, q_info in QUERIES.items():
            formatted_sql = q_info['sql'].format(data_path=path)
            
            try:
                times = []
                print(f"  Running {q_name}...", end="", flush=True)
                
                for i in range(ITERATIONS):
                    t = run_query(conn, q_name, formatted_sql, max_threads, i)
                    times.append(t)
                    print(f" {t:.2f}s", end="", flush=True)
                    
                avg_time = sum(times) / len(times)
                print(f" | Avg: {avg_time:.4f}s")
                
                results.append({
                    "Experiment": "Format_Comparison",
                    "Format": file_format,
                    "Query": q_name,
                    "Threads": max_threads,
                    "Avg_Time_Sec": avg_time,
                    "Raw_Times": times
                })
            except Exception as e:
                print(f"\n  [Error] Failed to run CSV query {q_name}: {e}")

    # 保存结果
    df = pd.DataFrame(results)
    df.to_csv(RESULT_FILE, index=False)
    print(f"\n=== 评测完成。结果已保存至 {RESULT_FILE} ===")

if __name__ == "__main__":
    main()
