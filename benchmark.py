import duckdb
import time
import os
import subprocess
import pandas as pd
import platform
import glob
import psutil
import threading

# ... (Previous imports)

class ResourceMonitor:
    def __init__(self, interval=0.1):
        self.interval = interval
        self.running = False
        self.cpu_usage = []
        self.memory_usage = []
        self.thread = None

    def _monitor(self):
        process = psutil.Process(os.getpid())
        while self.running:
            # CPU percent over interval (blocking for interval inside psutil if > 0)
            # Use interval=None and manual sleep to control loop better if needed, 
            # but psutil.cpu_percent(interval=0.1) is good for sampling.
            try:
                cpu = process.cpu_percent(interval=self.interval)
                mem = process.memory_info().rss / (1024 * 1024) # MB
                self.cpu_usage.append(cpu)
                self.memory_usage.append(mem)
            except Exception:
                break

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
        
    def get_stats(self):
        if not self.cpu_usage:
            return 0, 0
        avg_cpu = sum(self.cpu_usage) / len(self.cpu_usage)
        max_mem = max(self.memory_usage) if self.memory_usage else 0
        return avg_cpu, max_mem

def run_query(conn, query_name, sql, threads, iteration):
    """执行单个查询并记录时间与资源"""
    
    # 设置线程数
    conn.execute(f"PRAGMA threads={threads};")
    
    # 清理缓存 (如果配置开启)
    drop_os_caches()
    
    # 强制清理 DuckDB 内部缓存 (Buffer Manager)
    # conn.execute("PRAGMA memory_limit='120GB';") # Optional reset
    
    monitor = ResourceMonitor(interval=0.1)
    
    start_time = time.time()
    monitor.start()
    try:
        conn.execute(sql).fetchall() # Fetch results to ensure execution completes
    except Exception as e:
        print(f"Error executing {query_name}: {e}")
    finally:
        monitor.stop()
    end_time = time.time()
    
    avg_cpu, max_mem = monitor.get_stats()
    duration = end_time - start_time
    
    print(f"   Iter {iteration}: {duration:.4f}s | CPU: {avg_cpu:.1f}% | Mem: {max_mem:.1f}MB")
    return duration, avg_cpu, max_mem

# ... (Configuration constants remain the same)

# ... (Main loop logic needs update to store new metrics)

# 数据文件路径模式 (Glob Pattern)
PARQUET_PATH = 'data/yellow_tripdata_*.parquet' 
CSV_PATH = 'data/yellow_tripdata_*.csv'         

# 结果保存路径
RESULT_FILE = 'duckdb_benchmark_results.csv'

# 线程数测试列表 (建议根据机器核心数调整)
THREAD_COUNTS = [1, 2, 4, 8, 16, 24] 

# 每个实验重复次数 (取平均值)
ITERATIONS = 5 

# 是否在每次运行前尝试清除系统缓存 (需要 sudo 权限)
CLEAR_CACHE = True

# ================= 查询定义 (Q1-Q3) =================
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
    Linux 命令: sync; echo 3 > /proc/sys/vm/drop_caches
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

    # [1. 实验一: 并行度扩展性测试 (Parquet)
    print(f"\n--- 实验一: 并行度测试 (Dataset: Parquet) ---")
    file_format = 'Parquet'
    path = PARQUET_PATH
    
    for threads in THREAD_COUNTS:
        print(f"\n[Configuration] Threads: {threads}")
        
        for q_name, q_info in QUERIES.items():
            formatted_sql = q_info['sql'].format(data_path=path)
            
            print(f"Running {q_name} with {threads} threads...")
            times = []
            cpus = []
            mems = []
            for i in range(ITERATIONS):
                duration, avg_cpu, max_mem = run_query(conn, q_name, q_info["sql"].format(data_path=path), threads, i+1)
                times.append(duration)
                cpus.append(avg_cpu)
                mems.append(max_mem)
            
            avg_time = sum(times) / len(times)
            avg_cpu_total = sum(cpus) / len(cpus)
            max_mem_peak = max(mems)
            
            results.append({
                "Experiment": "Parallelism",
                "Format": file_format,
                "Query": q_name,
                "Threads": threads,
                "Avg_Time_Sec": avg_time,
                "Avg_CPU_Pct": avg_cpu_total,
                "Max_Mem_MB": max_mem_peak,
                "Raw_Times": times
            })

    # [2. 实验二: 数据格式对比测试 (CSV)
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
                cpus = []
                mems = []
                print(f"  Running {q_name}...", end="", flush=True)
                
                for i in range(ITERATIONS):
                    duration, avg_cpu, max_mem = run_query(conn, q_name, formatted_sql, max_threads, i)
                    times.append(duration)
                    cpus.append(avg_cpu)
                    mems.append(max_mem)
                
                avg_time = sum(times) / len(times)
                avg_cpu_total = sum(cpus) / len(cpus)
                max_mem_peak = max(mems)
                
                results.append({
                    "Experiment": "Format_Comparison",
                    "Format": file_format,
                    "Query": q_name,
                    "Threads": max_threads,
                    "Avg_Time_Sec": avg_time,
                    "Avg_CPU_Pct": avg_cpu_total,
                    "Max_Mem_MB": max_mem_peak,
                    "Raw_Times": times
                })
            except Exception as e:
                print(f"\n  [Error] Failed to run CSV query {q_name}: {e}")



    # [3. 实验三: 数据规模扩展性测试 (Data Scale)]
    print(f"\n--- 实验三: 数据规模测试 (Data Scale) ---")
    # 定义不同规模的数据集路径
    scales = {
        "1_Month": "data/yellow_tripdata_2019-01.parquet",
        "1_Year": "data/yellow_tripdata_2019-*.parquet",
        "3_Years": "data/yellow_tripdata_*.parquet"
    }
    
    for scale_name, scale_path in scales.items():
        # 检查文件是否存在
        found_files = glob.glob(scale_path)
        if not found_files:
            print(f"  [Skip] {scale_name}: No files match '{scale_path}'")
            continue
        
        print(f"  [Run] {scale_name}: Found {len(found_files)} files.")
            
        conn = duckdb.connect(database=':memory:')
        for q_name, q_info in QUERIES.items():
            print(f"    Running {q_name} with {max_threads} threads...", end="", flush=True)
            times = []
            cpus = []
            mems = []
            for i in range(ITERATIONS):
                 duration, avg_cpu, max_mem = run_query(conn, q_name, q_info["sql"].format(data_path=scale_path), max_threads, i+1)
                 times.append(duration)
                 cpus.append(avg_cpu)
                 mems.append(max_mem)

            avg_time = sum(times) / len(times)
            avg_cpu_total = sum(cpus) / len(cpus)
            max_mem_peak = max(mems)
            
            results.append({
                "Experiment": "Data_Scale",
                "Format": "Parquet",
                "Query": q_name,
                "Threads": max_threads, 
                "Avg_Time_Sec": avg_time,
                "Data_Scale": scale_name, 
                "Avg_CPU_Pct": avg_cpu_total,
                "Max_Mem_MB": max_mem_peak,
                "Raw_Times": times
            })

    # 保存结果
    df = pd.DataFrame(results)
    df.to_csv(RESULT_FILE, index=False)
    print(f"\n=== 评测完成。结果已保存至 {RESULT_FILE} ===")

if __name__ == "__main__":
    main()
