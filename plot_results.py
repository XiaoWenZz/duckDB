import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

import matplotlib.font_manager as fm

# 配置字体
def get_cjk_font():
    # 尝试常见的 macOS/Linux 中文字体
    font_names = ['PingFang SC', 'Heiti SC', 'STHeiti', 'Arial Unicode MS', 'SimHei', 'WenQuanYi Micro Hei', 'Droid Sans Fallback']
    
    # 获取系统所有可用字体
    system_fonts = set(f.name for f in fm.fontManager.ttflist)
    
    for font in font_names:
        if font in system_fonts:
            return font
    return 'sans-serif' # Fallback

cjk_font = get_cjk_font()
plt.rcParams['font.sans-serif'] = [cjk_font] + plt.rcParams['font.sans-serif']
plt.rcParams['axes.unicode_minus'] = False
sns.set_theme(style="whitegrid", font=cjk_font)

RESULT_FILE = 'duckdb_benchmark_results.csv'
OUT_DIR = 'figures'

def plot_scalability(df):
    """绘制并行度扩展性图 (Speedup vs Threads)"""
    subset = df[df['Experiment'] == 'Parallelism'].copy()
    
    # 计算加速比 Speedup = T(1) / T(N)
    # 先获取每个 Query 在 Threads=1 时的基准时间
    baseline = subset[subset['Threads'] == 1].set_index('Query')['Avg_Time_Sec']
    
    def calculate_speedup(row):
        base = baseline.get(row['Query'])
        if base:
            return base / row['Avg_Time_Sec']
        return 0
    
    subset['Speedup'] = subset.apply(calculate_speedup, axis=1)
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=subset, x='Threads', y='Speedup', hue='Query', style='Query', markers=True, dashes=False, linewidth=2.5, markersize=9)
    
    # 绘制理想线性加速参考线
    max_threads = subset['Threads'].max()
    plt.plot([1, max_threads], [1, max_threads], '--', color='grey', label='Ideal Linear Speedup', alpha=0.6)
    
    plt.title('DuckDB 并行扩展性 (Parallel Scalability)', fontsize=14)
    plt.xlabel('线程数 (Threads)', fontsize=12)
    plt.ylabel('加速比 (Speedup)', fontsize=12)
    plt.legend()
    plt.xticks(sorted(subset['Threads'].unique()))
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, 'scalability.png'), dpi=300)
    print("Generated figures/scalability.png")

def plot_format_comparison(df):
    """绘制数据格式对比图 (Parquet vs CSV)"""
    # 选取 Experiment 2 的数据 (通常选最大线程数)
    subset = df[df['Experiment'] == 'Format_Comparison'].copy()
    
    # 还需要加入 Parquet 在相同线程数下的数据用于对比
    max_threads = subset['Threads'].max()
    parquet_subset = df[(df['Experiment'] == 'Parallelism') & (df['Threads'] == max_threads)].copy()
    parquet_subset['Experiment'] = 'Format_Comparison' # 统一 Experiment 以便绘图
    parquet_subset['Format'] = 'Parquet'
    
    combined = pd.concat([subset, parquet_subset])
    
    plt.figure(figsize=(10, 6))
    bp = sns.barplot(data=combined, x='Query', y='Avg_Time_Sec', hue='Format', palette='viridis')
    
    # 在柱子上标注具体数值
    for p in bp.patches:
        height = p.get_height()
        if height > 0:
            bp.annotate(f'{height:.2f}s', 
                        (p.get_x() + p.get_width() / 2., height), 
                        ha = 'center', va = 'center', 
                        xytext = (0, 9), 
                        textcoords = 'offset points')

    plt.title(f'数据格式性能对比 (Threads={max_threads})', fontsize=14)
    plt.xlabel('查询类型 (Query Type)', fontsize=12)
    plt.ylabel('执行时间 (Seconds) - 越低越好', fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, 'format_comparison.png'), dpi=300)
    print("Generated figures/format_comparison.png")

def plot_data_scale(df):
    """绘制数据规模扩展性图 (Time vs Data Scale)"""
    subset = df[df['Experiment'] == 'Data_Scale'].copy()
    
    if subset.empty:
        print("No data found for Data_Scale experiment.")
        return

    # 简单的映射排序
    scale_order = ['1_Month', '1_Year', '3_Years']
    subset['Data_Scale'] = pd.Categorical(subset['Data_Scale'], categories=scale_order, ordered=True)
    
    plt.figure(figsize=(10, 6))
    sns.lineplot(data=subset, x='Data_Scale', y='Avg_Time_Sec', hue='Query', style='Query', markers=True, dashes=False, linewidth=2.5, markersize=9)
    
    plt.title('数据规模扩展性 (Data Scalability)', fontsize=14)
    plt.xlabel('数据规模 (Data Scale)', fontsize=12)
    plt.ylabel('执行时间 (Seconds)', fontsize=12)
    plt.tight_layout()
    plt.savefig(os.path.join(OUT_DIR, 'data_scale.png'), dpi=300)
    print("Generated figures/data_scale.png")

def main():
    if not os.path.exists(RESULT_FILE):
        print(f"Error: {RESULT_FILE} not found.")
        return
        
    df = pd.read_csv(RESULT_FILE)
    
    plot_scalability(df)
    plot_format_comparison(df)
    plot_data_scale(df)

if __name__ == "__main__":
    main()
