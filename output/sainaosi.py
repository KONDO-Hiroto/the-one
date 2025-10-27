import csv
from pathlib import Path
import pandas as pd
import glob
import re

# 自然順ソート用のキー関数
def natural_sort_key(text):
    return [int(c) if c.isdigit() else c for c in re.split(r'(\d+)', text)]

# 入力データファイルと出力ディレクトリ
input_file = "3_EventLogReport_pri.txt"
output_dir = "3cluster"
Path(output_dir).mkdir(exist_ok=True)

# 0~43200sを3600s毎に区切る
time_range = []
for i in range(0, 43200, 3600):
    time = (i, i + 3600)
    time_range.append(time)

# データを時間範囲で分割してCSVに保存
def process_and_save(input_file, output_dir, time_ranges):
    with open(input_file, "r") as file:
        data = []
        for line in file:
            parts = line.strip().split()
            if len(parts) < 6:
                continue
            data.append(parts)

    data.sort(key=lambda x: float(x[0]))  # 時間でソート

    for index, (start_time, end_time) in enumerate(time_ranges, start=1):
        grouped_data = [entry for entry in data if start_time <= float(entry[0]) < end_time]

        if grouped_data:
            output_file = Path(output_dir) / f"3cluster_{index}.csv"
            with open(output_file, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Time", "Type", "Host", "Tohost", "Id", "Status"])
                writer.writerows(grouped_data)
            print(f"Saved {len(grouped_data)} entries to {output_file}")

# データ伝搬率 High の計算（宛先を重複なくカウント）
def calculate_high(output_dir, id_prefix, step=3, max_id=36):
    # 自然順ソートでファイルリストを取得
    file_list = sorted(glob.glob(f"{output_dir}/3cluster_*.csv"), key=natural_sort_key)
    all_results = []  # すべての ID の結果を保持

    for id_index in range(1, max_id + 1, step):  # `step` 間隔で処理
        target_id = f"{id_prefix}{id_index}"  # 処理対象の ID（例: M_Hp1, M_Hp3）

        results = []
        seen_tohosts = set()  # すべてのファイルで受け取った宛先のリストを保持
        cumulative_high = 0  # High の累積結果を保持

        for file_path in file_list:
            data = pd.read_csv(file_path)

            total_nodes = pd.concat([data['Host'], data['Tohost']]).drop_duplicates().nunique()

            # 新たに受け取った宛先をリストに追加する前に重複しないようにする
            mh_received_nodes = 0
            for tohost in data[(data['Id'] == target_id) & (data['Status'] == 'R')]['Tohost'].drop_duplicates():
                if tohost not in seen_tohosts:  # まだカウントされていない宛先
                    seen_tohosts.add(tohost)  # 宛先をリストに追加
                    mh_received_nodes += 1

            High = (mh_received_nodes / total_nodes) * 100 if total_nodes > 0 else 0
            cumulative_high += High  # 累積 High の値を加算

            results.append({
                'file': file_path, 
                'target_id': target_id, 
                'total_nodes': total_nodes, 
                'mh_received_nodes': mh_received_nodes, 
                'High': High, 
                'cumulative_high': cumulative_high  # 累積結果を追加
            })

        all_results.extend(results)  # 各 ID の結果をまとめる

    # すべての結果を 1 つの CSV に保存
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(f'{output_dir}/High_3cluster_{id_prefix}_cumulative.csv', index=False)
    print(f"High の累積結果を {output_dir}/High_3cluster_{id_prefix}_cumulative.csv に保存しました。")

# データ到達率 Low の計算（宛先を重複なくカウント）
def calculate_low(output_dir):
    # 自然順ソートでファイルリストを取得
    file_list = sorted(glob.glob(f"{output_dir}/3cluster_*.csv"), key=natural_sort_key)
    results = []

    seen_tohosts = set()  # すべてのファイルで受け取った宛先のリストを保持

    for file_path in file_list:
        data = pd.read_csv(file_path)
        
        # 'M_L' に関連するデータをフィルタリング
        ml_data = data[data['Id'].str.startswith('M_L')]
        
        # 作成数（全ての 'M_L'）
        ml_created = len(ml_data)
        
        # 新たに受け取った宛先をリストに追加する前に重複しないようにする
        ml_received = 0
        for tohost in ml_data[ml_data['Status'] == 'D']['Tohost']:
            if tohost not in seen_tohosts:  # まだカウントされていない宛先
                seen_tohosts.add(tohost)  # 宛先をリストに追加
                ml_received += 2
        
        Low = (ml_received / ml_created) * 100 if ml_created > 0 else 0
        results.append({
            'File': file_path, 'M_L Created': ml_created, 'M_L Received': ml_received, 'Low': Low
        })

    output_file = f"{output_dir}/Low_3cluster.csv"
    pd.DataFrame(results).to_csv(output_file, index=False)
    print(f"Low の結果を {output_file} に保存しました。")

# 実行フローの修正
if __name__ == "__main__":
    process_and_save(input_file, output_dir, time_range)
    
    # 複数の M_Hp ID を 3 個刻みで計算
    calculate_high(output_dir, id_prefix="M_Hp", step=1, max_id=24)
    calculate_low(output_dir)