import os
import csv

# ====== 設定 ======
# 個別CSV が置いてあるフォルダ
# ※ Windows のパスなので r"" で書いています
BASE_DIR = "csv_out/416_EventLogReport"

# 対象メッセージ（ここでは M_Hp1〜M_Hp11）
MESSAGE_IDS = [f"M_Hp{i}" for i in range(1, 36)]
#MESSAGE_IDS = [f"M_Hq{i}" for i in range(1, 36)]
#MESSAGE_IDS = [f"M_Hr{i}" for i in range(1, 36)]

# 個別CSVのファイル名フォーマット
# 例: M_Hp1_propagation_events_step1pct.csv
FILENAME_TEMPLATE = "{mid}_propagation_events_step1pct.csv"

# 出力する「結合CSV」のファイル名
OUTPUT_CSV = "416_M_Hp1-35_propagation_events_step1pct_combined.csv"
# ==================


def main():
    output_path = OUTPUT_CSV

    # 出力CSVを新規作成してヘッダを書いておく
    with open(output_path, "w", newline="", encoding="utf-8-sig") as fout:
        writer = csv.writer(fout)
        # 縦長データ：time, rate, message_id
        writer.writerow([
            "time_sim_sec",
            "cumulative_propagation_rate_percent",
            "message_id",
        ])

        # 各 M_Hp* の CSV を順番に読み込んで追記
        for mid in MESSAGE_IDS:
            fname = FILENAME_TEMPLATE.format(mid=mid)
            path = os.path.join(BASE_DIR, fname)

            if not os.path.exists(path):
                print(f"[WARN] ファイルが見つかりません: {path} -> スキップ")
                continue

            print(f"[INFO] 読み込み中: {path}")

            with open(path, "r", newline="", encoding="utf-8-sig") as fin:
                reader = csv.reader(fin)
                rows = list(reader)

                if not rows:
                    print(f"[WARN] 空のファイルです: {path}")
                    continue

                header = rows[0]
                # 必要な列の位置を確認
                try:
                    idx_time = header.index("time_sim_sec")
                    idx_rate = header.index("cumulative_propagation_rate_percent")
                except ValueError:
                    print(f"[WARN] 必要な列が見つかりません: {path} -> スキップ")
                    continue

                # データ行を1行ずつ書き出し
                for row in rows[1:]:
                    if len(row) <= max(idx_time, idx_rate):
                        continue
                    time_val = row[idx_time]
                    rate_val = row[idx_rate]
                    writer.writerow([time_val, rate_val, mid])

    print(f"\n結合CSVを出力しました: {output_path}")


if __name__ == "__main__":
    main()
