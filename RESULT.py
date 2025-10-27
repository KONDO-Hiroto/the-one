import os

LOG_PATH    = "81_EventLogReport.txt"  # 入力ログ
BIN_SECONDS = 3600
TOTAL_HOURS = 12
EXCLUDE_BOX = True
EXCLUDE_DO = True
OUT_DIR     = "81_result" # 出力フォルダ

if not os.path.exists(OUT_DIR):
    os.makedirs(OUT_DIR)

# 便利関数
def is_box(name):
    if name is None:
        return False
    return str(name).lower().startswith("box")

def is_do(name):
    if name is None:
        return False
    return str(name).lower().startswith("do")

def is_high_id(mid):
    """
    高重要度のID判定:
      - M_H（例: M_H1）
      - M_Hp* / M_Hq* / M_Hr*（例: M_Hp1, M_Hq12, M_Hr3）
    ※ 大文字/小文字は無視します
    """
    if mid is None:
        return False
    s = str(mid).lower()
    return s.startswith("m_h")  # m_h, m_hp, m_hq, m_hr ... ぜんぶ含む

def is_low_id(mid):
    if mid is None:
        return False
    return str(mid).lower().startswith("m_l")

def natural_sort_ids(ids_list):
    def sort_key(s):
        s = str(s)
        num = ""
        i = len(s) - 1
        while i >= 0 and s[i].isdigit():
            num = s[i] + num
            i -= 1
        if num == "":
            return (s, -1)
        head = s[:len(s) - len(num)]
        try:
            return (head, int(num))
        except:
            return (s, -1)
    lst = list(ids_list)
    lst.sort(key=sort_key)
    return lst

# ログ読込
# events の要素は {"time","type"('C'|'R'|'D'),"src","dst","id"}
events = []
with open(LOG_PATH, "r", encoding="utf-8", errors="ignore") as f:
    for raw in f:
        line = raw.strip()
        if not line:
            continue
        parts = line.split()
        try:
            t = float(parts[0])
        except:
            continue
        typ = parts[1] if len(parts) > 1 else None

        # C 行（4フィールド想定）: time C src id
        if typ == "C" and len(parts) >= 4:
            src = parts[2]
            mid = parts[3]
            events.append({"time": t, "type": "C", "src": src, "dst": None, "id": mid})
            continue

        # DE ... R / D を R / D に正規化
        if typ == "DE" and len(parts) >= 6:
            src = parts[2]; dst = parts[3]; mid = parts[4]; st = parts[5]
            if st == "R":
                events.append({"time": t, "type": "R", "src": src, "dst": dst, "id": mid})
            elif st == "D":
                events.append({"time": t, "type": "D", "src": src, "dst": dst, "id": mid})
            continue

# 分母ノード（src/dst のユニーク）。box* / do* は設定に従って除外
nodes_all = set()
for e in events:
    if e.get("src") is not None:
        nodes_all.add(e["src"])
    if e.get("dst") is not None:
        nodes_all.add(e["dst"])

filtered_nodes = set()
for n in nodes_all:
    if EXCLUDE_BOX and is_box(n):
        continue
    if EXCLUDE_DO and is_do(n):
        continue
    filtered_nodes.add(n)
nodes_all = filtered_nodes
N = len(nodes_all)

# 高重要度：R または D を“受信”として、初回だけを時間ごとにカウント
# 宛先が box / do の行は、分母に合わせて除外
rd_events_high = []
for e in events:
    if (e["type"] == "R" or e["type"] == "D") and is_high_id(e["id"]):
        if EXCLUDE_BOX and is_box(e["dst"]):
            continue
        if EXCLUDE_DO and is_do(e["dst"]):
            continue
        rd_events_high.append(e)

# ID一覧（M_H, M_Hp*, M_Hq*, M_Hr* を含む）
mh_ids_set = set()
for e in rd_events_high:
    mh_ids_set.add(e["id"])
mh_ids = natural_sort_ids(mh_ids_set)

# 1時間ごとの“初回受信ノード数”と率[%]、累積[%]
high_csv_path = os.path.join(OUT_DIR, "MH_hourly_firsttime_81.csv")
with open(high_csv_path, "w", encoding="utf-8") as f:
    f.write("Id,hour_index,N,n_firsttime_receivers_hour,rate_percent_hour,cumulative_rate_percent\n")
    for mid in mh_ids:
        already_seen_nodes = set()  # そのIDで既に受信済みノード

        h = 1
        while h <= TOTAL_HOURS:
            start_time = (h - 1) * BIN_SECONDS
            end_time   = h * BIN_SECONDS

            # この時間帯に R か D を受け取った宛先（ユニーク）
            hour_receivers = set()
            for e in rd_events_high:
                if e["id"] == mid:
                    if e["time"] > start_time and e["time"] <= end_time:
                        hour_receivers.add(e["dst"])

            # 初回だけ残す
            first_time_nodes = set()
            for node in hour_receivers:
                if node not in already_seen_nodes:
                    first_time_nodes.add(node)

            # その時間の新規受信台数と率
            n_hour = len(first_time_nodes)
            if N > 0:
                rate_hour = (n_hour / N) * 100.0
            else:
                rate_hour = 0.0

            # 累積更新
            for node in first_time_nodes:
                already_seen_nodes.add(node)
            if N > 0:
                cum_rate = (len(already_seen_nodes) / N) * 100.0
            else:
                cum_rate = 0.0

            # 出力（四捨五入）
            f.write(mid + "," + str(h) + "," + str(N) + "," + str(n_hour) + "," + str(round(rate_hour)) + "," + str(round(cum_rate)) + "\n")

            h = h + 1

# 低重要度：C と“初回 D”のみ（※ do除外の影響は分母Nにのみ反映。到達そのものの判定は従来どおり）
# L（作成数）
L_per_id = {}
L_total = 0
for e in events:
    if e["type"] == "C" and is_low_id(e["id"]):
        mid = e["id"]
        if mid not in L_per_id:
            L_per_id[mid] = 0
        L_per_id[mid] = L_per_id[mid] + 1
        L_total = L_total + 1

# P（初回Dあり）
delivered_once_ids = set()
for e in events:
    if e["type"] == "D" and is_low_id(e["id"]):
        delivered_once_ids.add(e["id"])
P_total = len(delivered_once_ids)

# 全体サマリ
low_summary_path = os.path.join(OUT_DIR, "ML_arrival_rate_81.csv")
with open(low_summary_path, "w", encoding="utf-8") as f:
    if L_total > 0:
        rate_total = (P_total / L_total) * 100.0
    else:
        rate_total = 0.0
    f.write("L_created_messages,P_first_delivered_messages,arrival_rate_percent\n")
    f.write(str(L_total) + "," + str(P_total) + "," + str(round(rate_total, 1)) + "\n")

# ID別
def natural_sorted_keys(d):
    lst = []
    for k in d.keys():
        lst.append(k)
    return natural_sort_ids(lst)

low_byid_path = os.path.join(OUT_DIR, "ML_arrival_rate_by_id_81.csv")
with open(low_byid_path, "w", encoding="utf-8") as f:
    f.write("Id,L_created,P_first_delivered,arrival_rate_percent\n")
    for mid in natural_sorted_keys(L_per_id):
        L_i = L_per_id.get(mid, 0)
        P_i = 1 if mid in delivered_once_ids else 0
        rate_i = (P_i / L_i) * 100.0 if L_i > 0 else 0.0
        f.write(mid + "," + str(L_i) + "," + str(P_i) + "," + str(round(rate_i, 1)) + "\n")

print("OK")