#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EventLogReport（CONN, C, S, DE）から以下を集計：
  (1) 高重要度 M_H* の 1時間ごとの伝搬率（R=Relayedを受信とみなす）
      - 出力: 全体（長い形式/横展開）, クラスタ別（p,q,r）
  (2) 低重要度 M_L* の到達率（初回到達のみ：DE...D を1回だけカウント、Aは除外）
      - 出力: 全体サマリ, ID別

使い方:
  - LOG_PATH / OUT_DIR / 時間幅 / クラスタ正規表現などを設定して実行
"""

from pathlib import Path
import re
import math
import pandas as pd

# ===================== 設定 =====================
LOG_PATH = Path("75_EventLogReport.txt")   # 入力ログ（EventLogReport）
OUT_DIR  = Path("75_out")                    # 出力ディレクトリ

# 伝搬率の分母Nから boxNN を除外するか（True: 除外 / False: 含む）
EXCLUDE_BOX_IN_DENOM = True
BOX_RE = re.compile(r"^box\d+$", re.IGNORECASE)

# 高重要度（R=Relayedを受信と扱う）
ID_HIGH_RE = re.compile(r"^M_H", re.IGNORECASE)

# 低重要度（初回到達のみ：DE...D を1回だけカウント、Aは除外）
ID_LOW_RE = re.compile(r"^M_L", re.IGNORECASE)

# 時間幅（秒）: 1時間=3600、30分=1800など
BIN_SECONDS = 3600
TOTAL_HOURS = 12   # 12時間想定（必要に応じて変更）

# クラスタ判定（ノード名で振り分ける場合）
CLUSTER_REGEX = {
    "p": re.compile(r"^p\d+$", re.IGNORECASE),
    "q": re.compile(r"^q\d+$", re.IGNORECASE),
    "r": re.compile(r"^r\d+$", re.IGNORECASE),
}
# ==============================================


def parse_eventlogreport(path: Path) -> pd.DataFrame:
    """EventLogReport（CONN / C / S / DE）をDataFrameに正規化"""
    rows = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            parts = line.split()
            # time
            try:
                t = float(parts[0])
            except Exception:
                continue
            etype = parts[1] if len(parts) > 1 else None

            if etype == "CONN" and len(parts) >= 5:
                src, dst, state = parts[2], parts[3], parts[4]
                rows.append(dict(Time=t, Type="CONN", Src=src, Dst=dst, Id=None, Status=state))
            elif etype == "C" and len(parts) >= 4:
                src, mid = parts[2], parts[3]
                rows.append(dict(Time=t, Type="C", Src=src, Dst=None, Id=mid, Status=None))
            elif etype == "S" and len(parts) >= 5:
                src, dst, mid = parts[2], parts[3], parts[4]
                rows.append(dict(Time=t, Type="S", Src=src, Dst=dst, Id=mid, Status=None))
            elif etype == "DE" and len(parts) >= 6:
                src, dst, mid, st = parts[2], parts[3], parts[4], parts[5]
                rows.append(dict(Time=t, Type="DE", Src=src, Dst=dst, Id=mid, Status=st))
            else:
                # 未知形式は無視
                continue
    df = pd.DataFrame(rows)
    return df


def compute_denominator_nodes(df: pd.DataFrame, exclude_box: bool = True) -> set:
    """分母となるノード集合（Src/Dstのユニーク）"""
    nodes = set(df.get("Src", pd.Series(dtype=str)).dropna().astype(str)) | \
            set(df.get("Dst", pd.Series(dtype=str)).dropna().astype(str))
    nodes.discard("")
    if exclude_box:
        nodes = {n for n in nodes if not BOX_RE.match(n)}
    return nodes


def natural_key(mid: str):
    """IDを自然順（末尾の数字を数値として）に並べるキー"""
    m = re.search(r"(\d+)$", mid or "")
    return (mid if m is None else mid[:m.start()], int(m.group(1)) if m else -1, mid)


def hourly_bins(total_hours: int, bin_sec: int):
    """(hour_index, start, end) のタプルを返す（hour_indexは1始まり）"""
    for h in range(1, total_hours + 1):
        yield h, (h - 1) * bin_sec, h * bin_sec


def calc_high_hourly_R_based(df: pd.DataFrame, nodes_all: set, out_dir: Path):
    """
    高重要度 M_H* の伝搬率（受信=R、DE行のみ）を時間帯ごとに集計。
    - その時間内に R を受け取ったユニーク宛先台数 / N * 100
    - 累積ユニーク宛先 / N * 100
    """
    N = len(nodes_all)
    high_R = df[
        (df["Type"] == "DE")
        & df["Id"].notna() & df["Id"].astype(str).str.match(ID_HIGH_RE)
        & (df["Status"] == "R")
    ].copy()

    # 分母に合わせて受信側の boxNN を除外
    if EXCLUDE_BOX_IN_DENOM:
        high_R = high_R[~high_R["Dst"].astype(str).str.match(BOX_RE)]

    mh_ids = sorted(high_R["Id"].dropna().astype(str).unique().tolist(), key=natural_key)

    # 長い形式
    records = []
    for mid in mh_ids:
        sub = high_R[high_R["Id"] == mid].copy()
        seen = set()
        for h, start, end in hourly_bins(TOTAL_HOURS, BIN_SECONDS):
            h_slice = sub[(sub["Time"] > start) & (sub["Time"] <= end)]
            n_hour = h_slice["Dst"].astype(str).nunique()
            rate_hour = (n_hour / N * 100.0) if N > 0 else float("nan")
            seen.update(h_slice["Dst"].astype(str).unique())
            cum_rate = (len(seen) / N * 100.0) if N > 0 else float("nan")
            records.append([mid, h, N, n_hour, round(rate_hour), round(cum_rate)])

    long_df = pd.DataFrame(records, columns=[
        "Id", "hour_index", "N", "n_receivers_hour", "rate_percent_hour", "cumulative_rate_percent"
    ])
    long_csv = out_dir / "MH_all_hourly_progression_Rbased.csv"
    long_txt = out_dir / "MH_all_hourly_progression_Rbased.txt"
    long_df.to_csv(long_csv, index=False)
    with open(long_txt, "w", encoding="utf-8") as f:
        for _, r in long_df.iterrows():
            f.write(f"{r['Id']}\t{int(r['N'])}\t{int(r['n_receivers_hour'])}\t{int(r['rate_percent_hour'])}\t{int(r['cumulative_rate_percent'])}\n")

    # 横展開（時間×ID、値=その時間の%）
    if not long_df.empty:
        wide = long_df.pivot_table(index="hour_index", columns="Id", values="rate_percent_hour", fill_value=0)
        wide = wide.sort_index(axis=1)
        wide.to_csv(out_dir / "MH_all_hourly_wide_rates_Rbased.csv")

    return long_df


def calc_high_hourly_R_based_by_cluster(df: pd.DataFrame, nodes_all: set, out_dir: Path):
    """クラスタ別（p,q,r）に分けて、同じRベースの時間集計を出力"""
    # クラスタ別ノード集合
    cluster_nodes = {c: {n for n in nodes_all if rgx.match(n)} for c, rgx in CLUSTER_REGEX.items()}

    high_R = df[
        (df["Type"] == "DE")
        & df["Id"].notna() & df["Id"].astype(str).str.match(ID_HIGH_RE)
        & (df["Status"] == "R")
    ].copy()

    if EXCLUDE_BOX_IN_DENOM:
        high_R = high_R[~high_R["Dst"].astype(str).str.match(BOX_RE)]

    mh_ids = sorted(high_R["Id"].dropna().astype(str).unique().tolist(), key=natural_key)

    records = []
    for cname, cnodes in cluster_nodes.items():
        N_c = len(cnodes)
        sub_cluster = high_R[high_R["Dst"].astype(str).isin(cnodes)].copy()
        for mid in mh_ids:
            sub = sub_cluster[sub_cluster["Id"] == mid].copy()
            seen = set()
            for h, start, end in hourly_bins(TOTAL_HOURS, BIN_SECONDS):
                h_slice = sub[(sub["Time"] > start) & (sub["Time"] <= end)]
                n_hour = h_slice["Dst"].astype(str).nunique()
                rate_hour = (n_hour / N_c * 100.0) if N_c > 0 else float("nan")
                seen.update(h_slice["Dst"].astype(str).unique())
                cum_rate = (len(seen) / N_c * 100.0) if N_c > 0 else float("nan")
                # NaNは丸めない（後で整形）
                records.append([mid, h, cname, N_c, n_hour, rate_hour, cum_rate])

    long_df = pd.DataFrame(records, columns=[
        "Id", "hour_index", "Cluster", "N_cluster",
        "n_receivers_hour", "rate_percent_hour", "cumulative_rate_percent"
    ])
    long_csv = out_dir / "MH_all_hourly_progression_by_cluster_Rbased.csv"
    long_txt = out_dir / "MH_all_hourly_progression_by_cluster_Rbased.txt"
    long_df.to_csv(long_csv, index=False)
    with open(long_txt, "w", encoding="utf-8") as f:
        for _, r in long_df.iterrows():
            rp = "" if (pd.isna(r["rate_percent_hour"])) else str(int(round(r["rate_percent_hour"])))
            cp = "" if (pd.isna(r["cumulative_rate_percent"])) else str(int(round(r["cumulative_rate_percent"])))
            f.write(f"{r['Id']}\t{int(r['N_cluster'])}\t{int(r['n_receivers_hour'])}\t{rp}\t{cp}\t{r['Cluster']}\t{int(r['hour_index'])}\n")

    # クラスタごと横展開
    for cname in CLUSTER_REGEX.keys():
        sub = long_df[long_df["Cluster"] == cname]
        if sub.empty:
            continue
        wide = sub.pivot_table(index="hour_index", columns="Id", values="rate_percent_hour", fill_value=0)
        wide = wide.sort_index(axis=1)
        wide.to_csv(out_dir / f"MH_cluster_wide_rates_{cname}_Rbased.csv")

    return long_df


def calc_low_arrival_rate_first_delivery(df: pd.DataFrame, out_dir: Path):
    """
    低重要度 M_L* の到達率（初回到達のみ）:
      - L = C行の件数
      - P = DE行のうち Status == 'D' を持つメッセージIDが「少なくとも1回」あるか（IDユニーク）
        ※ A(Delivered-again) は除外、同一IDの複数到達は P=1 として扱う
    """
    low = df[df["Id"].notna() & df["Id"].astype(str).str.match(ID_LOW_RE)].copy()

    created = low[low["Type"] == "C"]
    L_total = int(created.shape[0])

    delivered_first = low[(low["Type"] == "DE") & (low["Status"] == "D")]
    delivered_ids = set(delivered_first["Id"].dropna().astype(str).unique().tolist())
    P_total = len(delivered_ids)

    # 全体サマリ
    summary = pd.DataFrame([{
        "L_created_messages": L_total,
        "P_first_delivered_messages": P_total,
        "arrival_rate_percent": (P_total / L_total * 100.0) if L_total > 0 else float("nan")
    }])
    summary.to_csv(out_dir / "Low_arrival_rate_summary_FIRSTD.csv", index=False)

    # ID別（各 Id 毎に L と P=0/1）
    L_per_id = created.groupby("Id")["Time"].count().rename("L_created").reset_index()
    P_per_id = pd.DataFrame({"Id": list(delivered_ids)})
    P_per_id["P_first_delivered"] = 1

    per_id = L_per_id.merge(P_per_id, on="Id", how="left").fillna({"P_first_delivered": 0})
    per_id["P_first_delivered"] = per_id["P_first_delivered"].astype(int)
    per_id["arrival_rate_percent"] = per_id.apply(
        lambda r: (r["P_first_delivered"] / r["L_created"] * 100.0) if r["L_created"] > 0 else float("nan"),
        axis=1
    )
    per_id.sort_values("Id").to_csv(out_dir / "Low_arrival_rate_by_id_FIRSTD.csv", index=False)

    return summary, per_id


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = parse_eventlogreport(LOG_PATH)

    # 分母ノード集合（box除外/含む は設定に追従）
    nodes_all = compute_denominator_nodes(df, exclude_box=EXCLUDE_BOX_IN_DENOM)

    # 高重要度（Rベース）1時間ごと：全体/クラスタ別
    calc_high_hourly_R_based(df, nodes_all, OUT_DIR)
    calc_high_hourly_R_based_by_cluster(df, nodes_all, OUT_DIR)

    # 低重要度（初回到達のみ）
    calc_low_arrival_rate_first_delivery(df, OUT_DIR)

    # 参考: 分母のサマリを残す
    with open(OUT_DIR / "denominator_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"N_overall(excluding_boxes={EXCLUDE_BOX_IN_DENOM}): {len(nodes_all)}\n")
        for cname, rgx in CLUSTER_REGEX.items():
            nset = {n for n in nodes_all if rgx.match(n)}
            f.write(f"N_{cname}: {len(nset)}\n")


if __name__ == "__main__":
    main()
