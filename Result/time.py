import csv
import os
from pathlib import Path
from collections import defaultdict

# ========= 設定 =========
LOG_FILE = "416_EventLogReport.txt"  # EventLogReport のパス

# 対象とするメッセージIDのプレフィックス（複数OK）
# 例: M_Hp*, M_Hq*, M_Hr* をすべて対象にする
MESSAGE_PREFIXES = ("M_Hp", "M_Hq", "M_Hr")

SIM_START = 0.0                         # シミュレーション開始時刻 [s]
SIM_END   = 21600.0                     # シミュレーション終了時刻 [s]

# TTL [秒]（生成時刻 + TTL まで評価）
TTL_SECS  = 21600.0

# 伝搬率の分母に含めるノードのプレフィックス
# 例: p*, q*, r* だけを分母にする（Do* は含まれない）
NODE_PREFIXES = ("p", "q", "r")
# 全ノードを分母にしたければ None にする
# NODE_PREFIXES = None
# ========================


def node_is_target(name: str) -> bool:
    """分母に含めるノードかどうかを判定"""
    if NODE_PREFIXES is None:
        return True
    return any(name.startswith(pfx) for pfx in NODE_PREFIXES)


def msg_is_target(mid: str) -> bool:
    """対象メッセージプレフィックスかどうか"""
    return any(mid.startswith(pfx) for pfx in MESSAGE_PREFIXES)


def get_output_dir_for_log(log_file: str) -> Path:
    """
    ログファイル名ごとに出力ディレクトリを作成する。
    例: 123_EventLogReport_pri.txt → csv_out/123_EventLogReport_pri/
    """
    base = Path(log_file).stem  # 拡張子抜きのファイル名
    out_dir = Path("csv_out") / base
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def parse_log_for_prefixes(log_file: str):
    """
    ログを一回なめて、
    - 各メッセージの生成時刻（event == 'C'）
    - 各メッセージ・各ノードの最初の受信時刻（status == 'R'）
    - 分母となるノード集合（p*, q*, r* など）
    をまとめて取得する。
    """
    creation_time = {}                  # msg_id -> time
    first_recv_time = defaultdict(dict) # msg_id -> {node -> time}
    target_nodes = set()
    max_time_in_log = SIM_START

    with open(log_file, "r") as f:
        for line in f:
            parts = line.split()
            if len(parts) != 6:
                continue

            t_str, ev, src, dst, mid, status = parts
            try:
                t = float(t_str)
            except ValueError:
                continue

            # ログ全体の最大時刻
            if t > max_time_in_log:
                max_time_in_log = t

            # 分母に含めるノード集合を更新
            for node in (src, dst):
                if node_is_target(node):
                    target_nodes.add(node)

            # 対象メッセージプレフィックス以外はスキップ
            if not msg_is_target(mid):
                continue

            # ★ 生成イベント (event == 'C')
            if ev == "C":
                if mid not in creation_time or t < creation_time[mid]:
                    creation_time[mid] = t

            # 受信イベント (status == 'R') の最初の受信時刻
            if status == "R":
                node = dst
                if not node_is_target(node):
                    continue
                if (node not in first_recv_time[mid]
                        or t < first_recv_time[mid][node]):
                    first_recv_time[mid][node] = t

    # 生成イベントが無いメッセージについては、
    # 「そのメッセージの最初の受信時刻」を生成時刻の近似として使う
    for mid, recv_dict in first_recv_time.items():
        if mid not in creation_time and recv_dict:
            creation_time[mid] = min(recv_dict.values())

    return creation_time, first_recv_time, target_nodes, max_time_in_log


def compute_event_based_curve_for_message(
    msg_id: str,
    creation_time: float,
    first_recv_time_for_msg: dict,
    target_nodes: set,
    sim_end: float,
    ttl_secs: float,
):
    """
    1つのメッセージについて、
    「新しくどこかのノードに届いた時刻」だけで
    累計伝搬率を計算する。
    """
    if creation_time is None:
        # 生成も受信も無いようなメッセージ（実質存在しない）は無視
        return None

    # TTL到来時刻（生成時刻 + TTL）。シミュレーション終端とも比較して短い方を使う
    ttl_end_time = min(creation_time + ttl_secs, sim_end)

    # 分母となるノード数
    """
    N_nodes = len(target_nodes)
    """
    N_nodes = 900
    if N_nodes == 0:
        raise RuntimeError("分母となる対象ノードが0です。NODE_PREFIXES を確認してください。")

    # 対象ノードでの受信時刻（TTL以内のみ）を取り出し、時刻順に並べる
    recv_items = [
        (node, t)
        for node, t in first_recv_time_for_msg.items()
        if t <= ttl_end_time and node in target_nodes
    ]
    recv_items.sort(key=lambda x: x[1])

    times_sim = []            # シミュレーション時刻 [s]
    times_from_creation = []  # 生成後の経過時間 [s]
    reached_nodes_list = []   # 累計到達ノード数
    rate_list = []            # 累計伝搬率 [%]

    # 生成直後（まだ誰にも届いていない）状態を1行入れておく
    times_sim.append(creation_time)
    times_from_creation.append(0.0)
    reached_nodes_list.append(0)
    rate_list.append(0.0)  # 0%

    reached_nodes_set = set()

    for node, t in recv_items:
        if node in reached_nodes_set:
            continue  # 念のため二重カウント防止
        reached_nodes_set.add(node)

        elapsed = t - creation_time
        reached = len(reached_nodes_set)
        rate_percent = (reached / N_nodes) * 100.0  # ここで [%] に変換

        times_sim.append(t)
        times_from_creation.append(elapsed)
        reached_nodes_list.append(reached)
        rate_list.append(rate_percent)

    return {
        "msg_id": msg_id,
        "times_sim": times_sim,
        "times_from_creation": times_from_creation,
        "reached_nodes": reached_nodes_list,
        "rates_percent": rate_list,
        "ttl_end_time": ttl_end_time,
    }


def downsample_result_by_percent(result: dict, step: float = 1.0) -> dict:
    """
    累計伝搬率が step[%] 以上増えたタイミングだけに間引く。
    例: step=1.0 → 1% 以上増えたときの行だけ残す。
    最初の行と最後の行は必ず残す。
    """
    rates = result["rates_percent"]
    if not rates:
        return result

    idx_keep = [0]  # 最初の行は必ず残す
    last_rate = rates[0]

    for i in range(1, len(rates)):
        rate = rates[i]
        if rate - last_rate >= step - 1e-9:  # step%以上増えたら採用
            idx_keep.append(i)
            last_rate = rate

    # 最後の行を必ず含める
    if idx_keep[-1] != len(rates) - 1:
        idx_keep.append(len(rates) - 1)

    # 新しい result を作成
    def pick(lst):
        return [lst[i] for i in idx_keep]

    return {
        "msg_id": result["msg_id"],
        "times_sim": pick(result["times_sim"]),
        "times_from_creation": pick(result["times_from_creation"]),
        "reached_nodes": pick(result["reached_nodes"]),
        "rates_percent": pick(result["rates_percent"]),
        "ttl_end_time": result["ttl_end_time"],
    }


def save_curve_to_csv(result: dict, output_dir: Path, suffix: str = ""):
    """1メッセージ分の計算結果を CSV に保存"""
    msg_id = result["msg_id"]
    if suffix:
        csv_path = output_dir / f"{msg_id}_propagation_events{suffix}.csv"
    else:
        csv_path = output_dir / f"{msg_id}_propagation_events.csv"

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "message_id",
            "time_sim_sec",
            "time_from_creation_sec",
            "reached_nodes",
            "cumulative_propagation_rate_percent",
        ])
        for t_sim, t_rel, n, r in zip(
            result["times_sim"],
            result["times_from_creation"],
            result["reached_nodes"],
            result["rates_percent"],
        ):
            writer.writerow([msg_id, t_sim, t_rel, n, r])

    print(f"  -> CSV 出力: {csv_path}")


if __name__ == "__main__":
    # ログごとの出力フォルダを作成
    OUTPUT_DIR = get_output_dir_for_log(LOG_FILE)
    print(f"出力フォルダ: {OUTPUT_DIR}")

    # 1) ログ全体を1回パースして、M_Hp*, M_Hq*, M_Hr* 系の情報を全部取る
    creation_time_dict, first_recv_time_dict, target_nodes, max_time_in_log = \
        parse_log_for_prefixes(LOG_FILE)

    print(f"対象メッセージプレフィックス: {MESSAGE_PREFIXES}")
    print(f"  見つかった対象メッセージ数: {len(first_recv_time_dict)}")
    print(f"  分母ノード数: {len(target_nodes)}")
    print(f"  ログ最大時刻: {max_time_in_log} [s]")

    # 2) 各メッセージごとにイベントベースの累計伝搬率カーブを計算して CSV 出力
    for mid in sorted(first_recv_time_dict.keys()):
        creation_time = creation_time_dict.get(mid, None)
        first_recv_for_msg = first_recv_time_dict[mid]

        if creation_time is None and not first_recv_for_msg:
            # 本当に何も起きていないメッセージはスキップ
            continue

        print(f"\nメッセージ {mid}:")
        print(f"  Creation time (event C): {creation_time} [s]")
        result = compute_event_based_curve_for_message(
            msg_id=mid,
            creation_time=creation_time,
            first_recv_time_for_msg=first_recv_for_msg,
            target_nodes=target_nodes,
            sim_end=SIM_END,
            ttl_secs=TTL_SECS,
        )

        if result is None:
            print("  受信イベントがないためスキップ")
            continue

        print(f"  TTL end time: {result['ttl_end_time']} [s]")

        # フル解像度のCSV
        save_curve_to_csv(result, OUTPUT_DIR)

        # 1%刻みで間引いたCSV（Excel用）
        result_step = downsample_result_by_percent(result, step=1.0)
        save_curve_to_csv(result_step, OUTPUT_DIR, suffix="_step1pct")

    print("\n全メッセージの処理が完了しました。")