#!/usr/bin/env python3
"""交付前自测（仅用标准库，经值班网页 API 驱动）：
  1) 断连超期：断开上游 >5s 后回包标未同步，断网期间继续走时不倒退
  2) 上游回拨：中继时钟不倒退、无跳变，以 90% 速度缓调追时
  3) 超时答复：回包延迟超过中继超时后，答复被丢弃，不延长 5 秒窗口
  4) 下游排队：冻结中继进程模拟内核排队，验证 T3−T2 如实反映排队耗时、
     值班网页给出扣除排队后的往返估计、入队超过 1 秒的请求被丢弃
  5) 跨缓调切速：上游回拨触发 90% 缓调，期间入队的请求 T2 仍按入队时
     的 90% 走时计算（而不是被发包时的速率污染）

运行：BASE=http://127.0.0.1:8000 python3 tests/selftest.py
排队/切速用例需要向中继进程发 SIGSTOP/SIGCONT，只在 Linux 同机运行时启用：
RELAY_PID=<中继进程号>。未设置时这些用例会标记为 SKIP。
"""
import importlib.util
import json
import os
import signal
import socket
import sys
import time
import urllib.request

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")
RELAY_PID = os.environ.get("RELAY_PID")
RELAY_PID = int(RELAY_PID) if RELAY_PID else None
RELAY_UDP_PORT = int(os.environ.get("RELAY_UDP_PORT", "10123"))
RESULTS = []

_spec = importlib.util.spec_from_file_location(
    "ntp_mod", os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "common", "ntp.py"))
ntp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ntp)


def api(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def check(name, ok, detail="", skip=False):
    RESULTS.append((name, bool(ok)))
    tag = "SKIP" if skip else ("PASS" if ok else "FAIL")
    print(f"[{tag}] {name}  {detail}")


def status():
    return api("/api/status")


def query():
    return api("/api/query", {})


def set_upstream(**kw):
    return api("/api/upstream", kw)


def wait_synced(want=True, timeout=25.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            if status()["synchronized"] == want:
                return True
        except Exception:
            pass
        time.sleep(0.4)
    return False


def udp_query_while_paused(hold: float):
    """在中继被 SIGSTOP 期间发请求，让报文在内核队列里排队 hold 秒。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(6)
    os.kill(RELAY_PID, signal.SIGSTOP)
    try:
        time.sleep(0.05)
        t1 = time.time()
        s.sendto(ntp.build_request(t1), ("127.0.0.1", RELAY_UDP_PORT))
        time.sleep(hold)
        t3_expected = time.time()
        os.kill(RELAY_PID, signal.SIGCONT)
        try:
            data, _ = s.recvfrom(512)
        except socket.timeout:
            return None
        t4 = time.time()
    finally:
        try:
            os.kill(RELAY_PID, signal.SIGCONT)
        except OSError:
            pass
        s.close()
    msg = ntp.parse(data)
    return {"msg": msg, "t1": t1, "t3_expected": t3_expected, "t4": t4}


def test_queue():
    print("== 4. 内核排队计入中继耗时 ==")
    if not RELAY_PID:
        check("排队用例（需 RELAY_PID）", True, "未设置 RELAY_PID", skip=True)
        return
    before = status()["stats"]
    hold = 0.4
    r = udp_query_while_paused(hold)
    check("排队期间仍正常应答", r is not None)
    if r is None:
        return
    q = (r["msg"]["xmit"] - r["msg"]["recv"]) * 1000
    rtt = (r["t4"] - r["t1"]) * 1000
    check("T3−T2 反映排队耗时", abs(q - hold * 1000) < 60,
          f"T3−T2={q:.1f}ms，实际冻结 {hold*1000:.0f}ms")
    check("T2 早于发包时刻", r["msg"]["recv"] < r["msg"]["xmit"])
    check("排队时同步标记照常", r["msg"]["li"] == 0 and r["msg"]["stratum"] == 2,
          f"li={r['msg']['li']} stratum={r['msg']['stratum']}")
    check("T3 早于客户端收到时刻", r["msg"]["xmit"] <= r["t4"] + 0.01)

    wq = query()
    check("网页试查给出 T3−T2", wq.get("queue_ms") is not None,
          f"queue_ms={wq.get('queue_ms')}")
    check("网页给出扣除排队后的往返估计",
          wq.get("adjusted_rtt_ms") is not None
          and wq["adjusted_rtt_ms"] <= wq["rtt_ms"],
          f"rtt={wq['rtt_ms']}ms adjusted={wq['adjusted_rtt_ms']}ms")

    # 冻结 1.2s > 1s 阈值：应答不得返回，且计入 dropped_stale
    r2 = udp_query_while_paused(1.2)
    after = status()["stats"]
    check("入队超过 1 秒的请求被丢弃", r2 is None)
    check("过期丢弃计入统计", after["dropped_stale"] > before["dropped_stale"],
          f"dropped_stale {before['dropped_stale']} -> {after['dropped_stale']}")


def test_slew_enqueue():
    print("== 5. 跨缓调切速时 T2 按入队走时计算 ==")
    if not RELAY_PID:
        check("跨切速用例（需 RELAY_PID）", True, "未设置 RELAY_PID", skip=True)
        return
    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    wait_synced(True)
    # 迅速回拨 40s：下一次有效答复后进入 90% 缓调（约 400s 才能追平）
    set_upstream(offset_seconds=-40)
    t0 = time.time()
    while status()["rate"] != 0.9:
        if time.time() - t0 > 6:
            check("进入 90% 缓调", False, "超时未进入缓调")
            break
        time.sleep(0.1)
    else:
        check("进入 90% 缓调", True)

    # 在缓调进行中暂停进程：请求先入队（当时速率 0.9），
    # 恢复后立即发包（此时中继已被再次校准，发包时速率可能仍是 0.9，
    # 但关键是 T3−T2 必须按入队后的真实经过时间累计，与实测 RTT 接近）
    hold = 0.5
    r = udp_query_while_paused(hold)
    check("缓调期间排队仍正常应答", r is not None)
    if r is not None:
        q = (r["msg"]["xmit"] - r["msg"]["recv"]) * 1000
        rtt = (r["t4"] - r["t1"]) * 1000
        # 中继按 0.9 走时，排队 0.5s 的中继时间应约为 450ms
        check("T2/T3 使用同一走时轴（≈0.9×排队）",
              abs(q - hold * 900) < 80,
              f"T3−T2={q:.1f}ms，预期≈{hold*900:.0f}ms")
        check("排队耗时不超过客户端实测往返", q <= rtt + 5,
              f"T3−T2={q:.1f}ms rtt={rtt:.1f}ms")

    print("   （对照）缓调中冻结 1.2s 仍应丢弃：")
    r2 = udp_query_while_paused(1.2)
    check("切速期间超龄请求照常丢弃", r2 is None)

    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    check("复位后恢复同步", wait_synced(True))


def main():
    print("== 0. 复位上游，等待首次对时 ==")
    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    check("首次对时", wait_synced(True))

    print("== 1. 断连超期 ==")
    set_upstream(disconnected=True)
    time.sleep(1.5)
    q1 = query()
    check("短时断网仍标同步", q1["synchronized"] is True, f"li={q1['li']}")
    time.sleep(4.5)  # 距最后一次有效答复 > 5 秒
    q2 = query()
    check("断连满 5 秒标未同步",
          q2["synchronized"] is False and q2["li"] == 3 and q2["stratum"] == 16,
          f"li={q2['li']} stratum={q2['stratum']}")
    t_a = q2["server_time"]
    time.sleep(1.0)
    q3 = query()
    t_b = q3["server_time"]
    check("断网期间按最后同步继续走时", 0.5 < (t_b - t_a) < 1.5,
          f"1.0s 内前进 {t_b - t_a:.3f}s")
    check("断网期间不倒退", t_b > t_a)

    print("== 2. 恢复上游 ==")
    set_upstream(disconnected=False)
    check("恢复后重新同步", wait_synced(True))

    print("== 3. 上游回拨 30 秒 ==")
    set_upstream(offset_seconds=-30)
    time.sleep(1.2)  # 等至少一个有效答复触发缓调
    samples, rates = [], []
    for _ in range(20):
        s = status()
        samples.append(s["time"])
        rates.append(s["rate"])
        time.sleep(0.2)
    deltas = [samples[i + 1] - samples[i] for i in range(len(samples) - 1)]
    check("回拨后时钟不倒退", all(d >= -1e-3 for d in deltas),
          f"最小相邻差 {min(deltas):+.4f}s")
    check("回拨后无跳变", max(deltas) < 0.5, f"最大相邻步进 {max(deltas):.3f}s")
    check("回拨后减速到 90% 追时", any(abs(r - 0.9) < 1e-9 for r in rates),
          f"观测速率={sorted(set(rates))}")
    check("回拨期间仍标同步", query()["synchronized"] is True)

    print("== 4. 超时答复不延长 5 秒窗口 ==")
    set_upstream(offset_seconds=0, delay_ms=2000)  # 延迟 > 中继 1.5s 答复超时
    late_before = status()["stats"]["late"]
    time.sleep(6.5)
    s = status()
    check("大延迟下标未同步", s["synchronized"] is False,
          f"距最后有效答复 {s['last_valid_age']:.1f}s")
    check("超时答复被丢弃（不计有效）", s["stats"]["late"] > late_before,
          f"late {late_before} -> {s['stats']['late']}")

    print("== 5. 复位 ==")
    set_upstream(offset_seconds=0, delay_ms=0, disconnected=False)
    check("复位后重新同步", wait_synced(True))
    q = query()
    check("授时时刻精确到毫秒",
          q["ok"] and isinstance(q["server_time_str"], str)
          and "." in q["server_time_str"],
          q.get("server_time_str", ""))

    test_queue()
    test_slew_enqueue()

    failed = sum(1 for _, ok in RESULTS if not ok)
    print(f"\n==== 自测完成: {len(RESULTS) - failed}/{len(RESULTS)} 通过 ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
