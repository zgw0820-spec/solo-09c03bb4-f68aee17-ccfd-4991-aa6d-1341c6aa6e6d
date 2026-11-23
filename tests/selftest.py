#!/usr/bin/env python3
"""交付前自测（仅用标准库，经值班网页 API 驱动）：
  1) 断连超期：断开上游 >5s 后回包标未同步，断网期间继续走时不倒退
  2) 上游回拨：中继时钟不倒退、无跳变，以 90% 速度缓调追时
  3) 超时答复：回包延迟超过中继超时后，答复被丢弃，不延长 5 秒窗口
运行：BASE=http://127.0.0.1:8000 python3 tests/selftest.py
"""
import json
import os
import sys
import time
import urllib.request

BASE = os.environ.get("BASE", "http://127.0.0.1:8000")
RESULTS = []


def api(path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}")


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

    failed = sum(1 for _, ok in RESULTS if not ok)
    print(f"\n==== 自测完成: {len(RESULTS) - failed}/{len(RESULTS)} 通过 ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
