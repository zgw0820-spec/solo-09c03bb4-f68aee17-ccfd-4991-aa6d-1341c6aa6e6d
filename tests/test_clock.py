"""中继时钟与下游报文处理的离线用例（不依赖 FastAPI / 网络）：

  1) 跨缓调切速：排队请求的 T2 必须按入队时的走时速率换算；
  2) 历史回查：时钟先前进到“现在”、再回查更早的单调时刻，结果不能被
     “现在”的裁决污染（切速点必须仍然生效）；
  3) T2/T3 写入 NTP 报文的 receive/transmit 字段并可正确读回。

运行：python3 tests/test_clock.py
"""
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(ROOT, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sys.path.insert(0, ROOT)
from common import ntp        # noqa: E402
from relay.clock import RelayClock, MIN_RATE, MAX_RATE, SETTLE_EPS  # noqa: E402

RESULTS = []


def check(name, ok, detail=""):
    RESULTS.append((name, bool(ok)))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}  {detail}")


def test_basic_slew_and_requery():
    c = RelayClock()
    check("未对时 now_at 为 None", c.now_at(0.0) is None)
    c.update(1000.0, 10.0)          # 首次对时
    check("首次对时后 now", abs(c.now(10.0) - 1000.0) < 1e-9)
    check("首次对时前 never_synced_at", c.never_synced_at(9.999))
    check("首次对时后已对时", not c.never_synced_at(10.0))

    # 上游落后 20s -> 90% 缓调，追平需 200s（回拨场景，绝不倒退）
    c.update(990.0, 20.0)
    check("落后时进入 90% 缓调", c.rate_at(20.0) == MIN_RATE)
    expect = 1010.0                 # 10.0->20.0 常速走时，当前中继时刻 = 1010
    check("缓调起点连续（不跳变）", abs(c.now(20.0) - expect) < 1e-9,
          f"now={c.now(20.0):.6f}")
    check("缓调期间按 90% 走时", abs((c.now(21.0) - c.now(20.0)) - MIN_RATE) < 1e-9)
    # 追平点：gap=20s，需 20/(1-0.9)=200s（浮点边界不取整秒）
    check("缓调到期恢复 1.0", abs((c.now(222.0) - c.now(221.0)) - 1.0) < 1e-9)

    # 历史上入队的请求：在 20.5s 入队、21.0s 发包
    t2 = c.now_at(20.5)
    t3 = c.now_at(21.0)
    check("跨切速 T2 用入队时走时", abs((t3 - t2) - 0.5 * MIN_RATE) < 1e-9,
          f"T3-T2={t3-t2:.6f}")


def test_history_not_polluted_by_now():
    c = RelayClock()
    c.update(500.0, 0.0)
    c.update(500.0 + 2.0, 1.0)      # 落后 2s，110% 追赶，20s 后追平
    check("进入 110% 快调", c.rate_at(1.0) == MAX_RATE)
    # 先把时钟推到 100s（远超切速点 21s），让“现在”已恢复常速
    c.now(100.0)
    before = c.now_at(5.0)
    after = c.now_at(5.5)
    check("回查历史仍用 110%",
          abs((after - before) - 0.5 * MAX_RATE) < 1e-9,
          f"rate@5s={c.rate_at(5.0)} Δ={after-before:.6f}")
    # 反序回查（先近后远）也一样
    a = c.now_at(10.0)
    b = c.now_at(2.0)
    check("乱序回查结果一致", abs((a - b) - (8.0 * MAX_RATE)) < 1e-9
          or abs((a - b) - (8.0 * MAX_RATE)) < 1e-6)


def test_packet_fields():
    t2, t3 = 1795399387.123456, 1795399387.623456
    pkt = ntp.build_response(ntp.LI_OK, 2, b"\x00" * 8, t2, t3)
    m = ntp.parse(pkt)
    check("receive 字段承载 T2", abs(m["recv"] - t2) < 1e-6, f"{m['recv']}")
    check("transmit 字段承载 T3", abs(m["xmit"] - t3) < 1e-6, f"{m['xmit']}")
    check("T3−T2 可读回", abs((m["xmit"] - m["recv"]) - 0.5) < 1e-6)


def test_sync_ttl():
    c = RelayClock()
    c.update(0.0, 100.0)
    check("有效答复内标同步", c.synchronized(104.9))
    check("满 5 秒标未同步", not c.synchronized(105.0))


def main():
    test_basic_slew_and_requery()
    test_history_not_polluted_by_now()
    test_packet_fields()
    test_sync_ttl()
    failed = sum(1 for _, ok in RESULTS if not ok)
    print(f"\n==== 时钟离线用例: {len(RESULTS)-failed}/{len(RESULTS)} 通过 ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
