"""中继主体：定时经 UDP 向上游取时（异步、不阻塞下游），同时用 NTPv4
给下游授时；并内嵌 FastAPI 值班网页。全部跑在同一个 asyncio 事件循环里。

下游应答的时间戳约定（RFC 5905 四点）：
  T2 = 请求进入内核（入队）那一刻的中继时刻，按入队时的走时速率计算；
  T3 = 即将发出应答那一刻的中继时刻。
T3 - T2 就是“内核排队 + 中继处理”的耗时，客户端可用它扣除排队误差。
"""
import asyncio
import logging
import os
import socket
import time

import uvicorn

from common import ntp
from relay.clock import RelayClock
from relay.udp import TimestampedUdpServer
from relay.web import create_app

log = logging.getLogger("relay")

UPSTREAM_HOST = os.getenv("UPSTREAM_HOST", "127.0.0.1")
UPSTREAM_PORT = int(os.getenv("UPSTREAM_PORT", "10124"))
POLL_INTERVAL = float(os.getenv("RELAY_POLL_INTERVAL", "1.0"))   # 取时周期
REPLY_TIMEOUT = float(os.getenv("RELAY_REPLY_TIMEOUT", "1.5"))   # 答复超时
LISTEN_PORT = int(os.getenv("RELAY_UDP_PORT", "10123"))          # 对下游授时端口
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))
QUEUE_MAX_AGE = float(os.getenv("RELAY_QUEUE_MAX_AGE", "1.0"))   # 入队超龄即丢弃
RCVBUF_BYTES = int(os.getenv("RELAY_UDP_RCVBUF", str(1 << 20)))  # 下游接收缓冲


class Stats:
    def __init__(self):
        self.sent = 0           # 发出的上游请求
        self.valid = 0          # 有效答复
        self.late = 0           # 超时答复（丢弃，不刷新同步时间）
        self.unknown = 0        # 无法匹配的答复
        self.downstream_queries = 0   # 合法的下游请求
        self.dropped_stale = 0  # 入队超过 1 秒被丢弃的请求
        self.last_queue_ms = None     # 最近一次 T3-T2
        self.max_queue_ms = 0.0       # 观测到的最大 T3-T2


class UpstreamClient(asyncio.DatagramProtocol):
    """NTP 客户端：周期性向上游发请求；等待答复期间事件循环照常服务下游。"""

    def __init__(self, clock: RelayClock, stats: Stats, upstream_addr):
        self.clock = clock
        self.stats = stats
        self.upstream_addr = upstream_addr
        self.transport = None
        # origin 时间戳 -> (发送单调时刻, 超时截止, 发送 unix 时刻 T1)
        self.outstanding = {}

    def connection_made(self, transport):
        self.transport = transport

    def send_request(self):
        if self.transport is None:
            return
        now_mono = time.monotonic()
        t1 = time.time()
        pkt = ntp.build_request(t1)
        self.outstanding[pkt[40:48]] = (now_mono, now_mono + REPLY_TIMEOUT, t1)
        # 清理早已超时的记录，避免泄漏
        stale = [k for k, v in self.outstanding.items() if v[1] < now_mono - 60]
        for k in stale:
            del self.outstanding[k]
        self.stats.sent += 1
        self.transport.sendto(pkt, self.upstream_addr)

    def datagram_received(self, data, addr):
        now_mono = time.monotonic()
        t4 = time.time()
        if len(data) < 48:
            return
        msg = ntp.parse(data)
        rec = self.outstanding.pop(msg["origin_raw"], None)
        if rec is None:
            self.stats.unknown += 1
            return
        _send_mono, deadline, t1 = rec
        if now_mono > deadline:
            # 超时答复：丢弃。不得更新时钟，也不得延长 5 秒同步窗口
            self.stats.late += 1
            log.info("丢弃超时答复（晚到 %.0f ms）", (now_mono - deadline) * 1000)
            return
        # 四点时间戳估算偏差：theta = ((T2-T1)+(T3-T4))/2
        theta = ((msg["recv"] - t1) + (msg["xmit"] - t4)) / 2.0
        upstream_now = t4 + theta
        before = self.clock.now(now_mono)
        self.clock.update(upstream_now, now_mono)
        self.stats.valid += 1
        gap = self.clock.last_gap
        if before is None:
            log.info("首次对时完成，时刻=%.3f", upstream_now)
        elif gap is not None and abs(gap) > 0.05:
            log.info("校准偏差 %.1f ms，走时速度调整为 %.0f%%",
                     gap * 1000, self.clock.status()["rate"] * 100)

    def error_received(self, exc):  # ICMP 不可达等，交由超时机制处理
        log.warning("上游通道异常: %s", exc)


class DownstreamServer:
    """NTP 服务端：随时回应下游查询；未同步时回包置 LI=3。"""

    def __init__(self, clock: RelayClock, stats: Stats):
        self.clock = clock
        self.stats = stats
        self.transport = None
        self.last_result = None

    def handle(self, data, addr, recv_mono: float):
        if len(data) < 48 or (data[0] & 0x7) != ntp.MODE_CLIENT:
            return
        stats = self.stats
        stats.downstream_queries += 1

        # 处理（读取）时刻：用于判断排队时长
        proc_mono = time.monotonic()
        # 先把时钟分段推进到“现在”，保证下面回查入队时刻时能看到
        # 这期间所有已到期的缓调切速点
        self.clock.advance(proc_mono)
        queue_age = proc_mono - recv_mono
        if queue_age > QUEUE_MAX_AGE:
            stats.dropped_stale += 1
            log.info("丢弃入队 %.0f ms 的过期请求", queue_age * 1000)
            return

        # T2：按请求入队时中继的走时计算（跨缓调切速也一致）
        synced_at_enqueue = not self.clock.never_synced_at(recv_mono)
        t2 = self.clock.now_at(recv_mono) if synced_at_enqueue else 0.0
        if t2 is None:      # 入队时刻早于仍保留的时钟历史（不应发生）
            t2 = 0.0
            synced_at_enqueue = False

        # T3：即将发包时的中继时刻
        send_mono = time.monotonic()
        t3 = self.clock.now_at(send_mono)
        if t3 is None:      # 入队时已对时、但取值时点不在分段表内（极端情况）
            t3 = t2 or 0.0

        # 同步标记：入队时还没完成首次对时 -> 照常回未同步；
        # 其余情况以发包时的状态为准。
        synced_now = (synced_at_enqueue
                      and self.clock.synchronized(send_mono))
        li = ntp.LI_OK if synced_now else ntp.LI_ALARM
        stratum = 2 if synced_now else 16

        pkt = ntp.build_response(li, stratum, data[40:48], t2, t3)
        sent = self.transport.send(pkt, addr) if self.transport else False

        # 排队耗时按时刻差直接相减（同一时钟轴，不受时间戳 32 位截断影响）；
        # 若入队早于首次对时（T2=0），没有可用的中继走时，不参与统计
        queue_ms = round((t3 - t2) * 1000, 3) if synced_at_enqueue else None
        if queue_ms is not None:
            stats.last_queue_ms = queue_ms
            stats.max_queue_ms = max(stats.max_queue_ms, queue_ms)
        self.last_result = {
            "t2": t2,
            "t3": t3,
            "queue_ms": queue_ms,
            "queue_age_ms": round(queue_age * 1000, 3),
            "sync_at_enqueue": synced_at_enqueue,
            "synchronized": synced_now,
            "li": li,
            "stratum": stratum,
            "source": addr,
            "sent": sent,
        }
        return self.last_result


async def poll_loop(client: UpstreamClient):
    """按固定节拍取时：即使某轮被下游积压拖慢，也回到原周期而不是往后顺延。"""
    next_at = time.monotonic()
    while True:
        client.send_request()
        next_at += POLL_INTERVAL
        delay = next_at - time.monotonic()
        if delay < 0:
            # 已错过节拍（不应发生，除非事件循环被长时间阻塞）：立即补发并重新对齐
            next_at = time.monotonic()
            delay = 0
        await asyncio.sleep(delay)


async def monitor_loop(clock: RelayClock):
    last = None
    while True:
        synced = clock.synchronized()
        if last is not None and synced != last:
            log.warning("同步状态变化 -> %s", "同步" if synced else "未同步")
        last = synced
        await asyncio.sleep(0.5)


async def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    loop = asyncio.get_running_loop()
    clock = RelayClock()
    stats = Stats()

    # 启动时解析一次上游地址（compose 网络内主机名稳定）
    infos = await loop.getaddrinfo(UPSTREAM_HOST, UPSTREAM_PORT,
                                   type=socket.SOCK_DGRAM)
    upstream_addr = infos[0][4]
    log.info("上游 %s:%s -> %s", UPSTREAM_HOST, UPSTREAM_PORT, upstream_addr)

    client = UpstreamClient(clock, stats, upstream_addr)
    await loop.create_datagram_endpoint(lambda: client,
                                        local_addr=("0.0.0.0", 0))

    server = DownstreamServer(clock, stats)
    udp = TimestampedUdpServer(loop, server.handle, LISTEN_PORT,
                               rcvbuf=RCVBUF_BYTES)
    server.transport = udp
    log.info("下游授时 UDP 端口 %d（内核收包时间戳=%s）；取时周期 %.1fs；"
             "答复超时 %.1fs；队列丢弃阈值 %.1fs",
             LISTEN_PORT, "开" if udp.kernel_ts else "关",
             POLL_INTERVAL, REPLY_TIMEOUT, QUEUE_MAX_AGE)

    asyncio.create_task(poll_loop(client))
    asyncio.create_task(monitor_loop(clock))

    app = create_app(clock, stats, server, udp)
    config = uvicorn.Config(app, host="0.0.0.0", port=WEB_PORT,
                            log_level="info")
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(main())
