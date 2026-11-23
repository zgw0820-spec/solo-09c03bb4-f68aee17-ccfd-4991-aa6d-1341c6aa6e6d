"""中继主体：定时经 UDP 向上游取时（异步、不阻塞下游），同时用 NTPv4
给下游授时；并内嵌 FastAPI 值班网页。全部跑在同一个 asyncio 事件循环里。"""
import asyncio
import logging
import os
import socket
import time

import uvicorn

from common import ntp
from relay.clock import RelayClock
from relay.web import create_app

log = logging.getLogger("relay")

UPSTREAM_HOST = os.getenv("UPSTREAM_HOST", "127.0.0.1")
UPSTREAM_PORT = int(os.getenv("UPSTREAM_PORT", "10124"))
POLL_INTERVAL = float(os.getenv("RELAY_POLL_INTERVAL", "1.0"))   # 取时周期
REPLY_TIMEOUT = float(os.getenv("RELAY_REPLY_TIMEOUT", "1.5"))   # 答复超时
LISTEN_PORT = int(os.getenv("RELAY_UDP_PORT", "10123"))          # 对下游授时端口
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))


class Stats:
    def __init__(self):
        self.sent = 0      # 发出的上游请求
        self.valid = 0     # 有效答复
        self.late = 0      # 超时答复（丢弃，不刷新同步时间）
        self.unknown = 0   # 无法匹配的答复


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


class DownstreamServer(asyncio.DatagramProtocol):
    """NTP 服务端：随时回应下游查询；未同步时回包置 LI=3。"""

    def __init__(self, clock: RelayClock):
        self.clock = clock
        self.queries = 0
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        if len(data) < 48 or (data[0] & 0x7) != ntp.MODE_CLIENT:
            return
        self.queries += 1
        now_mono = time.monotonic()
        t = self.clock.now(now_mono)
        synced = self.clock.synchronized(now_mono)
        li = ntp.LI_OK if synced else ntp.LI_ALARM
        stratum = 2 if synced else 16
        serve = t if t is not None else 0.0  # 从未对时：时刻置 0，仅标未同步
        pkt = ntp.build_response(li, stratum, data[40:48], serve, serve)
        self.transport.sendto(pkt, addr)


async def poll_loop(client: UpstreamClient):
    while True:
        client.send_request()
        await asyncio.sleep(POLL_INTERVAL)


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

    server = DownstreamServer(clock)
    await loop.create_datagram_endpoint(lambda: server,
                                        local_addr=("0.0.0.0", LISTEN_PORT))
    log.info("下游授时 UDP 端口 %d；取时周期 %.1fs；答复超时 %.1fs",
             LISTEN_PORT, POLL_INTERVAL, REPLY_TIMEOUT)

    asyncio.create_task(poll_loop(client))
    asyncio.create_task(monitor_loop(clock))

    app = create_app(clock, stats, server)
    config = uvicorn.Config(app, host="0.0.0.0", port=WEB_PORT,
                            log_level="info")
    await uvicorn.Server(config).serve()


if __name__ == "__main__":
    asyncio.run(main())
