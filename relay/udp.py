"""带内核接收时间戳的 UDP 服务端。

下游请求可能先在内核接收队列里排队，事件循环稍后才读到。要把它算进中继
处理耗时，就必须拿到“数据报到内核的时刻”，而不是 userspace 读到它的时刻。
Linux 上用 SO_TIMESTAMPNS 让内核在 recvmsg 的辅助数据里回填这个时刻。

内核回填的是 CLOCK_REALTIME（含纳秒）时间戳，而中继时钟以
time.monotonic() 为时间轴。宿主机系统时钟连续走时（不发生阶跃）时，
两者的差是一个常量，启动时标定一次即可；标定取多次采样的最小值，
以减小用户态调度抖动带来的正偏差。
"""
import asyncio
import logging
import socket
import struct
import time

log = logging.getLogger("relay.udp")

# Python 3.11 的部分构建没有导出这两个常量，Linux 上内核编号固定为 35
SO_TIMESTAMPNS = getattr(socket, "SO_TIMESTAMPNS", 35)
SO_TIMESTAMP = getattr(socket, "SO_TIMESTAMP", 29)
CMSG_SPACE = socket.CMSG_SPACE
MSG_CTRUNC = getattr(socket, "MSG_CTRUNC", 0x8)

# 单次可读事件最多连续处理的报文数；到量后让出事件循环，
# 保证上游取时、网页等任务在队列积压时仍能按时运行。
DRAIN_BATCH = 256


def calibrate_realtime_offset(samples: int = 11) -> float:
    """返回 mono - realtime 的常量偏移（取多次采样的最小值）。"""
    best = None
    for _ in range(samples):
        real = time.time()
        mono = time.monotonic()
        delta = mono - real
        if best is None or delta < best:
            best = delta
    return best


class TimestampedUdpServer:
    """在 asyncio 事件循环里收包，并为每个报文附带内核接收时刻（单调轴）。"""

    def __init__(self, loop, handler, port: int, host: str = "0.0.0.0",
                 rcvbuf: int | None = None):
        self.loop = loop
        self.handler = handler            # handler(data, addr, recv_mono)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if rcvbuf:
            # 集中查时的突发请求先在内核排队；放大接收缓冲，少丢在核内
            try:
                self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
            except OSError:
                pass
        self.kernel_ts = True
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        except OSError:
            self.kernel_ts = False
            log.warning("内核不支持 SO_TIMESTAMPNS，改用用户态收包时刻（排队耗时不可见）")
        self.sock.bind((host, port))
        self.sock.setblocking(False)
        self.realtime_offset = calibrate_realtime_offset()
        self._closed = False
        loop.add_reader(self.sock.fileno(), self._on_readable)

    # ---- 收包 ---------------------------------------------------------
    def _recv(self):
        ancsize = CMSG_SPACE(16)
        data, anc, flags, addr = self.sock.recvmsg(512, ancsize)
        if self.kernel_ts and not (flags & MSG_CTRUNC):
            for level, ctype, cdata in anc:
                if level != socket.SOL_SOCKET:
                    continue
                if ctype == SO_TIMESTAMPNS and len(cdata) >= 16:
                    sec, nsec = struct.unpack("@ll", cdata[:16])
                    # 标定/内核差异可能让换算结果略晚于读取时刻；按读取
                    # 时刻封顶，避免出现负的排队时长。
                    return data, addr, min(sec + nsec / 1e9 + self.realtime_offset,
                                           time.monotonic())
                if ctype == SO_TIMESTAMP and len(cdata) >= 8:
                    sec, usec = struct.unpack("@ii", cdata[:8])
                    return data, addr, min(sec + usec / 1e6 + self.realtime_offset,
                                           time.monotonic())
        return data, addr, time.monotonic()

    def _on_readable(self):
        if self._closed:
            return
        for _ in range(DRAIN_BATCH):
            try:
                data, addr, recv_mono = self._recv()
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:
                log.warning("下游收包失败: %s", exc)
                return
            try:
                self.handler(data, addr, recv_mono)
            except Exception:  # 单个报文异常不影响后续收包
                log.exception("处理下游报文异常")
        # 本批到量可能还有积压：让出事件循环后再继续
        self.loop.call_soon(self._on_readable)

    # ---- 发包 ---------------------------------------------------------
    def send(self, data: bytes, addr) -> bool:
        try:
            self.sock.sendto(data, addr)
            return True
        except (BlockingIOError, InterruptedError):
            return False
        except OSError as exc:
            log.warning("下游回包失败: %s", exc)
            return False

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.loop.remove_reader(self.sock.fileno())
        finally:
            self.sock.close()
