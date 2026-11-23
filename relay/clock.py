"""中继虚拟时钟：按最后一次同步继续走时；首次对时后不倒退，只能以
正常速度的 90%~110% 缓调（slew），直至追上上游。

时钟状态被切成若干线性分段 ``(起始单调时刻, 起始中继时刻, 速率)``：
每次重新锚定或缓调切换都会追加一段，旧分段保持原样。因此可以精确回答
“某个过去的单调时刻，中继时刻是多少、当时走时速率是多少”，这正是把
内核排队时间换算成中继时刻 T2 所需要的（入队后即使发生过缓调切速，
T2 仍按入队时的走时计算）。
"""
import time
from collections import deque

MIN_RATE = 0.9   # 最慢走时（上游回拨/落后时）
MAX_RATE = 1.1   # 最快走时（追赶上游时）
SYNC_TTL = 5.0   # 距最后一次有效上游答复满 5 秒即判未同步
SETTLE_EPS = 0.0005  # 小于 0.5ms 的偏差直接忽略，避免抖动

# 只为最近的历史保留分段，排队请求最多看 1 秒前，余量足够
HISTORY_SECONDS = 120.0
MAX_SEGMENTS = 1024


class RelayClock:
    def __init__(self):
        self.synced_once = False        # 是否完成过首次对时
        self.first_sync_mono = None     # 首次对时的单调时刻
        self._rate = 1.0                # 当前走时速度
        self._revert_at = None          # 缓调结束、恢复 1.0 的单调时刻
        # 分段表：(start_mono, value_at_start, rate)；按 start_mono 递增
        self._segments = deque()
        self.last_valid_mono = None     # 最后一次有效上游答复的单调时刻
        self.last_gap = None            # 最后一次测得的与上游的差（秒）

    # ---- 内部：分段维护 -------------------------------------------------
    def _materialize(self, upper_mono: float):
        """把缓调到期点补进分段表（可能连续多段），并裁掉过老历史。

        注意：裁决只依据已经确定的边界，不能用“当前时间”做前提，
        否则用较早的 monotonic 时间回查历史时会漏掉中间的切速点。
        """
        changed = False
        while self._revert_at is not None and upper_mono >= self._revert_at:
            boundary = self._revert_at
            start, value, rate = self._segments[-1]
            self._segments.append((boundary, value + (boundary - start) * rate, 1.0))
            self._rate = 1.0
            self._revert_at = None
            changed = True
        if changed and self._segments:
            self._prune(self._segments[-1][0])

    def _prune(self, now_mono: float):
        cutoff = now_mono - HISTORY_SECONDS
        while len(self._segments) > 2 and self._segments[1][0] < cutoff:
            self._segments.popleft()

    def _segment_at(self, mono: float):
        """返回覆盖 mono 的分段 (start, value, rate)；早于首次对时返回 None。"""
        if not self._segments or mono < self._segments[0][0]:
            return None
        for seg in reversed(self._segments):
            if seg[0] <= mono:
                return seg
        return None

    def _value_at(self, mono: float):
        seg = self._segment_at(mono)
        if seg is None:
            return None
        start, value, rate = seg
        return value + (mono - start) * rate

    # ---- 对外查询 -------------------------------------------------------
    def now(self, now_mono: float | None = None):
        """当前中继时刻（unix 秒）；从未对时或查询早于首次对时则返回 None。"""
        if not self.synced_once:
            return None
        if now_mono is None:
            now_mono = time.monotonic()
        self._materialize(now_mono)
        return self._value_at(now_mono)

    def advance(self, mono: float):
        """把分段表推进到 mono（把已到期的缓调切速点落成分段）。"""
        if self.synced_once:
            self._materialize(mono)

    def now_at(self, mono: float):
        """在任意（通常是过去的）单调时刻的中继时刻，按当时的走时速率计算。"""
        if not self.synced_once:
            return None
        return self._value_at(mono)

    def rate_at(self, mono: float):
        if not self.synced_once:
            return None
        seg = self._segment_at(mono)
        return None if seg is None else seg[2]

    def never_synced_at(self, mono: float) -> bool:
        """该单调时刻是否还没完成首次对时。"""
        return self.first_sync_mono is None or mono < self.first_sync_mono

    def update(self, upstream_now: float, now_mono: float):
        """收到有效上游答复后校准。首次直接跳变；之后只缓调、不倒退。"""
        self._materialize(now_mono)
        if not self.synced_once:
            self.synced_once = True
            self.first_sync_mono = now_mono
            self._segments.append((now_mono, upstream_now, 1.0))
            self._rate = 1.0
            self._revert_at = None
            self.last_gap = 0.0
        else:
            current = self._value_at(now_mono)
            gap = upstream_now - current
            self.last_gap = gap
            if abs(gap) < SETTLE_EPS:
                rate = 1.0
                revert_at = None
            elif gap > 0:
                # 落后上游：加速到 110% 追赶
                rate = MAX_RATE
                revert_at = now_mono + gap / (MAX_RATE - 1.0)
            else:
                # 上游回拨/落后：减速到 90%，等上游追上来，绝不倒退
                rate = MIN_RATE
                revert_at = now_mono + (-gap) / (1.0 - MIN_RATE)
            # 走时状态没变（常速、无待切速点）时不必追加分段，分段表保持稀疏
            if rate != self._rate or revert_at != self._revert_at:
                # 在 now_mono 处续接一段，历史分段保持不变
                self._segments.append((now_mono, current, rate))
                self._rate = rate
                self._revert_at = revert_at
                self._prune(now_mono)
                while len(self._segments) > MAX_SEGMENTS:
                    self._segments.popleft()
        self.last_valid_mono = now_mono

    def synchronized(self, now_mono: float | None = None) -> bool:
        """尚未同步，或距最后一次有效答复已满 5 秒 -> 未同步。"""
        if not self.synced_once or self.last_valid_mono is None:
            return False
        if now_mono is None:
            now_mono = time.monotonic()
        return (now_mono - self.last_valid_mono) < SYNC_TTL

    def status(self) -> dict:
        now_mono = time.monotonic()
        self._materialize(now_mono)
        return {
            "time": self._value_at(now_mono),
            "synced_once": self.synced_once,
            "synchronized": self.synchronized(now_mono),
            "rate": self._rate,
            "slew_remaining": (None if self._revert_at is None
                               else max(0.0, self._revert_at - now_mono)),
            "last_valid_age": (None if self.last_valid_mono is None
                               else now_mono - self.last_valid_mono),
            "last_gap": self.last_gap,
        }
