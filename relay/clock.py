"""中继虚拟时钟：按最后一次同步继续走时；首次对时后不倒退，只能以
正常速度的 90%~110% 缓调（slew），直至追上上游。"""
import time

MIN_RATE = 0.9   # 最慢走时（上游回拨/落后时）
MAX_RATE = 1.1   # 最快走时（追赶上游时）
SYNC_TTL = 5.0   # 距最后一次有效上游答复满 5 秒即判未同步
SETTLE_EPS = 0.0005  # 小于 0.5ms 的偏差直接忽略，避免抖动


class RelayClock:
    def __init__(self):
        self.synced_once = False        # 是否完成过首次对时
        self._base_mono = 0.0           # 锚点：本地单调时钟
        self._base_time = 0.0           # 锚点：中继时刻
        self._rate = 1.0                # 当前走时速度
        self._revert_at = None          # 缓调结束、恢复 1.0 的单调时刻
        self.last_valid_mono = None     # 最后一次有效上游答复的单调时刻
        self.last_gap = None            # 最后一次测得的与上游的差（秒）

    def _settle(self, now_mono: float):
        """若缓调已到期，把锚点推进到缓调结束点并恢复常速。"""
        if self._revert_at is not None and now_mono >= self._revert_at:
            self._base_time += (self._revert_at - self._base_mono) * self._rate
            self._base_mono = self._revert_at
            self._rate = 1.0
            self._revert_at = None

    def now(self, now_mono: float | None = None):
        """当前中继时刻（unix 秒）；从未对时则返回 None。"""
        if not self.synced_once:
            return None
        if now_mono is None:
            now_mono = time.monotonic()
        self._settle(now_mono)
        return self._base_time + (now_mono - self._base_mono) * self._rate

    def update(self, upstream_now: float, now_mono: float):
        """收到有效上游答复后校准。首次直接跳变；之后只缓调、不倒退。"""
        if not self.synced_once:
            self.synced_once = True
            self._base_mono = now_mono
            self._base_time = upstream_now
            self._rate = 1.0
            self._revert_at = None
            self.last_gap = 0.0
        else:
            self._settle(now_mono)
            current = self._base_time + (now_mono - self._base_mono) * self._rate
            # 重新锚定到当前点，再决定新的缓调速率
            self._base_time = current
            self._base_mono = now_mono
            gap = upstream_now - current
            self.last_gap = gap
            if abs(gap) < SETTLE_EPS:
                self._rate = 1.0
                self._revert_at = None
            elif gap > 0:
                # 落后上游：加速到 110% 追赶
                self._rate = MAX_RATE
                self._revert_at = now_mono + gap / (MAX_RATE - 1.0)
            else:
                # 上游回拨/落后：减速到 90%，等上游追上来，绝不倒退
                self._rate = MIN_RATE
                self._revert_at = now_mono + (-gap) / (1.0 - MIN_RATE)
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
        return {
            "time": self.now(now_mono),
            "synced_once": self.synced_once,
            "synchronized": self.synchronized(now_mono),
            "rate": self._rate,
            "last_valid_age": (None if self.last_valid_mono is None
                               else now_mono - self.last_valid_mono),
            "last_gap": self.last_gap,
        }
