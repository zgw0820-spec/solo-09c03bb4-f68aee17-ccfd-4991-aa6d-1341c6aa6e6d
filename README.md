# 机房内网 NTP 中继 + 值班网页

机房采集机靠内网授时：本地 NTP 时间源（上游）→ 中继（定时取时 + 对下游授时）→ 下游采集机。
中继短时断网仍按最后一次同步继续走时；FastAPI 值班网页用于观察和演练。

## 组成

| 组件 | 说明 | 端口（容器内） |
|---|---|---|
| `upstream/` | 本地 NTP 时间源：UDP 授时 + 控制面（钟差/回包延迟/断开） | UDP 123，HTTP 8001 |
| `relay/` | 中继：NTP 客户端定时向上游取时，NTPv4 服务端对下游授时，内嵌值班网页 | UDP 123，HTTP 8000 |
| `common/ntp.py` | 极简 NTPv4 报文编解码（RFC 5905，48 字节） | - |
| `tests/selftest.py` | 交付自测：断连超期 / 上游回拨 / 超时答复 | - |

## 启动

```bash
docker compose up --build -d
# 值班网页: http://localhost:8000
# 宿主机试查中继: UDP 127.0.0.1:8123
```

## 行为要点

- 中继每 1s 经 UDP 向上游取时（`RELAY_POLL_INTERVAL`），异步收发，**等待上游答复期间照常回应下游查询**。
- 答复超时 `RELAY_REPLY_TIMEOUT`（默认 1.5s）：**超时答复直接丢弃**，不校时钟、也不延长同步窗口。
- **同步判定**：尚未同步，或距最后一次有效答复已满 5 秒 → 回包 LI=3（未同步）、stratum=16；否则 LI=0、stratum=2。
- **断网续走**：失去有效答复后按最后一次同步的速率继续走时。
- **不倒退**：首次对时后，即使上游回拨或重连，中继时钟只以正常速度的 90%~110% 缓调，直至追上上游，绝不回拨。
- 值班网页可：调上游钟差（负数即回拨）、调回包延迟、断开上游、向中继发 UDP 试查并查看授时时刻（毫秒）与同步状态。

## 网页 / API

- `GET /` 值班网页
- `GET /api/status` 中继状态（时刻、同步、速率、最后有效答复年龄、报文统计、上游配置）
- `POST /api/upstream` 调上游 `{"offset_seconds": -30, "delay_ms": 2000, "disconnected": false}`
- `POST /api/query` 网页后端经 UDP 试查中继，返回 T3 时刻（毫秒）、LI、stratum、往返时延

## 自测

```bash
python3 tests/selftest.py        # 需服务已启动，BASE 默认 http://127.0.0.1:8000
```

覆盖：断连满 5 秒标未同步且继续走时、超时答复被丢弃、上游回拨 30s 时中继不倒退且无跳变（90% 缓调）。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `RELAY_POLL_INTERVAL` | 1.0 | 中继取时周期（秒） |
| `RELAY_REPLY_TIMEOUT` | 1.5 | 上游答复超时（秒），超时答复丢弃 |
| `RELAY_UDP_PORT` | 123（容器） | 中继对下游授时端口 |
| `UPSTREAM_HOST/PORT` | upstream/123 | 上游地址 |
| `UPSTREAM_API` | http://upstream:8001 | 上游控制面 |
| `WEB_PORT` | 8000 | 值班网页端口 |
