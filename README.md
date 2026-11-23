# 机房内网 NTP 中继 + 值班网页

机房采集机靠内网授时：本地 NTP 时间源（上游）→ 中继（定时取时 + 对下游授时）→ 下游采集机。
中继短时断网仍按最后一次同步继续走时；FastAPI 值班网页用于观察和演练。

采集机集中查时，UDP 请求可能先在**内核接收队列**里排队。中继把这段内核排队
时间计入自身的“处理耗时”，并在应答里用 T2 / T3 明确给出，客户端可据此扣除
排队误差。

## 组成

| 组件 | 说明 | 端口（容器内） |
|---|---|---|
| `upstream/` | 本地 NTP 时间源：UDP 授时 + 控制面（钟差/回包延迟/断开） | UDP 123，HTTP 8001 |
| `relay/` | 中继：NTP 客户端定时向上游取时，NTPv4 服务端对下游授时，内嵌值班网页 | UDP 123，HTTP 8000 |
| `relay/udp.py` | 带内核收包时间戳（`SO_TIMESTAMPNS`）的 UDP 服务端 | - |
| `common/ntp.py` | 极简 NTPv4 报文编解码（RFC 5905，48 字节） | - |
| `tests/selftest.py` | 端到端自测：断连超期 / 回拨缓调 / 超时答复 / 排队计入 / 跨切速 | - |
| `tests/test_clock.py` | 时钟与报文离线用例（无需启动服务） | - |

## 启动

```bash
docker compose up --build -d
# 值班网页: http://localhost:8000
# 宿主机试查中继: UDP 127.0.0.1:8123
```

## 下游应答的时间戳语义

NTPv4 报文里的两个服务端时间戳改为中继时刻的中继处理区间：

| 字段 | 值 | 含义 |
|---|---|---|
| receive timestamp | **T2** | 请求进入内核（入队）那一刻的中继时刻 |
| transmit timestamp | **T3** | 即将发出应答那一刻的中继时刻 |

- **T3 − T2** 即“内核排队 + 中继处理”耗时。客户端用它扣除排队引起的
  往返偏差：`adjusted_rtt = RTT_meas − (T3 − T2)`。
- T2 用**入队当时的走时速率**换算。若入队后中继发生缓调切速（90%/110%），
  T2 仍按入队时的分段计算，不会被后来的速率污染。
- 入队时中继还没完成首次对时 → 照常回未同步（LI=3、stratum=16），T2 填 0。
- 其余应答的同步标记（LI / stratum）以**发包时**的同步状态为准。
- 入队超过 `RELAY_QUEUE_MAX_AGE`（默认 1 秒）的请求直接丢弃、不应答。

实现要点：

- Linux 上用 `SO_TIMESTAMPNS` 让内核在 `recvmsg` 辅助数据里回填纳秒级
  收包时刻（`CLOCK_REALTIME`）。宿主机系统时钟假定连续走时，启动时标定一次
  `mono − realtime` 常量（多次取最小值）把它换算到 `time.monotonic()` 轴；
  若内核不支持该选项，自动退化为用户态读取时刻并标注
  `kernel_timestamp=false`。
- 中继时钟保存最近 120s 的线性分段 `(起点, 起点时刻, 速率)`，因此能精确
  回答任意过去时刻的中继时刻。

## 行为要点

- 中继每 1s 经 UDP 向上游取时（`RELAY_POLL_INTERVAL`）。取时循环按固定节拍
  对齐：**即使下游队列有积压、事件循环被短暂拖慢，上游取时仍回到原周期**，
  不会逐轮顺延。
- 下游收包按批处理（每批上限 256 个，随后让出事件循环），避免集中查时
  饿死上游取时和网页请求。
- 答复超时 `RELAY_REPLY_TIMEOUT`（默认 1.5s）：**超时答复直接丢弃**，不校时钟、也不延长同步窗口。
- **同步判定**：尚未同步，或距最后一次有效答复已满 5 秒 → 回包 LI=3（未同步）、stratum=16；否则 LI=0、stratum=2。
- **断网续走**：失去有效答复后按最后一次同步的速率继续走时。
- **不倒退**：首次对时后，即使上游回拨或重连，中继时钟只以正常速度的 90%~110% 缓调，直至追上上游，绝不回拨。
- 值班网页可：调上游钟差（负数即回拨）、调回包延迟、断开上游、向中继发 UDP 试查。

## 网页 / API

- `GET /` 值班网页
- `GET /api/status` 中继状态（时刻、同步、速率、缓调剩余、最后有效答复年龄、
  报文统计、最近一次下游 T3−T2、上游配置）
- `POST /api/upstream` 调上游 `{"offset_seconds": -30, "delay_ms": 2000, "disconnected": false}`
- `POST /api/query` 网页后端经 UDP 试查中继。返回：
  - `server_time(_str)`：T3（发包时刻）
  - `enqueue_time(_str)`：T2（入队时刻）
  - `queue_ms`：带内 T3−T2
  - `rtt_ms`：实测往返
  - `adjusted_rtt_ms`：扣除 T3−T2 后的往返估计
  - `offset_est_ms`：客户端钟差估算（NTP 四点法）
  - `li` / `stratum` / `synchronized` / `kernel_timestamp`

## 自测

```bash
# 离线用例（无需启动服务）
python3 tests/test_clock.py

# 端到端（需 docker compose up 已启动）
python3 tests/selftest.py

# 连同排队 / 跨切速用例一起跑（仅 Linux 同机，需中继进程号）
RELAY_PID=$(docker inspect -f '{{.State.Pid}}' ntp-relay) python3 tests/selftest.py
```

排队用例通过 `SIGSTOP/SIGCONT` 冻结中继进程，让请求真实地停在内核接收队列里；
跨切速用例先让上游回拨触发 90% 缓调，再在缓调期间制造排队，验证 T3−T2 ≈
0.9 × 排队时长。未设置 `RELAY_PID` 时这些用例标记为 SKIP。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `RELAY_POLL_INTERVAL` | 1.0 | 中继取时周期（秒） |
| `RELAY_REPLY_TIMEOUT` | 1.5 | 上游答复超时（秒），超时答复丢弃 |
| `RELAY_QUEUE_MAX_AGE` | 1.0 | 入队超过该时长（秒）的请求丢弃 |
| `RELAY_UDP_RCVBUF` | 1048576 | 下游 UDP 接收缓冲（字节），集中查时减少核内丢包 |
| `RELAY_UDP_PORT` | 123（容器） | 中继对下游授时端口 |
| `UPSTREAM_HOST/PORT` | upstream/123 | 上游地址 |
| `UPSTREAM_API` | http://upstream:8001 | 上游控制面 |
| `WEB_PORT` | 8000 | 值班网页端口 |
