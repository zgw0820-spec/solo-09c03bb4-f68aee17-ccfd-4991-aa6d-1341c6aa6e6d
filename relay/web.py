"""值班网页：查看中继状态、调上游钟差/回包延迟/断开上游、向中继发 UDP 试查。"""
import asyncio
import os
import socket
import time
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from common import ntp

UPSTREAM_API = os.getenv("UPSTREAM_API", "http://127.0.0.1:8001")
RELAY_UDP_PORT = int(os.getenv("RELAY_UDP_PORT", "10123"))

PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>NTP 中继值班台</title>
<style>
  body { font-family: system-ui, "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 24px; background:#0f1420; color:#e8ecf4; }
  h1 { font-size: 20px; } h2 { font-size: 16px; color:#9fb3d1; margin:0 0 10px; }
  section { background:#171e2e; border:1px solid #2a3550; border-radius:10px;
            padding:16px 20px; margin-bottom:16px; }
  #clock { font-size: 34px; font-family: ui-monospace, Consolas, monospace; }
  .badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:13px; }
  .ok { background:#14532d; color:#86efac; } .bad { background:#7f1d1d; color:#fca5a5; }
  label { margin-right:16px; } input[type=number]{ width:110px; }
  button { padding:6px 16px; border-radius:6px; border:1px solid #3b82f6;
           background:#1d4ed8; color:#fff; cursor:pointer; }
  pre { background:#0b1020; padding:12px; border-radius:8px; font-size:14px; }
  .row { margin:8px 0; } .k{ color:#8ea2c0; display:inline-block; min-width:190px; }
</style>
</head>
<body>
<h1>机房 NTP 中继值班台</h1>

<section>
  <h2>中继状态</h2>
  <div id="clock">--</div>
  <div class="row"><span class="k">同步状态</span><span id="sync" class="badge bad">--</span></div>
  <div class="row"><span class="k">走时速度</span><span id="rate">--</span></div>
  <div class="row"><span class="k">距最后一次有效上游答复</span><span id="age">--</span></div>
  <div class="row"><span class="k">报文统计</span><span id="stats">--</span></div>
</section>

<section>
  <h2>上游时间源控制</h2>
  <div class="row">
    <label>钟差（秒，负数为回拨）<input id="offset" type="number" step="0.5" value="0"></label>
    <label>回包延迟（毫秒）<input id="delay" type="number" step="50" min="0" value="0"></label>
    <label><input id="disc" type="checkbox"> 断开上游（丢包）</label>
    <button id="apply">应用</button>
  </div>
  <div class="row"><span class="k">上游当前配置</span><span id="upcfg">--</span></div>
  <div class="row"><span class="k">上游有效时刻</span><span id="uptime">--</span></div>
</section>

<section>
  <h2>向下游试查（网页后端经 UDP 查询中继）</h2>
  <div class="row">
    <button id="qbtn">发 UDP 试查</button>
    <label><input id="autoq" type="checkbox"> 每秒自动试查</label>
  </div>
  <pre id="qresult">尚未试查</pre>
</section>

<script>
const $ = id => document.getElementById(id);
function fmt(ts){
  if (ts === null || ts === undefined) return '--';
  const ms = Math.round(ts * 1000);
  const d = new Date(ms);
  const p = (n,w)=>String(n).padStart(w,'0');
  return `${d.getFullYear()}-${p(d.getMonth()+1,2)}-${p(d.getDate(),2)} ` +
         `${p(d.getHours(),2)}:${p(d.getMinutes(),2)}:${p(d.getSeconds(),2)}.${p(ms%1000,3)}`;
}
async function api(url, method, body){
  const r = await fetch(url, {method: method||'GET',
    headers:{'Content-Type':'application/json'},
    body: body ? JSON.stringify(body) : undefined});
  return r.json();
}
let lastTime = null, lastAt = 0, rate = 1;
async function refresh(){
  try {
    const s = await api('/api/status');
    lastTime = s.time; lastAt = performance.now(); rate = s.rate || 1;
    const el = $('sync');
    if (s.synchronized) { el.textContent = '同步'; el.className = 'badge ok'; }
    else {
      el.textContent = s.synced_once ? '未同步（按最后同步继续走时）' : '未同步（从未对时）';
      el.className = 'badge bad';
    }
    $('rate').textContent = (s.rate*100).toFixed(0) + ' %';
    $('age').textContent = s.last_valid_age === null ? '从未收到' : s.last_valid_age.toFixed(1) + ' 秒';
    const st = s.stats;
    $('stats').textContent = `发出 ${st.sent} / 有效 ${st.valid} / 超时丢弃 ${st.late} / 未知 ${st.unknown} / 下游查询 ${s.downstream_queries}`;
    const u = s.upstream || {};
    if (u.error) { $('upcfg').textContent = '控制不可达: ' + u.error; $('uptime').textContent = '--'; }
    else {
      $('upcfg').textContent = `钟差 ${u.offset_seconds} s / 延迟 ${u.delay_ms} ms / ${u.disconnected ? '已断开' : '已连接'}`;
      $('uptime').textContent = fmt(u.effective_time);
    }
  } catch(e) { /* 忽略瞬时错误 */ }
}
setInterval(()=>{ $('clock').textContent =
  lastTime === null ? '--' : fmt(lastTime + (performance.now()-lastAt)/1000*rate); }, 100);
setInterval(refresh, 500); refresh();

$('apply').onclick = async () => {
  const body = { offset_seconds: parseFloat($('offset').value || '0'),
                 delay_ms: parseFloat($('delay').value || '0'),
                 disconnected: $('disc').checked };
  const r = await api('/api/upstream', 'POST', body);
  $('upcfg').textContent = `已应用 → 钟差 ${r.offset_seconds} s / 延迟 ${r.delay_ms} ms / ${r.disconnected ? '已断开' : '已连接'}`;
};
async function doQuery(){
  const r = await api('/api/query', 'POST', {});
  if (!r.ok) { $('qresult').textContent = '试查失败: ' + (r.error || ''); return; }
  $('qresult').textContent =
    `授时时刻 T3 : ${r.server_time_str}\\n` +
    `同步状态    : ${r.synchronized ? '同步 (LI=0)' : '未同步 (LI=3)'}\\n` +
    `层级 stratum: ${r.stratum}\\n` +
    `往返时延    : ${r.rtt_ms} ms`;
}
$('qbtn').onclick = doQuery;
let timer = null;
$('autoq').onchange = e => {
  if (e.target.checked) { doQuery(); timer = setInterval(doQuery, 1000); }
  else if (timer) { clearInterval(timer); timer = null; }
};
</script>
</body>
</html>
"""


def fmt_ts(ts):
    """unix 秒 -> 本地时间字符串，精确到毫秒。"""
    if ts is None:
        return None
    ms = round(ts * 1000)
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone()
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f".{ms % 1000:03d}"


async def ntp_query(port: int, timeout: float = 2.0) -> dict:
    """网页后端作为下游客户端，经 UDP 向中继发一次 NTP 查询。"""
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try:
        await loop.sock_connect(sock, ("127.0.0.1", port))
        t1 = time.time()
        await loop.sock_sendall(sock, ntp.build_request(t1))
        data = await asyncio.wait_for(loop.sock_recv(sock, 512), timeout)
        t4 = time.time()
        msg = ntp.parse(data)
        synced = msg["li"] != ntp.LI_ALARM
        return {
            "ok": True,
            "server_time": msg["xmit"],
            "server_time_str": fmt_ts(msg["xmit"]),
            "li": msg["li"],
            "synchronized": synced,
            "stratum": msg["stratum"],
            "rtt_ms": round((t4 - t1) * 1000, 2),
        }
    except asyncio.TimeoutError:
        return {"ok": False, "error": "查询超时"}
    finally:
        sock.close()


class UpstreamConfig(BaseModel):
    offset_seconds: float | None = None
    delay_ms: float | None = None
    disconnected: bool | None = None


def create_app(clock, stats, server) -> FastAPI:
    app = FastAPI(title="NTP 中继值班台")

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return PAGE

    @app.get("/api/status")
    async def status():
        st = clock.status()
        st["time_str"] = fmt_ts(st["time"])
        st["stats"] = vars(stats)
        st["downstream_queries"] = server.queries
        try:
            async with httpx.AsyncClient(timeout=1.5) as c:
                r = await c.get(f"{UPSTREAM_API}/api/config")
                st["upstream"] = r.json()
        except Exception as e:  # 上游控制面不可达不影响状态展示
            st["upstream"] = {"error": str(e)}
        return st

    @app.post("/api/upstream")
    async def set_upstream(cfg: UpstreamConfig):
        payload = {k: v for k, v in cfg.model_dump().items() if v is not None}
        async with httpx.AsyncClient(timeout=2.0) as c:
            r = await c.post(f"{UPSTREAM_API}/api/config", json=payload)
            return r.json()

    @app.post("/api/query")
    async def query():
        return await ntp_query(RELAY_UDP_PORT)

    return app
