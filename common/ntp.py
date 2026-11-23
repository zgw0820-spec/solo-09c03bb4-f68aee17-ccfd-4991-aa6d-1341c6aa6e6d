"""极简 NTPv4 报文工具（RFC 5905，48 字节固定头）。"""
import struct

# 1900-01-01 与 1970-01-01 之间的秒数
NTP_EPOCH_OFFSET = 2208988800

# li_vn_mode, stratum, poll, precision, root_delay, root_dispersion,
# ref_id(4s), ref_ts(8s), origin_ts(8s), recv_ts(8s), xmit_ts(8s)  = 48 字节
PACKET = struct.Struct("!BBBbII4s8s8s8s8s")

MODE_CLIENT = 3
MODE_SERVER = 4
VERSION = 4

LI_OK = 0        # 已同步
LI_ALARM = 3     # 未同步（alarm）


def li_vn_mode(li: int, version: int, mode: int) -> int:
    return (li << 6) | (version << 3) | mode


def unix_to_ntp(ts: float) -> bytes:
    """unix 秒 -> 8 字节 NTP 时间戳（32.32 定点）。"""
    if ts is None or ts <= 0:
        return b"\x00" * 8
    ntp = ts + NTP_EPOCH_OFFSET
    sec = int(ntp)
    frac = int((ntp - sec) * (1 << 32)) & 0xFFFFFFFF
    return struct.pack("!II", sec, frac)


def ntp_to_unix(data: bytes) -> float:
    sec, frac = struct.unpack("!II", data)
    if sec == 0 and frac == 0:
        return 0.0
    return (sec - NTP_EPOCH_OFFSET) + frac / (1 << 32)


def build_request(send_unix: float) -> bytes:
    """构造客户端请求（mode 3），发送时刻写入 transmit 字段。"""
    return PACKET.pack(
        li_vn_mode(LI_OK, VERSION, MODE_CLIENT),
        0, 0, 0,          # stratum, poll, precision
        0, 0,             # root delay / dispersion
        b"\x00" * 4,      # ref id
        b"\x00" * 8,      # reference ts
        b"\x00" * 8,      # origin ts
        b"\x00" * 8,      # receive ts
        unix_to_ntp(send_unix),  # transmit ts
    )


def build_response(li: int, stratum: int, origin_ts: bytes,
                   recv_unix: float, xmit_unix: float,
                   ref_id: bytes = b"RELY") -> bytes:
    """构造服务端应答（mode 4），origin 回显客户端的 transmit。"""
    return PACKET.pack(
        li_vn_mode(li, VERSION, MODE_SERVER),
        stratum, 0, -20,  # stratum, poll, precision(~1us)
        0, 0,
        ref_id,
        unix_to_ntp(recv_unix),   # reference ts（近似取接收时刻）
        origin_ts,                # origin ts = 客户端 transmit
        unix_to_ntp(recv_unix),   # receive ts
        unix_to_ntp(xmit_unix),   # transmit ts
    )


def parse(data: bytes) -> dict:
    """解析 48 字节 NTP 报文。"""
    (flags, stratum, _poll, _prec, _rd, _rdisp,
     ref_id, _ref, origin, recv, xmit) = PACKET.unpack(data[:48])
    return {
        "li": flags >> 6,
        "version": (flags >> 3) & 0x7,
        "mode": flags & 0x7,
        "stratum": stratum,
        "ref_id": ref_id,
        "origin_raw": origin,
        "origin": ntp_to_unix(origin),
        "recv": ntp_to_unix(recv),
        "xmit": ntp_to_unix(xmit),
    }
