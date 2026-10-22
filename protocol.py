"""自定义 TCP 通信协议（server 与 client 共用）。

帧格式（二进制头 + JSON 体）：

    +---------+---------+------+--------------+------------------------+
    | MAGIC   | VERSION | TYPE | LENGTH       | PAYLOAD                |
    | 2 字节  | 1 字节  | 1字节 | 4 字节 大端  | LENGTH 字节 UTF-8 JSON |
    +---------+---------+------+--------------+------------------------+

MAGIC 用于校验帧同步，VERSION 预留协议升级空间，LENGTH 前缀用于在
TCP 字节流中切分消息边界（配合 FrameDecoder 解决粘包/半包问题）。
"""

import json
import socket
import struct
from enum import IntEnum

MAGIC = b"PC"
VERSION = 1
HEADER_SIZE = 8                 # MAGIC(2) + VERSION(1) + TYPE(1) + LENGTH(4)
MAX_PAYLOAD_SIZE = 64 * 1024    # 超过该大小的帧视为非法，直接断开连接


class ProtocolError(Exception):
    """协议违规（坏帧、超限、未知类型）或连接中断。"""


class MsgType(IntEnum):
    # ---- 客户端 → 服务端 ----
    LOGIN = 1          # {"username": str}
    CHAT = 2           # {"content": str}
    PRIVATE = 3        # {"to": str, "content": str}
    LIST_USERS = 4     # {}
    PING = 5           # {}

    # ---- 服务端 → 客户端 ----
    PONG = 7           # {}
    LOGIN_ACK = 102    # {"ok": bool, "message": str, "online": [str]}
    CHAT_MSG = 103     # {"from": str, "content": str, "ts": float}
    PRIVATE_MSG = 104  # {"from": str, "content": str, "ts": float}
    USER_LIST = 105    # {"users": [str]}
    SYSTEM = 106       # {"content": str, "ts": float}   上线/下线通知
    ERROR = 107        # {"code": int, "message": str}


def encode_frame(msg_type: MsgType, payload: dict | None = None) -> bytes:
    """将 (类型, JSON 负载) 编码为一帧完整字节。"""
    data = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(data) > MAX_PAYLOAD_SIZE:
        raise ProtocolError(f"payload 过大: {len(data)} 字节（上限 {MAX_PAYLOAD_SIZE}）")
    header = struct.pack("!2sBBI", MAGIC, VERSION, int(msg_type), len(data))
    return header + data


def send_frame(sock: socket.socket, msg_type: MsgType, payload: dict | None = None) -> None:
    """编码并通过 sendall 完整发送一帧（sendall 保证不出现半帧写出）。"""
    sock.sendall(encode_frame(msg_type, payload))


class FrameDecoder:
    """流式拆包器：feed() 喂入任意长度的原始字节，next_frame() 依序取出完整帧。

    一个连接对应一个实例，可正确处理 TCP 粘包（一次收到多帧）与
    半包（一帧被拆成多次到达）。
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf.extend(data)

    def next_frame(self) -> tuple[MsgType, dict] | None:
        """返回下一帧 (类型, 负载)；缓冲区中没有完整帧时返回 None。"""
        if len(self._buf) < HEADER_SIZE:
            return None
        magic, version, type_id, length = struct.unpack_from("!2sBBI", self._buf)
        if magic != MAGIC:
            raise ProtocolError(f"帧头 MAGIC 不匹配: {magic!r}")
        if version != VERSION:
            raise ProtocolError(f"不支持的协议版本: {version}")
        if length > MAX_PAYLOAD_SIZE:
            raise ProtocolError(f"payload 长度超限: {length}")
        if len(self._buf) < HEADER_SIZE + length:
            return None  # 半包，等待更多字节

        frame = bytes(self._buf[:HEADER_SIZE + length])
        del self._buf[:HEADER_SIZE + length]
        try:
            payload = json.loads(frame[HEADER_SIZE:].decode("utf-8")) if length else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ProtocolError(f"payload JSON 解析失败: {e}") from e
        try:
            msg_type = MsgType(type_id)
        except ValueError:
            raise ProtocolError(f"未知消息类型: {type_id}") from None
        if not isinstance(payload, dict):
            raise ProtocolError("payload 必须是 JSON 对象")
        return msg_type, payload


def recv_frame(sock: socket.socket, decoder: FrameDecoder) -> tuple[MsgType, dict]:
    """从阻塞 socket 读出完整一帧；连接关闭时抛 ProtocolError。

    decoder 必须与该连接绑定且由同一线程使用（内部缓冲可能残留
    已到达但尚未取出的后续帧）。
    """
    while True:
        frame = decoder.next_frame()
        if frame is not None:
            return frame
        chunk = sock.recv(4096)
        if not chunk:
            raise ProtocolError("连接已关闭")
        decoder.feed(chunk)
