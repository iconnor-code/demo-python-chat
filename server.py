"""TCP 聊天服务器：每连接一线程 + 锁保护的会话表。

用法: python3 server.py [--host 0.0.0.0] [--port 4000]
"""

import argparse
import logging
import socket
import threading
import time

from protocol import FrameDecoder, MsgType, ProtocolError, recv_frame, send_frame

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO, datefmt="%H:%M:%S")
log = logging.getLogger("chat-server")

USERNAME_MAX_LEN = 32


class ClientSession:
    """一个客户端连接的完整生命周期：登录 → 消息循环 → 清理。"""

    def __init__(self, server: "ChatServer", conn: socket.socket, addr: tuple) -> None:
        self.server = server
        self.conn = conn
        self.addr = addr
        self.username: str | None = None
        self.decoder = FrameDecoder()
        # 每连接独立发送锁：广播来自任意线程，避免同一 socket 上多帧交错写出
        self.send_lock = threading.Lock()

    def run(self) -> None:
        try:
            if self._handle_login():
                self._serve_loop()
        except ProtocolError as e:
            log.info("%s 协议错误，断开: %s", self.addr, e)
        except OSError as e:
            log.info("%s 网络错误，断开: %s", self.addr, e)
        finally:
            self._cleanup()

    # ---- 登录阶段 ----

    def _handle_login(self) -> bool:
        """循环处理 LOGIN 直到成功；登录失败（重名/非法用户名）允许同连接重试。"""
        while True:
            msg_type, payload = recv_frame(self.conn, self.decoder)
            if msg_type != MsgType.LOGIN:
                self.deliver(MsgType.ERROR, {"code": 1, "message": "第一条消息必须是 LOGIN"})
                return False
            username = str(payload.get("username", "")).strip()
            error = self._validate_username(username) or self.server.register(self, username)
            if error:
                self.deliver(MsgType.LOGIN_ACK, {"ok": False, "message": error, "online": []})
                continue
            break

        self.username = username
        self.deliver(MsgType.LOGIN_ACK, {
            "ok": True,
            "message": f"欢迎 {username}",
            "online": self.server.usernames(),
        })
        self.server.broadcast_system(f"{username} 加入了聊天", exclude=self)
        log.info("%s 登录为 %s", self.addr, username)
        return True

    @staticmethod
    def _validate_username(username: str) -> str | None:
        if not username:
            return "用户名不能为空"
        if len(username) > USERNAME_MAX_LEN:
            return f"用户名最长 {USERNAME_MAX_LEN} 个字符"
        if any(c.isspace() for c in username):
            return "用户名不能包含空白字符"
        return None

    # ---- 消息循环 ----

    def _serve_loop(self) -> None:
        handlers = {
            MsgType.CHAT: self._on_chat,
            MsgType.PRIVATE: self._on_private,
            MsgType.LIST_USERS: self._on_list_users,
            MsgType.PING: self._on_ping,
        }
        while True:
            msg_type, payload = recv_frame(self.conn, self.decoder)
            handler = handlers.get(msg_type)
            if handler is None:
                self.deliver(MsgType.ERROR, {"code": 2, "message": f"意外的消息类型 {msg_type.name}"})
                continue
            handler(payload)

    def _on_chat(self, payload: dict) -> None:
        content = str(payload.get("content", "")).strip()
        if not content:
            self.deliver(MsgType.ERROR, {"code": 3, "message": "消息内容不能为空"})
            return
        # 群聊广播给所有人（含发送者，客户端统一渲染）
        self.server.broadcast(MsgType.CHAT_MSG, {"from": self.username, "content": content, "ts": time.time()})

    def _on_private(self, payload: dict) -> None:
        to = str(payload.get("to", "")).strip()
        content = str(payload.get("content", "")).strip()
        if not to or not content:
            self.deliver(MsgType.ERROR, {"code": 3, "message": "私聊需要目标用户和内容"})
            return
        if to == self.username:
            self.deliver(MsgType.ERROR, {"code": 4, "message": "不能给自己发私聊"})
            return
        target = self.server.get_session(to)
        if target is None:
            self.deliver(MsgType.ERROR, {"code": 4, "message": f"用户 {to} 不在线"})
            return
        target.deliver(MsgType.PRIVATE_MSG, {"from": self.username, "content": content, "ts": time.time()})

    def _on_list_users(self, payload: dict) -> None:
        self.deliver(MsgType.USER_LIST, {"users": self.server.usernames()})

    def _on_ping(self, payload: dict) -> None:
        self.deliver(MsgType.PONG, {})

    # ---- 发送与清理 ----

    def deliver(self, msg_type: MsgType, payload: dict) -> None:
        """线程安全地向该连接发送一帧（调用方可能是任意广播线程）。"""
        with self.send_lock:
            send_frame(self.conn, msg_type, payload)

    def _cleanup(self) -> None:
        try:
            self.conn.close()
        except OSError:
            pass
        self.server.unregister(self)
        if self.username:
            self.server.broadcast_system(f"{self.username} 离开了聊天")
            log.info("%s (%s) 断开", self.addr, self.username)
        else:
            log.info("%s 断开（未登录）", self.addr)


class ChatServer:
    """监听线程 + 会话表；会话表的所有读写都在 self.lock 保护下。"""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.sessions: dict[str, ClientSession] = {}
        self.lock = threading.Lock()

    # ---- 会话表操作 ----

    def register(self, session: ClientSession, username: str) -> str | None:
        """把用户名绑定到会话；重名返回错误消息。"""
        with self.lock:
            if username in self.sessions:
                return f"用户名 {username} 已被使用"
            self.sessions[username] = session
            return None

    def unregister(self, session: ClientSession) -> None:
        with self.lock:
            if session.username is not None and self.sessions.get(session.username) is session:
                del self.sessions[session.username]

    def usernames(self) -> list[str]:
        with self.lock:
            return sorted(self.sessions)

    def get_session(self, username: str) -> ClientSession | None:
        with self.lock:
            return self.sessions.get(username)

    def _snapshot(self, exclude: ClientSession | None = None) -> list[ClientSession]:
        with self.lock:
            return [s for s in self.sessions.values() if s is not exclude]

    # ---- 广播 ----

    def broadcast(self, msg_type: MsgType, payload: dict, exclude: ClientSession | None = None) -> None:
        for session in self._snapshot(exclude):
            try:
                session.deliver(msg_type, payload)
            except (OSError, ProtocolError):
                pass  # 目标连接已坏，由它的读线程负责清理

    def broadcast_system(self, content: str, exclude: ClientSession | None = None) -> None:
        self.broadcast(MsgType.SYSTEM, {"content": content, "ts": time.time()}, exclude=exclude)

    # ---- 主循环 ----

    def serve_forever(self) -> None:
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen()
        log.info("聊天服务器已启动 %s:%d（Ctrl+C 停止）", self.host, self.port)
        try:
            while True:
                conn, addr = srv.accept()
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                log.info("新连接 %s", addr)
                threading.Thread(target=ClientSession(self, conn, addr).run, daemon=True).start()
        except KeyboardInterrupt:
            log.info("收到 Ctrl+C，正在关闭...")
        finally:
            srv.close()
            for session in self._snapshot():
                try:
                    session.conn.shutdown(socket.SHUT_RDWR)
                    session.conn.close()
                except OSError:
                    pass


def main() -> None:
    parser = argparse.ArgumentParser(description="TCP 聊天服务器")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址（默认 0.0.0.0）")
    parser.add_argument("--port", type=int, default=4000, help="监听端口（默认 4000）")
    args = parser.parse_args()
    ChatServer(args.host, args.port).serve_forever()


if __name__ == "__main__":
    main()
