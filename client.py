"""TCP 聊天客户端（终端 CLI）。

用法: python3 client.py [--host 127.0.0.1] [--port 4000] [--username alice]
"""

import argparse
import socket
import sys
import threading
import time

from protocol import FrameDecoder, MsgType, ProtocolError, recv_frame, send_frame

PROMPT = "> "
PING_INTERVAL = 15.0


class ChatClient:
    def __init__(self, host: str, port: int, username: str | None) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.sock: socket.socket | None = None
        self.decoder = FrameDecoder()          # 整个连接生命周期共用一个拆包器
        self.running = threading.Event()       # set 表示运行中
        self.reading = threading.Event()       # 主线程是否正阻塞在 input() 上
        self.print_lock = threading.Lock()

    # ---- 登录 ----

    def connect_and_login(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        self.sock.settimeout(None)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        while True:
            username = self.username or input("请输入用户名: ").strip()
            self.username = None  # 之后的每次重试都重新询问
            if not username:
                print("用户名不能为空，请重试")
                continue
            send_frame(self.sock, MsgType.LOGIN, {"username": username})
            msg_type, payload = recv_frame(self.sock, self.decoder)
            if msg_type == MsgType.LOGIN_ACK and payload.get("ok"):
                self.username = username
                online = payload.get("online", []) or []
                others = ", ".join(u for u in online if u != username) or "无其他人"
                print(f"已登录为 {username}，当前在线: {others}")
                return
            print(f"登录失败: {payload.get('message', '未知原因')}，请重试")

    # ---- 主流程 ----

    def run(self) -> int:
        try:
            self.connect_and_login()
        except EOFError:
            print("已退出（未登录）")
            return 0
        except (ProtocolError, OSError) as e:
            print(f"连接或登录失败: {e}")
            return 1

        self.running.set()
        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._keepalive_loop, daemon=True).start()
        print("直接输入文本发送群聊；/help 查看命令")
        try:
            self._input_loop()
        except (KeyboardInterrupt, EOFError):
            pass
        finally:
            self._shutdown()
        return 0

    # ---- 输入（主线程） ----

    def _input_loop(self) -> None:
        while self.running.is_set():
            with self.print_lock:
                sys.stdout.write(PROMPT)
                sys.stdout.flush()
            self.reading.set()
            try:
                line = input()
            finally:
                self.reading.clear()
            self._handle_line(line.strip())

    def _handle_line(self, line: str) -> None:
        if not line:
            return
        if line in ("/quit", "/exit"):
            self.running.clear()
            return
        if line == "/users":
            self._send(MsgType.LIST_USERS, {})
            return
        if line == "/help":
            self._print("命令: /msg <用户名> <内容> 或 @用户名 <内容> 私聊 | /users 在线列表 | /quit 退出")
            return
        if line.startswith("/msg"):
            parts = line.split(maxsplit=2)
            if len(parts) < 3:
                self._print("用法: /msg <用户名> <内容>")
                return
            self._send_private(parts[1], parts[2])
            return
        if line.startswith("@"):
            parts = line[1:].split(maxsplit=1)
            if len(parts) != 2:
                self._print("用法: @用户名 <内容>")
                return
            self._send_private(parts[0], parts[1])
            return
        if line.startswith("/"):
            self._print(f"未知命令 {line.split()[0]}，/help 查看帮助")
            return
        self._send(MsgType.CHAT, {"content": line})

    def _send_private(self, to: str, content: str) -> None:
        content = content.strip()
        if not content:
            self._print("私聊内容不能为空")
            return
        self._send(MsgType.PRIVATE, {"to": to, "content": content})
        self._print(f"[私聊→{to}] {content}")  # 本地回显，服务端只投递给对方

    def _send(self, msg_type: MsgType, payload: dict) -> None:
        try:
            send_frame(self.sock, msg_type, payload)
        except ProtocolError:
            self._print("发送失败: 消息过长（超过 64KB）")
        except OSError:
            self._print("发送失败: 与服务器的连接已断开")
            self.running.clear()

    # ---- 接收（守护线程） ----

    def _receive_loop(self) -> None:
        try:
            while True:
                msg_type, payload = recv_frame(self.sock, self.decoder)
                if msg_type == MsgType.CHAT_MSG:
                    self._print(f"[{self._fmt_ts(payload.get('ts'))}] {payload.get('from')}: {payload.get('content')}")
                elif msg_type == MsgType.PRIVATE_MSG:
                    self._print(f"[{self._fmt_ts(payload.get('ts'))}] [私聊] {payload.get('from')} → 你: {payload.get('content')}")
                elif msg_type == MsgType.SYSTEM:
                    self._print(f"[系统] {payload.get('content')}")
                elif msg_type == MsgType.USER_LIST:
                    users = payload.get("users", [])
                    self._print(f"在线用户 ({len(users)}): {', '.join(users)}")
                elif msg_type == MsgType.ERROR:
                    self._print(f"[错误] {payload.get('message')}")
                # PONG 仅用于保活，不展示
        except (ProtocolError, OSError):
            self._print("与服务器的连接已断开")
            self.running.clear()

    # ---- 保活（守护线程） ----

    def _keepalive_loop(self) -> None:
        while self.running.is_set():
            time.sleep(PING_INTERVAL)
            if not self.running.is_set():
                return
            try:
                send_frame(self.sock, MsgType.PING, {})
            except (OSError, ProtocolError):
                self.running.clear()
                return

    # ---- 输出与退出 ----

    @staticmethod
    def _fmt_ts(ts) -> str:
        try:
            return time.strftime("%H:%M:%S", time.localtime(float(ts)))
        except (TypeError, ValueError):
            return time.strftime("%H:%M:%S")

    def _print(self, text: str) -> None:
        """线程安全打印：清掉当前输入行的提示符，打印后重绘，避免与输入交错。"""
        with self.print_lock:
            if self.reading.is_set():
                sys.stdout.write("\r\033[K")  # 回到行首并清至行尾
                sys.stdout.write(text + "\n")
                sys.stdout.write(PROMPT)
            else:
                sys.stdout.write(text + "\n")
            sys.stdout.flush()

    def _shutdown(self) -> None:
        self.running.clear()
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
        print("已退出")


def main() -> None:
    parser = argparse.ArgumentParser(description="TCP 聊天客户端")
    parser.add_argument("--host", default="127.0.0.1", help="服务器地址（默认 127.0.0.1）")
    parser.add_argument("--port", type=int, default=4000, help="服务器端口（默认 4000）")
    parser.add_argument("--username", default=None, help="用户名（不给则登录时询问）")
    args = parser.parse_args()

    client = ChatClient(args.host, args.port, args.username)
    try:
        sys.exit(client.run())
    except KeyboardInterrupt:
        print("\n已退出")
        sys.exit(0)


if __name__ == "__main__":
    main()
