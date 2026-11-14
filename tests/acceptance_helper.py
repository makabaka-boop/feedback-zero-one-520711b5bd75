#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验收脚本共享辅助：在临时目录拉起独立 server 进程并通过 HTTP 交互。"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(os.path.dirname(HERE), "server.py")


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self):
        self.tmpdir = tempfile.mkdtemp(prefix="chain_accept_")
        self.db = os.path.join(self.tmpdir, "warehouse.db")
        self.port = free_port()
        self.base = "http://127.0.0.1:%d" % self.port
        self.proc = None

    def start(self, extra_env=None):
        env = dict(os.environ)
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        if extra_env:
            env.update(extra_env)
        self.proc = subprocess.Popen(
            [sys.executable, "-I", SERVER, self.db,
             "127.0.0.1", str(self.port)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env,
        )
        deadline = time.time() + 10
        while time.time() < deadline:
            try:
                self.get("/snapshot")
                return
            except Exception:
                if self.proc.poll() is not None:
                    out, err = self.proc.communicate()
                    raise RuntimeError("server 提前退出:\n" + err.decode())
                time.sleep(0.05)
        raise RuntimeError("server 启动超时")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    def restart(self):
        self.stop()
        self.start()

    def _request(self, method, path, payload=None, raw=None):
        if raw is not None:
            data = raw if isinstance(raw, bytes) else raw.encode("utf-8")
        else:
            data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode("utf-8"))

    def post(self, path, payload):
        return self._request("POST", path, payload=payload)

    def post_raw(self, path, raw):
        return self._request("POST", path, raw=raw)

    def get(self, path):
        req = urllib.request.Request(self.base + path, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))

    def block_status(self, block_id):
        import sqlite3
        conn = sqlite3.connect(self.db)
        try:
            row = conn.execute(
                "SELECT status FROM blocks WHERE id=?", (block_id,)
            ).fetchone()
            return row[0] if row else None
        finally:
            conn.close()


_passed = 0


def check(cond, label):
    if not cond:
        raise AssertionError("断言失败: " + label)
    global _passed
    _passed += 1
    print("  ok -", label)


def expect_bad(status, body, path_part, label):
    check(status == 400 and body.get("path", "").startswith(path_part),
          "%s（400 且路径前缀 %s，实际: %s %s）"
          % (label, path_part, status, body))


def finish(name):
    print("%s 验收通过（%d 项检查）" % (name, _passed))
