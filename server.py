#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""链上研究数仓后端：收块、快照、续取（含分叉撤回）。

仅使用 Python 标准库（http.server / sqlite3 / json / threading 等）。
所有持久化状态集中在一个 SQLite 文件内，每次收块在单事务内原子完成：
块、链状态、tip 未花费集、版本批次要么全部生效，要么全部回滚。
"""

import json
import os
import sqlite3
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

GENESIS = "G"

# 块的三种链上状态
STATUS_PENDING = "pending"   # 父链不齐，待定
STATUS_VALID = "valid"       # 父链齐全且逐交易解释通过
STATUS_INVALID = "invalid"   # 父链齐全但语义无效（重复花费/越界/超额），后代连带无效


class ValidationError(Exception):
    """请求不合法（400）。path 为 JSON 路径，$ 表示根/语法层。"""

    def __init__(self, path, message):
        super().__init__(message)
        self.path = path
        self.message = message


class Engine:
    """封装 SQLite 连接与全部业务逻辑；进程内串行锁保证并发等价。"""

    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    # ---------- 初始化 ----------

    def _init_schema(self):
        with self.lock:
            c = self.conn
            c.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta(
                    key   TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                -- 块：id 即内容主键；data 为完整块 JSON；status 为链上状态
                CREATE TABLE IF NOT EXISTS blocks(
                    id     TEXT PRIMARY KEY,
                    parent TEXT NOT NULL,
                    work   INTEGER NOT NULL,
                    data   TEXT NOT NULL,
                    status TEXT NOT NULL
                );
                -- 版本批次（仅 tip 变化时写一行，version 即新版本号）
                CREATE TABLE IF NOT EXISTS batches(
                    version INTEGER PRIMARY KEY,
                    removed TEXT NOT NULL,
                    added   TEXT NOT NULL
                );
                -- 当前 tip 的未花费集（每次结算随事务整体重写）
                CREATE TABLE IF NOT EXISTS utxos(
                    ref_block TEXT NOT NULL,
                    ref_tx    INTEGER NOT NULL,
                    ref_out   INTEGER NOT NULL,
                    amount    INTEGER NOT NULL,
                    PRIMARY KEY(ref_block, ref_tx, ref_out)
                );
                """
            )
            row = c.execute(
                "SELECT value FROM meta WHERE key='version'"
            ).fetchone()
            if row is None:
                # 空创世块：无父（用空串表示）、权重 0、无交易
                genesis_block = {"id": GENESIS, "parent": "",
                                 "work": 0, "txs": []}
                c.execute(
                    "INSERT INTO blocks(id,parent,work,data,status) "
                    "VALUES(?,?,?,?,?)",
                    (GENESIS, "", 0,
                     json.dumps(genesis_block, separators=(",", ":")),
                     STATUS_VALID),
                )
                c.execute("INSERT INTO meta(key,value) VALUES('version','0')")
                c.execute("INSERT INTO meta(key,value) VALUES('tip',?)",
                          (GENESIS,))
            c.commit()

    # ---------- 收块主流程 ----------

    def ingest(self, block):
        """收一个块，返回收块后的快照（事务内读取，保证一致）。"""
        with self.lock:
            block_id = block["id"]
            parent = block["parent"]

            existing = self.conn.execute(
                "SELECT data FROM blocks WHERE id=?", (block_id,)
            ).fetchone()
            if existing is not None:
                # 同 id 同结构重传：幂等，状态不变；异内容在请求层已拒绝，
                # 这里再兜底比对一次。
                if existing[0] != json.dumps(block, separators=(",", ":")):
                    raise ValidationError("$.id", "同 id 块内容不一致")
                return self._snapshot_locked()

            if self._forms_cycle(block_id, parent):
                # 含占位父的环（父沿 parent 链最终指回自身）直接拒绝，不保存
                raise ValidationError(
                    "$.parent", "父链成环（含占位父引用），拒绝该块"
                )

            try:
                self.conn.execute("BEGIN IMMEDIATE")
                self.conn.execute(
                    "INSERT INTO blocks(id,parent,work,data,status) "
                    "VALUES(?,?,?,?,?)",
                    (block_id, parent, block["work"],
                     json.dumps(block, separators=(",", ":")),
                     STATUS_PENDING),
                )

                blocks = {
                    r[0]: {"id": r[0], "parent": r[1], "work": r[2],
                           "txs": json.loads(r[3])["txs"]}
                    for r in self.conn.execute(
                        "SELECT id,parent,work,data FROM blocks"
                    )
                }

                memo = {}       # id -> (status, utxo dict)
                affected = {block_id}
                affected.update(self._descendants_locked(block_id))

                # 级联判定：新块及其全部后代逐个解释
                for bid in affected:
                    self._evaluate(bid, blocks, memo, frozenset())

                for bid in affected:
                    status = memo[bid][0]
                    self.conn.execute(
                        "UPDATE blocks SET status=? WHERE id=?",
                        (status, bid),
                    )

                # 在有效块中选累计 work 最大者为 tip，平局取 ASCII 最小 id。
                # 先对全量块求值（记忆化，未受影响的块只会补算一次），
                # 保证 memo 覆盖包括 G 在内的所有块。
                for bid in blocks:
                    self._evaluate(bid, blocks, memo, frozenset())

                new_tip = GENESIS
                best = self._cumwork(GENESIS, blocks)
                for bid in blocks:
                    if memo[bid][0] != STATUS_VALID:
                        continue
                    cw = self._cumwork(bid, blocks)
                    if cw > best or (cw == best and bid < new_tip):
                        best = cw
                        new_tip = bid

                old_tip = self._meta_locked("tip")
                if new_tip != old_tip:
                    # 求两条 tip 链的共同祖先，各自独有段天然保持根->叶顺序
                    old_chain = self._chain_to_root(old_tip, blocks)
                    new_chain = self._chain_to_root(new_tip, blocks)
                    old_set = set(old_chain)
                    new_set = set(new_chain)

                    # 新链独有块：根至叶加入（chain 本身是 tip->根，反转即可）
                    added = [b for b in reversed(new_chain)
                             if b not in old_set]
                    # 旧链独有块：叶至根撤回（chain 本身就是 tip->根）
                    removed = [b for b in old_chain if b not in new_set]

                    new_version = int(self._meta_locked("version")) + 1

                    removed_full = [blocks[b] for b in removed]
                    added_full = [blocks[b] for b in added]
                    self.conn.execute(
                        "INSERT INTO batches(version,removed,added) "
                        "VALUES(?,?,?)",
                        (new_version,
                         json.dumps(removed_full, separators=(",", ":")),
                         json.dumps(added_full, separators=(",", ":"))),
                    )

                    # 整体重写 tip 未花费集
                    self.conn.execute("DELETE FROM utxos")
                    utxo_dict = memo[new_tip][1]
                    for (rb, ti, oi), amount in utxo_dict.items():
                        self.conn.execute(
                            "INSERT INTO utxos(ref_block,ref_tx,ref_out,"
                            "amount) VALUES(?,?,?,?)",
                            (rb, ti, oi, amount),
                        )

                    self.conn.execute(
                        "UPDATE meta SET value=? WHERE key='version'",
                        (str(new_version),),
                    )
                    self.conn.execute(
                        "UPDATE meta SET value=? WHERE key='tip'",
                        (new_tip,),
                    )

                snapshot = self._snapshot_locked()
                self.conn.commit()
                return snapshot
            except Exception:
                self.conn.rollback()
                raise

    def _forms_cycle(self, start, parent):
        """沿已保存块的 parent 链判断 start 是否会成环（含占位父）。"""
        cur = parent
        seen = set()
        while cur != start and cur not in seen:
            row = self.conn.execute(
                "SELECT parent FROM blocks WHERE id=?", (cur,)
            ).fetchone()
            if row is None:
                return False        # 父缺失：只是待定，不是环
            seen.add(cur)
            cur = row[0]
        return cur == start

    def _descendants_locked(self, block_id):
        """parent 索引上的全部后代（DFS）。"""
        result = []
        stack = [block_id]
        seen = {block_id}
        while stack:
            cur = stack.pop()
            for r in self.conn.execute(
                "SELECT id FROM blocks WHERE parent=?", (cur,)
            ):
                child = r[0]
                if child not in seen:
                    seen.add(child)
                    result.append(child)
                    stack.append(child)
        return result

    def _evaluate(self, bid, blocks, memo, visiting):
        """递归解释一个块，记忆化返回 (status, utxo 字典)。"""
        if bid in memo:
            return memo[bid]
        if bid in visiting:
            # 理论上插入前已拦环，此处仅作防御
            memo[bid] = (STATUS_INVALID, {})
            return memo[bid]

        block = blocks[bid]
        parent = block["parent"]
        if bid == GENESIS:
            # 递归基例：创世块父为空，本身有效且 UTXO 集为空
            memo[bid] = (STATUS_VALID, {})
            return memo[bid]
        if parent not in blocks:
            memo[bid] = (STATUS_PENDING, None)
            return memo[bid]

        p_status, p_utxo = self._evaluate(
            parent, blocks, memo, visiting | {bid}
        )
        if p_status != STATUS_VALID:
            # 缺父待定 / 父无效则连带无效
            memo[bid] = (
                STATUS_PENDING if p_status == STATUS_PENDING
                else STATUS_INVALID,
                None,
            )
            return memo[bid]

        utxo = dict(p_utxo)
        try:
            for tx_idx, tx in enumerate(block["txs"]):
                inputs, outputs = tx[0], tx[1]
                consumed = set()
                in_total = 0
                for ref in inputs:
                    key = (ref[0], ref[1], ref[2])
                    # 必须是本分支未花费输出（本块较早交易的花费也已移除）
                    if key not in utxo or key in consumed:
                        raise ValueError("重复花费或引用不存在的输出")
                    consumed.add(key)
                    in_total += utxo[key]
                out_total = 0
                for amount in outputs:
                    out_total += amount
                # 空输入允许铸造（不受额度约束）；有输入时输出不得超额
                if inputs and out_total > in_total:
                    raise ValueError("输出总额超过输入总额")
                for key in consumed:
                    del utxo[key]
                for out_idx, amount in enumerate(outputs):
                    utxo[(bid, tx_idx, out_idx)] = amount
        except (ValueError, KeyError, IndexError, TypeError):
            memo[bid] = (STATUS_INVALID, None)
            return memo[bid]

        memo[bid] = (STATUS_VALID, utxo)
        return memo[bid]

    def _cumwork(self, bid, blocks):
        total = 0
        cur = bid
        seen = set()
        while cur in blocks and cur not in seen:
            seen.add(cur)
            total += blocks[cur]["work"]
            cur = blocks[cur]["parent"]
        return total

    @staticmethod
    def _chain_to_root(tip, blocks):
        """从 tip 向根返回 [tip, ..., G]（不缺父时）。"""
        chain = []
        cur = tip
        seen = set()
        while cur in blocks and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            cur = blocks[cur]["parent"]
        return chain

    # ---------- 快照与续取 ----------

    def _meta_locked(self, key):
        return self.conn.execute(
            "SELECT value FROM meta WHERE key=?", (key,)
        ).fetchone()[0]

    def snapshot(self):
        with self.lock:
            return self._snapshot_locked()

    def _snapshot_locked(self):
        version = int(self._meta_locked("version"))
        tip = self._meta_locked("tip")
        utxos = [
            [[rb, ti, oi], amount]
            for rb, ti, oi, amount in self.conn.execute(
                "SELECT ref_block,ref_tx,ref_out,amount FROM utxos "
                "ORDER BY ref_block,ref_tx,ref_out"
            )
        ]
        return {"version": version, "tip": tip, "utxos": utxos}

    def fetch(self, after):
        with self.lock:
            current = int(self._meta_locked("version"))
            if after < 0 or after > current:
                raise ValidationError(
                    "$.after", "after 必须是 0 至当前版本之间的整数"
                )
            rows = self.conn.execute(
                "SELECT version,removed,added FROM batches "
                "WHERE version>? ORDER BY version",
                (after,),
            ).fetchall()
            batches = [
                {"version": v,
                 "removed": json.loads(rm),
                 "added": json.loads(ad)}
                for v, rm, ad in rows
            ]
            return {"version": current, "batches": batches}

    def close(self):
        with self.lock:
            self.conn.close()


# ---------- HTTP 层 ----------

class Handler(BaseHTTPRequestHandler):
    server_version = "ChainWarehouse/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[http] %s - %s\n" % (self.address_string(),
                                               fmt % args))

    def _send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status, path, message):
        self._send_json(status, {"error": message, "path": path})

    def do_GET(self):
        if self.path.split("?", 1)[0] == "/snapshot":
            try:
                self._send_json(200, self.server.engine.snapshot())
            except Exception as exc:  # pragma: no cover - 兜底
                sys.stderr.write(repr(exc) + "\n")
                self._send_error(500, "$", "内部错误")
            return
        if self.path.split("?", 1)[0] in ("/blocks", "/fetch"):
            self._send_method_not_allowed("POST")
            return
        self._send_error(404, "$", "未知路径")

    def _send_method_not_allowed(self, allow):
        body = json.dumps({"error": "方法不允许", "path": "$"},
                          ensure_ascii=False).encode("utf-8")
        self.send_response(405)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Allow", allow)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/snapshot":
            self._send_method_not_allowed("GET")
            return
        if path not in ("/blocks", "/fetch"):
            self._send_error(404, "$", "未知路径")
            return
        try:
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                length = -1
            if length < 0:
                self._send_error(400, "$", "Content-Length 非法")
                return
            raw = self.rfile.read(length) if length > 0 else b""
            node, base = _parse_json_object(raw)
            if path == "/blocks":
                block = _validate_block_node(node, base)
                result = self.server.engine.ingest(block)
            else:
                after = _validate_fetch_node(node, base)
                result = self.server.engine.fetch(after)
            self._send_json(200, result)
        except ValidationError as exc:
            self._send_error(400, exc.path, exc.message)
        except Exception as exc:  # pragma: no cover - 兜底
            sys.stderr.write(repr(exc) + "\n")
            self._send_error(500, "$", "内部错误")


# ---------- 请求校验（严格模式：未知/重复/缺字段一律 400） ----------

def _parse_json_object(raw):
    """解析 JSON；语法错报 $，任意层级的重复键报出完整 JSON 路径。"""
    def pairs_hook(pairs):
        # 用 tuple 标记对象，JSON 本身没有 tuple 类型，不会与真实数据冲突
        return ("__obj__", pairs)

    try:
        root = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs_hook)
    except (ValueError, UnicodeDecodeError):
        raise ValidationError("$", "JSON 语法错误")
    if not (isinstance(root, tuple) and root
            and root[0] == "__obj__"):
        raise ValidationError("$", "请求体必须是 JSON 对象")
    return _normalize(root, "$"), "$"


def _normalize(node, path):
    """递归把 pairs-hook 结构还原为普通 dict/list，并检测重复键。"""
    if isinstance(node, tuple) and node and node[0] == "__obj__":
        result = {}
        for key, value in node[1]:
            if not isinstance(key, str):
                raise ValidationError(path, "对象键必须是字符串")
            child_path = _join(path, key)
            if key in result:
                raise ValidationError(child_path, "重复字段")
            result[key] = _normalize(value, child_path)
        return result
    if isinstance(node, list):
        return [_normalize(value, "%s[%d]" % (path, i))
                for i, value in enumerate(node)]
    return node


def _validate_block_node(node, base):
    for required in ("id", "parent", "work", "txs"):
        if required not in node:
            raise ValidationError(base, "缺少字段: %s" % required)
    allowed = {"id", "parent", "work", "txs"}
    for key in node:
        if key not in allowed:
            raise ValidationError(_join(base, key), "未知字段")

    block_id = node["id"]
    if not _is_identifier(block_id):
        raise ValidationError(_join(base, "id"),
                              "id 必须是非空 ASCII 字母数字串")
    if block_id == GENESIS:
        raise ValidationError(_join(base, "id"), "创世块 G 禁止重定义")

    parent = node["parent"]
    if not _is_identifier(parent):
        raise ValidationError(_join(base, "parent"),
                              "parent 必须是非空 ASCII 字母数字串")

    work = node["work"]
    if not _is_plain_int(work) or work <= 0:
        raise ValidationError(_join(base, "work"),
                              "work 必须是正整数（布尔值不接受）")

    txs = node["txs"]
    if not isinstance(txs, list):
        raise ValidationError(_join(base, "txs"), "txs 必须是数组")

    for tx_idx, tx in enumerate(txs):
        txp = "%s.txs[%d]" % (base, tx_idx)
        if not isinstance(tx, list) or len(tx) != 2:
            raise ValidationError(txp, "交易必须是 [inputs, outputs] 二元数组")
        inputs, outputs = tx
        if not isinstance(inputs, list):
            raise ValidationError(txp + "[0]", "inputs 必须是数组")
        for in_idx, ref in enumerate(inputs):
            rp = "%s[0][%d]" % (txp, in_idx)
            if not isinstance(ref, list) or len(ref) != 3:
                raise ValidationError(rp, "输入引用必须是三元数组")
            if not _is_identifier(ref[0]):
                raise ValidationError("%s[0]" % rp,
                                      "引用块 id 必须是非空 ASCII 字母数字串")
            if not _is_plain_int(ref[1]) or ref[1] < 0:
                raise ValidationError("%s[1]" % rp,
                                      "交易号必须是非负整数（布尔值不接受）")
            if not _is_plain_int(ref[2]) or ref[2] < 0:
                raise ValidationError("%s[2]" % rp,
                                      "输出号必须是非负整数（布尔值不接受）")

        if not isinstance(outputs, list):
            raise ValidationError(txp + "[1]", "outputs 必须是数组")
        for out_idx, amount in enumerate(outputs):
            ap = "%s[1][%d]" % (txp, out_idx)
            if not _is_plain_int(amount) or amount < 0:
                raise ValidationError(ap,
                                      "金额必须是非负整数（布尔值不接受）")

    return {"id": block_id, "parent": parent, "work": work, "txs": txs}


def _validate_fetch_node(node, base):
    allowed = {"after"}
    for key in node:
        if key not in allowed:
            raise ValidationError(_join(base, key), "未知字段")
    if "after" not in node:
        raise ValidationError(base, "缺少字段: after")
    after = node["after"]
    if not _is_plain_int(after):
        raise ValidationError(_join(base, "after"),
                              "after 必须是整数（布尔值不接受）")
    return after


def _is_identifier(value):
    return (isinstance(value, str) and len(value) > 0
            and all("a" <= ch <= "z" or "A" <= ch <= "Z"
                    or "0" <= ch <= "9" for ch in value))


def _is_plain_int(value):
    # bool 是 int 的子类，整数位置必须显式拒绝布尔
    return isinstance(value, int) and not isinstance(value, bool)


def _join(base, key):
    return base + "." + key if base != "$" else "$." + key


def main():
    if len(sys.argv) < 2:
        sys.stderr.write("用法: python3 server.py <sqlite数据库路径> "
                         "[host] [port]\n")
        return 2
    db_path = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 8000

    server = ThreadingHTTPServer((host, port), Handler)
    server.engine = Engine(db_path)
    sys.stderr.write("监听 http://%s:%d ，数据库 %s\n"
                     % (host, port, os.path.abspath(db_path)))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.engine.close()
        server.server_close()


if __name__ == "__main__":
    raise SystemExit(main())
