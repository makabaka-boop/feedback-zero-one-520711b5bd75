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
from fractions import Fraction
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


class ConflictError(Exception):
    """请求与当前状态冲突（409），如 trace 的 base 与当前版本不一致。"""

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

    # ---------- 来源追踪（/trace） ----------

    def trace(self, base_ver, seeds, known):
        """固定 base 版本的规范链，求全链约束下当前 UTXO 的逐来源上下界。

        seeds: 铸造输出引用列表（各为一种来源）；其余铸造输出同属"其他来源"。
        known: [(引用, 来源零基号, 下界, 上界)]，重复约束自然取交集。
        """
        with self.lock:
            version = int(self._meta_locked("version"))
            if base_ver != version:
                raise ConflictError(
                    "$.base", "base 必须等于当前版本（整次查询固定该版规范链）")
            tip = self._meta_locked("tip")
            chain = self._canonical_chain_locked(tip)
            seed_refs = [tuple(s) for s in seeds]
            known_refs = [(tuple(e[0]), e[1], e[2], e[3]) for e in known]
            return _trace_solve(base_ver, seed_refs, known_refs, chain)

    def _canonical_chain_locked(self, tip):
        """重放当前 tip 规范链（根->叶），返回
        (txs, amounts, mint_refs, utxo_refs)：
        - txs: [(块id, 交易号, inputs, outputs)]，链上顺序；
        - amounts: {(块id,交易号,输出号): 金额}（含已花费输出）；
        - mint_refs: 铸造输出（空输入交易的输出）集合；
        - utxo_refs: 当前 UTXO 引用，按引用排序。
        tip 必为 valid，重放必然成功。
        """
        blocks = {
            r[0]: json.loads(r[1])
            for r in self.conn.execute("SELECT id,data FROM blocks")
        }
        order = []
        cur = tip
        while cur and cur in blocks:
            order.append(cur)
            cur = blocks[cur]["parent"]
        order.reverse()

        txs = []
        amounts = {}
        mint_refs = set()
        utxo = {}
        for bid in order:
            for tx_idx, tx in enumerate(blocks[bid]["txs"]):
                inputs, outputs = tx[0], tx[1]
                for ref in inputs:
                    del utxo[(ref[0], ref[1], ref[2])]
                for out_idx, amount in enumerate(outputs):
                    key = (bid, tx_idx, out_idx)
                    amounts[key] = amount
                    utxo[key] = amount
                    if not inputs:
                        mint_refs.add(key)
                txs.append((bid, tx_idx, inputs, outputs))
        return txs, amounts, mint_refs, sorted(utxo.keys())

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
        if self.path.split("?", 1)[0] in ("/blocks", "/fetch", "/trace"):
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
        if path not in ("/blocks", "/fetch", "/trace"):
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
            elif path == "/fetch":
                after = _validate_fetch_node(node, base)
                result = self.server.engine.fetch(after)
            else:
                base_ver, seeds, known = _validate_trace_node(node, base)
                result = self.server.engine.trace(base_ver, seeds, known)
            self._send_json(200, result)
        except ValidationError as exc:
            self._send_error(400, exc.path, exc.message)
        except ConflictError as exc:
            self._send_error(409, exc.path, exc.message)
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


def _validate_ref_node(ref, path):
    """输出引用 [块id, 零基交易号, 零基输出号] 的结构校验。"""
    if not isinstance(ref, list) or len(ref) != 3:
        raise ValidationError(path, "引用必须是三元数组")
    if not _is_identifier(ref[0]):
        raise ValidationError(path + "[0]",
                              "引用块 id 必须是非空 ASCII 字母数字串")
    if not _is_plain_int(ref[1]) or ref[1] < 0:
        raise ValidationError(path + "[1]",
                              "交易号必须是非负整数（布尔值不接受）")
    if not _is_plain_int(ref[2]) or ref[2] < 0:
        raise ValidationError(path + "[2]",
                              "输出号必须是非负整数（布尔值不接受）")


def _validate_trace_node(node, base):
    """POST /trace 请求体结构校验（仅 base/seeds/known 三字段）。"""
    for required in ("base", "seeds", "known"):
        if required not in node:
            raise ValidationError(base, "缺少字段: %s" % required)
    allowed = {"base", "seeds", "known"}
    for key in node:
        if key not in allowed:
            raise ValidationError(_join(base, key), "未知字段")

    base_ver = node["base"]
    if not _is_plain_int(base_ver) or base_ver < 0:
        raise ValidationError(_join(base, "base"),
                              "base 必须是非负整数（布尔值不接受）")

    seeds = node["seeds"]
    if not isinstance(seeds, list):
        raise ValidationError(_join(base, "seeds"), "seeds 必须是数组")
    seen_refs = set()
    for idx, ref in enumerate(seeds):
        rp = "%s.seeds[%d]" % (base, idx)
        _validate_ref_node(ref, rp)
        key = (ref[0], ref[1], ref[2])
        if key in seen_refs:
            raise ValidationError(rp, "seeds 引用不得重复")
        seen_refs.add(key)

    known = node["known"]
    if not isinstance(known, list):
        raise ValidationError(_join(base, "known"), "known 必须是数组")
    for idx, entry in enumerate(known):
        ep = "%s.known[%d]" % (base, idx)
        if not isinstance(entry, list) or len(entry) != 4:
            raise ValidationError(
                ep, "known 项必须是 [引用, 来源号, 下界, 上界] 四元数组")
        _validate_ref_node(entry[0], ep + "[0]")
        src = entry[1]
        if not _is_plain_int(src) or src < 0:
            raise ValidationError(ep + "[1]",
                                  "来源号必须是非负整数（布尔值不接受）")
        if src >= len(seeds):
            raise ValidationError(ep + "[1]", "来源号必须在 seeds 范围内")
        lo, hi = entry[2], entry[3]
        if not _is_plain_int(lo) or lo < 0:
            raise ValidationError(ep + "[2]",
                                  "下界必须是非负整数（布尔值不接受）")
        if not _is_plain_int(hi) or hi < 0:
            raise ValidationError(ep + "[3]",
                                  "上界必须是非负整数（布尔值不接受）")
        if lo > hi:
            raise ValidationError(ep + "[3]", "上界必须不小于下界")
    return base_ver, seeds, known


def _is_identifier(value):
    return (isinstance(value, str) and len(value) > 0
            and all("a" <= ch <= "z" or "A" <= ch <= "Z"
                    or "0" <= ch <= "9" for ch in value))


def _is_plain_int(value):
    # bool 是 int 的子类，整数位置必须显式拒绝布尔
    return isinstance(value, int) and not isinstance(value, bool)


def _join(base, key):
    return base + "." + key if base != "$" else "$." + key


# ---------- 来源追踪求解（精确有理数 LP + 分支定界兜底） ----------
#
# 模型：每种 seed 铸造输出是一种来源，其余铸造输出同属"其他来源"（隐式）。
# 对每条"边"（交易输出、销毁槽）与每种 seed 设非负整数变量 x[(来源,边)]：
# - 铸造输出边：拥有者来源 = 全额，其余来源 = 0（常量，直接代入）；
# - 非铸造交易按来源守恒：输入来源量 = 输出来源量 + 销毁槽来源量；
# - 每个输出/销毁槽被各来源量恰好填满：Σ来源 x ≤ 金额（"其他来源"补差，
#   其守恒与非负由金额守恒自动保证，无需显式建模）；
# - known 提供单变量上下界。
# 在此约束系下求当前 UTXO 每个输出、每种 seed 来源量的上下界。
# 约束矩阵为网络流结构，LP 顶点整数（已大规模随机验证）；仍保留分支定界
# 兜底，保证任何情况下返回精确的整数界。

_LP_OPTIMAL = "optimal"
_LP_INFEASIBLE = "infeasible"
_LP_UNBOUNDED = "unbounded"


class _Tableau:
    """(<=, =) 行系的精确单纯形表，Bland 法则保证终止。

    单元格优先用 int：网络流矩阵的主元恒为 ±1 时全程整数运算；
    一旦出现其他主元，该行动态退化为 Fraction，混合运算依然精确。
    """

    def __init__(self, n_vars, rows):
        norm = []
        for coeffs, sense, rhs in rows:
            coeffs = [int(c) for c in coeffs]
            rhs = int(rhs)
            if rhs < 0:
                coeffs = [-c for c in coeffs]
                rhs = -rhs
                if sense == "<=":
                    sense = ">="
            norm.append((coeffs, sense, rhs))
        # 列规划：原始变量 + 松弛/剩余 + 人工变量
        plan = []
        n_cols = n_vars
        for coeffs, sense, rhs in norm:
            if sense == "<=":
                plan.append([("slack", n_cols, 1)])
                n_cols += 1
            elif sense == ">=":
                plan.append([("surplus", n_cols, -1), ("art", n_cols + 1, 1)])
                n_cols += 2
            else:
                plan.append([("art", n_cols, 1)])
                n_cols += 1
        self.n_orig = n_vars
        self.n_cols = n_cols
        self.artificial = set()
        self.basic = [-1] * len(norm)
        T = []
        for i, ((coeffs, sense, rhs), prow) in enumerate(zip(norm, plan)):
            row = [0] * (n_cols + 1)
            for j, c in enumerate(coeffs):
                row[j] = c
            for kind, cidx, sign in prow:
                row[cidx] = sign
                if kind in ("slack", "art"):
                    self.basic[i] = cidx
                if kind == "art":
                    self.artificial.add(cidx)
            row[n_cols] = rhs
            T.append(row)
        self.T = T
        self.m = len(T)
        self._basic_set = set(self.basic)

    def _pivot(self, r, j):
        T = self.T
        pv = T[r][j]
        if pv == 1:
            pass
        elif pv == -1:
            T[r] = [-x for x in T[r]]
        else:
            inv = Fraction(1) / pv
            T[r] = [x * inv for x in T[r]]
        prow = T[r]
        for i in range(self.m):
            if i != r:
                f = T[i][j]
                if f:
                    T[i] = [a - f * b for a, b in zip(T[i], prow)]
        self._basic_set.discard(self.basic[r])
        self.basic[r] = j
        self._basic_set.add(j)

    def _optimize(self, cost):
        """Bland 法则最大化；cost[j] 为 None 的列禁止入基。"""
        T = self.T
        n_cols = self.n_cols
        basic_set = self._basic_set
        while True:
            crow = [(cost[self.basic[i]], i) for i in range(self.m)
                    if cost[self.basic[i]]]
            entering = -1
            for j in range(n_cols):
                if j in basic_set:
                    continue
                cj = cost[j]
                if cj is None:
                    continue
                red = cj
                for cb, i in crow:
                    tij = T[i][j]
                    if tij:
                        red -= cb * tij
                if red > 0:
                    entering = j
                    break
            if entering < 0:
                return _LP_OPTIMAL
            # 最小比值（交叉相乘比较，避免产生分数）；平局取小下标基本变量
            leave = -1
            best_num = best_den = None
            best_basic = None
            for i in range(self.m):
                tij = T[i][entering]
                if tij > 0:
                    num = T[i][n_cols]
                    bi = self.basic[i]
                    if (best_num is None
                            or num * best_den < best_num * tij
                            or (num * best_den == best_num * tij
                                and bi < best_basic)):
                        best_num, best_den = num, tij
                        leave = i
                        best_basic = bi
            if leave < 0:
                return _LP_UNBOUNDED
            self._pivot(leave, entering)

    def phase1(self):
        """第一阶段：人工变量最小化；不可行返回 _LP_INFEASIBLE。"""
        cost = [0] * self.n_cols
        for j in self.artificial:
            cost[j] = -1
        self._optimize(cost)
        total = 0
        for i in range(self.m):
            if self.basic[i] in self.artificial:
                total += self.T[i][self.n_cols]
        if total > 0:
            return _LP_INFEASIBLE
        # 把仍为基本变量的人工变量（值 0，退化）换出；换不出的行是冗余行
        i = 0
        while i < self.m:
            if self.basic[i] not in self.artificial:
                i += 1
                continue
            pivoted = False
            for j in range(self.n_cols):
                if (j not in self.artificial and j not in self._basic_set
                        and self.T[i][j] != 0):
                    self._pivot(i, j)
                    pivoted = True
                    break
            if pivoted:
                i += 1
            else:
                self.T.pop(i)
                self.basic.pop(i)
                self.m -= 1
        self._basic_set = set(self.basic)
        return _LP_OPTIMAL

    def optimize(self, cost_orig):
        """在当前可行基上按新目标热启动第二阶段。"""
        cost = [None if j in self.artificial else 0
                for j in range(self.n_cols)]
        for j, c in enumerate(cost_orig):
            cost[j] = int(c)
        return self._optimize(cost)

    def solution(self):
        x = [0] * self.n_cols
        for i in range(self.m):
            x[self.basic[i]] = self.T[i][self.n_cols]
        return x[:self.n_orig]


def _lp_optimize(n_vars, rows, cost_orig):
    """一次性求解：返回 (status, 最优值, 最优解)。"""
    tab = _Tableau(n_vars, rows)
    if tab.phase1() != _LP_OPTIMAL:
        return _LP_INFEASIBLE, None, None
    if tab.optimize(cost_orig) != _LP_OPTIMAL:
        return _LP_UNBOUNDED, None, None
    x = tab.solution()
    val = sum(Fraction(c) * xi for c, xi in zip(cost_orig, x))
    return _LP_OPTIMAL, val, x


def _is_integral_point(x):
    return all(xi.denominator == 1 for xi in x)


def _ilp_var_bound(n_vars, rows, j, maximize):
    """整数规划求 x_j 的 min/max（精确）；无整数可行解返回 None。"""
    coeff = 1 if maximize else -1

    # 分支定界统一最小化 -coeff*x_j；其最优值取负即 coeff*x_j 的最大值。
    def lp_min(sub_rows):
        cost = [Fraction(0)] * n_vars
        cost[j] = Fraction(coeff)
        st, val, x = _lp_optimize(n_vars, sub_rows, cost)
        if st != _LP_OPTIMAL:
            return st, None, None
        return st, -val, x

    best_int = [None]

    def branch(sub_rows):
        st, lb, x = lp_min(sub_rows)
        if st != _LP_OPTIMAL:
            return
        if best_int[0] is not None and lb >= best_int[0]:
            return  # 剪枝
        if _is_integral_point(x):
            best_int[0] = lb
            return
        fj = next(t for t, xt in enumerate(x) if xt.denominator != 1)
        v = x[fj]
        fl = v.numerator // v.denominator
        row_lo = ([Fraction(1) if t == fj else Fraction(0)
                   for t in range(n_vars)], "<=", Fraction(fl))
        row_hi = ([Fraction(-1) if t == fj else Fraction(0)
                   for t in range(n_vars)], "<=", Fraction(-(fl + 1)))
        branch(sub_rows + [row_hi])
        branch(sub_rows + [row_lo])

    branch(list(rows))
    if best_int[0] is None:
        return None
    return -coeff * best_int[0]


def _solve_bounds(n_vars, rows, query_js):
    """对 query_js 中每个变量求整数精确 (lo, hi)；不可行返回 None。"""
    tab = _Tableau(n_vars, rows)
    if tab.phase1() != _LP_OPTIMAL:
        return None
    out = {}
    for j in query_js:
        lo = hi = None
        for maximize in (False, True):
            cost = [Fraction(0)] * n_vars
            cost[j] = Fraction(1 if maximize else -1)
            if tab.optimize(cost) != _LP_OPTIMAL:  # 变量有界，不会触发
                raise RuntimeError("trace 求解异常：无界")
            x = tab.solution()
            if _is_integral_point(x):
                b = int(x[j])
            else:
                # 分数顶点兜底：分支定界求精确整数界
                b = _ilp_var_bound(n_vars, rows, j, maximize)
                if b is None:
                    return None
            if maximize:
                hi = b
            else:
                lo = b
        out[j] = (lo, hi)
    return out


def _dense(coeffs, n):
    row = [0] * n
    for j, c in coeffs.items():
        row[j] = c
    return row


def _trace_solve(base_ver, seeds, known, chain):
    """在固定规范链上求解来源上下界，返回 /trace 的 200 响应体。

    seeds: [引用元组]；known: [(引用元组, 来源号, 下界, 上界)]；
    chain: (txs, amounts, mint_refs, utxo_refs)。
    """
    txs, amounts, mint_refs, utxo_refs = chain
    k = len(seeds)

    # ---- 语义校验：引用须在固定链内，上界不得超过输出金额 ----
    for i, ref in enumerate(seeds):
        if ref not in mint_refs:
            raise ValidationError(
                "$.seeds[%d]" % i, "seed 必须是规范链内的铸造输出引用")
    for i, (ref, src, lo, hi) in enumerate(known):
        if ref not in amounts:
            raise ValidationError(
                "$.known[%d][0]" % i, "引用必须在规范链内")
        if hi > amounts[ref]:
            raise ValidationError(
                "$.known[%d][3]" % i, "上界不得超过输出金额")

    # ---- 边模型：变量边 = 非铸造且金额>0 的输出边 + 金额>0 的销毁槽 ----
    var_edges = []
    edge_amount = {}
    for pos, (bid, tidx, inputs, outputs) in enumerate(txs):
        if not inputs:
            continue  # 铸造交易的输出全是常量，无销毁槽
        for out_idx, amount in enumerate(outputs):
            if amount > 0:
                ref = (bid, tidx, out_idx)
                var_edges.append(ref)
                edge_amount[ref] = amount
        burn = sum(amounts[(r[0], r[1], r[2])] for r in inputs) - sum(outputs)
        if burn > 0:
            edge = ("burn", pos)
            var_edges.append(edge)
            edge_amount[edge] = burn

    vidx = {}
    for i in range(k):
        for e in var_edges:
            vidx[(i, e)] = len(vidx)
    n_vars = len(vidx)

    # ---- 约束行 ----
    rows = []
    for pos, (bid, tidx, inputs, outputs) in enumerate(txs):
        if not inputs:
            continue
        for i in range(k):
            coeffs = {}
            supply = 0
            for out_idx, amount in enumerate(outputs):
                if amount > 0:
                    j = vidx[(i, (bid, tidx, out_idx))]
                    coeffs[j] = coeffs.get(j, 0) + 1
            burn_edge = ("burn", pos)
            if burn_edge in edge_amount:
                j = vidx[(i, burn_edge)]
                coeffs[j] = coeffs.get(j, 0) + 1
            for r in inputs:
                ref = (r[0], r[1], r[2])
                if ref in mint_refs:
                    if ref == seeds[i]:
                        supply += amounts[ref]
                elif amounts[ref] > 0:
                    j = vidx[(i, ref)]
                    coeffs[j] = coeffs.get(j, 0) - 1
            if any(coeffs.values()):
                rows.append((_dense(coeffs, n_vars), "=", supply))
    for e in var_edges:
        coeffs = {vidx[(i, e)]: 1 for i in range(k)}
        rows.append((_dense(coeffs, n_vars), "<=", edge_amount[e]))

    # ---- known：常量边直接判定，变量边加上下界 ----
    infeasible = False
    for ref, src, lo, hi in known:
        if ref in mint_refs or amounts[ref] == 0:
            const = amounts[ref] if ref == seeds[src] else 0
            if not (lo <= const <= hi):
                infeasible = True
        else:
            j = vidx[(src, ref)]
            rows.append((_dense({j: 1}, n_vars), "<=", hi))
            rows.append((_dense({j: -1}, n_vars), "<=", -lo))

    # ---- 查询：当前 UTXO 逐来源 ----
    query = {}
    for ref in utxo_refs:
        for i in range(k):
            if ref in mint_refs or amounts[ref] == 0:
                const = amounts[ref] if ref == seeds[i] else 0
                query[(i, ref)] = ("const", const)
            else:
                query[(i, ref)] = ("var", vidx[(i, ref)])

    if infeasible:
        return {"base": base_ver, "status": "infeasible", "rows": []}

    bounds = {}
    var_js = sorted({v for kind, v in query.values() if kind == "var"})
    if n_vars > 0:
        # 即使查询全是常量，也必须运行可行性判定（系统可能本身无解）
        bounds = _solve_bounds(n_vars, rows, var_js)
        if bounds is None:
            return {"base": base_ver, "status": "infeasible", "rows": []}

    rows_out = []
    for ref in utxo_refs:
        per_seed = []
        for i in range(k):
            kind, v = query[(i, ref)]
            if kind == "const":
                per_seed.append([v, v])
            else:
                lo, hi = bounds[v]
                per_seed.append([lo, hi])
        rows_out.append([list(ref), per_seed])
    return {"base": base_ver, "status": "ok", "rows": rows_out}


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
