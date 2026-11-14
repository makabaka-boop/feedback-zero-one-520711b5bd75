#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验收三：进程重启后状态完整恢复，批次/UTXO/链状态原子持久。"""

import json
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acceptance_helper import Server, check, finish  # noqa: E402


def main():
    srv = Server()
    srv.start()
    try:
        # 造一条含分叉切换的历史，产生多版本批次
        M = {"id": "M", "parent": "G", "work": 1, "txs": [[[], [100]]]}
        srv.post("/blocks", M)
        A = {"id": "A", "parent": "M", "work": 2,
             "txs": [[[["M", 0, 0]], [40, 60]]]}
        srv.post("/blocks", A)
        B = {"id": "B", "parent": "M", "work": 5, "txs": []}
        snap_before = srv.post("/blocks", B)[1]
        check(snap_before["tip"] == "B" and snap_before["version"] == 3,
              "重启前：B 高 work 切换为 tip，版本为 3（M/A/B 各切换一次）")

        # 一个待定块和一个无效块，验证状态列也持久
        P = {"id": "P", "parent": "MISS", "work": 1, "txs": []}
        srv.post("/blocks", P)
        BAD = {"id": "BAD", "parent": "A", "work": 1,
               "txs": [[[["M", 0, 0]], [1]]]}  # M 的输出已被 A 花费
        srv.post("/blocks", BAD)
        check(srv.block_status("P") == "pending"
              and srv.block_status("BAD") == "invalid",
              "重启前 P 待定、BAD 无效")

        batches_before = srv.post("/fetch", {"after": 0})[1]
        check([b["version"] for b in batches_before["batches"]]
              == [1, 2, 3],
              "重启前存在批次 1、2、3")

        # ---- 优雅终止后重启，快照逐字段一致 ----
        srv.restart()
        snap_after = srv.get("/snapshot")[1]
        check(snap_after == snap_before,
              "重启后快照与重启前完全一致（version/tip/utxos）")

        batches_after = srv.post("/fetch", {"after": 0})[1]
        check(batches_after == batches_before,
              "重启后续取全部批次与重启前一致（含完整块内容）")
        check(srv.block_status("P") == "pending"
              and srv.block_status("BAD") == "invalid"
              and srv.block_status("G") == "valid",
              "重启后 pending/invalid/创世块状态均保留")

        # 续取游标语义保持：after=2 只返回批次 3
        fetched = srv.post("/fetch", {"after": 2})[1]
        check(len(fetched["batches"]) == 1
              and fetched["batches"][0]["version"] == 3
              and [b["id"] for b in fetched["batches"][0]["removed"]]
              == ["A"],
              "重启后 after=2 仅返回批次 3，removed=[A]")

        # 待定块的父晚到，重启后仍能级联生效
        MISS = {"id": "MISS", "parent": "G", "work": 1, "txs": []}
        snap = srv.post("/blocks", MISS)[1]
        check(srv.block_status("P") == "valid",
              "晚到父 MISS 使重启前残留的待定块 P 转为 valid")

        # 重放历史块：全部幂等，版本与 tip 不动
        version_now = snap["version"]
        for b in (M, A, B, P, BAD, MISS):
            status, body = srv.post("/blocks", dict(b))
            assert status == 200, body
        check(srv.get("/snapshot")[1]["version"] == version_now,
              "重启后重放全部历史块幂等，不产生新版本")

        # ---- 强杀（SIGKILL）后重启：SQLite 回滚日志保证只见完整旧态 ----
        srv.kill()
        # 数据库文件应仍可独立打开且结构完整
        conn = sqlite3.connect(srv.db)
        try:
            keys = {r[0] for r in conn.execute(
                "SELECT key FROM meta")}
            check(keys == {"version", "tip"}, "强杀后 meta 表完整")
            n_blocks = conn.execute("SELECT COUNT(*) FROM blocks").fetchone()[0]
            n_batches = conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0]
            n_utxo = conn.execute("SELECT COUNT(*) FROM utxos").fetchone()[0]
            check(n_blocks >= 6 and n_batches == 3 and n_utxo >= 1,
                  "强杀后块/批次/UTXO 表行数均为已提交的完整状态")
        finally:
            conn.close()

        srv.start()
        snap_kill = srv.get("/snapshot")[1]
        check(snap_kill["version"] == version_now,
              "SIGKILL 重启后版本号停留在最后一次完整提交")
        check(snap_kill["utxos"] == snap_before["utxos"]
              and snap_kill["tip"] == snap_before["tip"],
              "SIGKILL 重启后 tip 与 UTXO 均为已提交状态（无半提交数据）")

        # 重启后服务可继续收块并正常产生新版本批次
        NEW = {"id": "NEW", "parent": "P", "work": 9, "txs": []}
        snap_new = srv.post("/blocks", NEW)[1]
        check(snap_new["tip"] == "NEW"
              and snap_new["version"] == version_now + 1,
              "重启恢复后新块可继续驱动 tip 切换与版本递增")
        fetched = srv.post("/fetch", {"after": version_now})[1]
        check(len(fetched["batches"]) == 1
              and fetched["batches"][0]["version"] == version_now + 1,
              "新版本批次可被续取")

        # 新进程对同一 DB 文件的初始化必须幂等（不重复插入 G）
        conn = sqlite3.connect(srv.db)
        try:
            g_count = conn.execute(
                "SELECT COUNT(*) FROM blocks WHERE id='G'").fetchone()[0]
            check(g_count == 1, "多次启动不会重复创建创世块 G")
            g_data = json.loads(conn.execute(
                "SELECT data FROM blocks WHERE id='G'").fetchone()[0])
            check(g_data == {"id": "G", "parent": "", "work": 0,
                             "txs": []},
                  "创世块结构为 {id:G, parent:空串, work:0, txs:[]}")
        finally:
            conn.close()
        time.sleep(0)  # 保留时序扩展位
    finally:
        srv.stop()
    finish("重启持久化/原子性")


if __name__ == "__main__":
    main()
