#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验收二：分支双花、铸造约束、同块引用、无效块连带后代、tip 选择。"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acceptance_helper import Server, check, expect_bad, finish  # noqa: E402


def post(srv, block):
    status, body = srv.post("/blocks", block)
    assert status == 200, (status, body)
    return body


def main():
    srv = Server()
    srv.start()
    try:
        # M：空输入铸造 100，产生 M.tx0.out0
        M = {"id": "M", "parent": "G", "work": 1,
             "txs": [[[], [100]]]}
        snap = post(srv, M)
        check(snap["tip"] == "M" and snap["utxos"]
              == [[["M", 0, 0], 100]], "铸造块 M 的快照含一个 100 UTXO")

        # 空输入允许铸造：MB 基于 M 再铸 7，与 M 原有 UTXO 并存
        mint = {"id": "MB", "parent": "M", "work": 1,
                "txs": [[[], [7]]]}
        snap = post(srv, mint)
        check(srv.block_status("MB") == "valid" and snap["tip"] == "MB",
              "空输入铸造块 MB 有效并成为 tip（累计 work 更高）")
        check(snap["utxos"] == [[["M", 0, 0], 100], [["MB", 0, 0], 7]],
              "铸造输出与原有 UTXO 并存且按引用排序")

        # A：花费 M 的输出，拆成 40 + 60（与 MB 竞争，id 更小平局胜出）
        A = {"id": "A", "parent": "M", "work": 1,
             "txs": [[[["M", 0, 0]], [40, 60]]]}
        snap = post(srv, A)
        check(snap["tip"] == "A", "A 与 MB 等 work，ASCII id 更小成为 tip")
        check(snap["utxos"] == [[["A", 0, 0], 40], [["A", 0, 1], 60]],
              "快照只含 tip 链 UTXO：侧链 MB 的铸造输出不在其中，"
              "旧输出被花费移除")

        # 同块较早交易可被后续交易引用
        A2 = {"id": "A2", "parent": "M", "work": 2,
              "txs": [
                  [[["M", 0, 0]], [30, 70]],
                  [[["A2", 0, 1]], [70]],
              ]}
        snap = post(srv, A2)
        check(srv.block_status("A2") == "valid" and snap["tip"] == "A2",
              "本块第二笔交易引用第一笔输出，块有效")
        check(snap["utxos"] == [[["A2", 0, 0], 30], [["A2", 1, 0], 70]],
              "链内滚动花费后的 UTXO 集合正确")

        # ---- 分支双花：B1、B2 都基于 A 花费同一输出 A.tx0.out0 ----
        B1 = {"id": "B1", "parent": "A", "work": 1,
              "txs": [[[["A", 0, 0]], [40]]]}
        B2 = {"id": "B2", "parent": "A", "work": 1,
              "txs": [[[["A", 0, 0]], [40]]]}
        post(srv, B1)
        snap = post(srv, B2)
        check(srv.block_status("B1") == "valid"
              and srv.block_status("B2") == "valid",
              "两个分支各自独立花费同一输出，各自都有效")
        # A2 累计 work 更高（M1+A2 2=3 > M1+A1+B1 1=3? 相等；A2 id 更小）
        # 当前 tip 由累计 work 决定：A2 链 = 1+2=3，B 链 = 1+1+1=3，平局 id 小者
        check(snap["tip"] == "A2", "累计 work 平局时 ASCII 最小 id 为 tip")

        # 给 B1 叠加高 work 子块，使 B1 链成为 tip，触发撤回 A2 链
        C1 = {"id": "C1", "parent": "B1", "work": 5, "txs": []}
        snap = post(srv, C1)
        check(snap["tip"] == "C1", "高 work 分支 C1 成为新 tip")
        status, fetched = srv.post("/fetch",
                                   {"after": srv.get("/snapshot")[1][
                                       "version"] - 1})
        batch = fetched["batches"][-1]
        check([b["id"] for b in batch["removed"]] == ["A2"]
              and [b["id"] for b in batch["added"]] == ["A", "B1", "C1"],
              "分叉切换：共同祖先 M 之外，旧链独有 A2 叶至根撤回，"
              "新链 A、B1、C1 根至叶加入")
        check(snap["utxos"] == [[["A", 0, 1], 60], [["B1", 0, 0], 40]],
              "新 tip 的 UTXO 已按新链重算（A 的 40 在 B1 被花费）")

        # 再把同输出在 B1 链上花第二次 -> 双花块无效
        DS = {"id": "DS", "parent": "C1", "work": 1,
              "txs": [[[["A", 0, 0]], [40]]]}
        snap = post(srv, DS)
        check(srv.block_status("DS") == "invalid",
              "同一输出在本分支已被 B1 花费，DS 双花 invalid")
        check(snap["tip"] == "C1" and snap["version"]
              == srv.get("/snapshot")[1]["version"],
              "无效块不改变 tip 与版本")

        # 无效块的后代连带无效，即使后代本身交易合法
        DSC = {"id": "DSC", "parent": "DS", "work": 100, "txs": []}
        post(srv, DSC)
        check(srv.block_status("DSC") == "invalid",
              "无效块 DS 的后代 DSC 连带 invalid，work 再高也不当 tip")
        check(srv.get("/snapshot")[1]["tip"] == "C1", "tip 仍是 C1")

        # 超额：输出总额 > 输入总额
        OV = {"id": "OV", "parent": "C1", "work": 1,
              "txs": [[[["A", 0, 1]], [61]]]}
        post(srv, OV)
        check(srv.block_status("OV") == "invalid", "输出超额块 invalid")

        # 越界：交易号/输出号越界
        OOB1 = {"id": "OOB1", "parent": "C1", "work": 1,
                "txs": [[[["A", 9, 0]], [1]]]}
        OOB2 = {"id": "OOB2", "parent": "C1", "work": 1,
                "txs": [[[["A", 0, 7]], [1]]]}
        post(srv, OOB1)
        post(srv, OOB2)
        check(srv.block_status("OOB1") == "invalid"
              and srv.block_status("OOB2") == "invalid",
              "越界交易号/输出号均 invalid 但已保存")

        # 同一块内重复花费同一输入
        DUP = {"id": "DUP", "parent": "C1", "work": 1,
               "txs": [[[["A", 0, 1], ["A", 0, 1]], [60, 60]]]}
        post(srv, DUP)
        check(srv.block_status("DUP") == "invalid",
              "同块同笔交易重复输入 invalid")

        # 同块两笔交易先后花同一输出：第二笔无效
        DUP2 = {"id": "DUP2", "parent": "C1", "work": 1,
                "txs": [
                    [[["A", 0, 1]], [60]],
                    [[["A", 0, 1]], [60]],
                ]}
        post(srv, DUP2)
        check(srv.block_status("DUP2") == "invalid",
              "第二笔交易重复花费第一笔已花输出，整块 invalid")

        # 零金额输出合法并进入 UTXO
        Z = {"id": "Z", "parent": "C1", "work": 2,
             "txs": [[[["B1", 0, 0]], [0, 40]]]}
        snap = post(srv, Z)
        check(srv.block_status("Z") == "valid", "零金额输出块有效")
        zero_utxo = [u for u in snap["utxos"] if u[0] == ["Z", 0, 0]]
        check(zero_utxo == [[["Z", 0, 0], 0]], "零金额 UTXO 仍被记录")

        # 结构非法仍 400（与语义无效对照）
        bad = {"id": "BAD", "parent": "C1", "work": 1,
               "txs": [[[["C1", 0]], [1]]]}  # 引用只有两元
        status, body = srv.post("/blocks", bad)
        expect_bad(status, body, "$.txs[0][0][0]", "二元输入引用 400")
        check(srv.block_status("BAD") is None, "结构非法块不落库")

        # 无效块内容重传幂等
        status, snap_before = srv.get("/snapshot")
        status, _ = srv.post("/blocks", dict(OV))
        check(srv.get("/snapshot")[1] == snap_before,
              "无效块同结构重传幂等，快照不变")
    finally:
        srv.stop()
    finish("分支双花/语义无效")


if __name__ == "__main__":
    main()
