#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验收一：父晚到、幂等重传、占位父环、分叉撤回（题目 A/B 示例）、并发串行。"""

import concurrent.futures
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acceptance_helper import Server, check, expect_bad, finish  # noqa: E402


def main():
    srv = Server()
    srv.start()
    try:
        # 初始空库：tip=G，version=0，无未花费
        status, snap = srv.get("/snapshot")
        check(status == 200 and snap == {"version": 0, "tip": "G",
                                         "utxos": []}, "初始快照为空创世态")

        # ---- 父晚到：C 父 B，B 父 G，先交 C 再交 B ----
        C = {"id": "C", "parent": "B", "work": 5, "txs": []}
        status, snap = srv.post("/blocks", C)
        check(status == 200 and snap["tip"] == "G" and snap["version"] == 0,
              "缺父块 C 收后仍待定，tip 保持 G，版本不变")
        check(srv.block_status("C") == "pending", "C 在库中状态为 pending")

        B = {"id": "B", "parent": "G", "work": 2, "txs": []}
        status, snap = srv.post("/blocks", B)
        check(status == 200 and snap["tip"] == "C" and snap["version"] == 1,
              "父 B 到达后级联生效，tip 变为 C，版本 +1")
        check(srv.block_status("B") == "valid"
              and srv.block_status("C") == "valid", "B、C 均为 valid")

        status, fetched = srv.post("/fetch", {"after": 0})
        check(status == 200 and fetched["version"] == 1
              and len(fetched["batches"]) == 1
              and fetched["batches"][0]["version"] == 1
              and fetched["batches"][0]["removed"] == []
              and [b["id"] for b in fetched["batches"][0]["added"]]
              == ["B", "C"],
              "续取 after=0 得到批次 1：根至叶加入 B、C")

        # ---- 同 id 同结构重传：幂等不变 ----
        status, snap2 = srv.post("/blocks", dict(C))
        check(status == 200 and snap2 == snap, "同结构重传 C 幂等，快照不变")

        # 同 id 异内容：拒绝且不改状态
        C_bad = {"id": "C", "parent": "B", "work": 9, "txs": []}
        status, body = srv.post("/blocks", C_bad)
        expect_bad(status, body, "$.id", "同 id 异 work 重传 400")
        check(srv.block_status("C") == "valid", "异内容拒绝后 C 状态未改")

        # ---- 题目示例（隔离复现）：A(work2)、B(work3) 同父，先 A 后 B ----
        # 造一个高 work 父 R，使 A1/B1 两条竞争链的累计 work 超过既有 C 链
        R = {"id": "R", "parent": "G", "work": 10, "txs": []}
        srv.post("/blocks", R)
        A1 = {"id": "A1", "parent": "R", "work": 2, "txs": []}
        B1 = {"id": "B1", "parent": "R", "work": 3, "txs": []}
        srv.post("/blocks", A1)
        status, snap = srv.post("/blocks", B1)
        check(snap["tip"] == "B1", "同父两条链中高 work 的 B1 成为 tip")
        ver_before = srv.get("/snapshot")[1]["version"]
        status, fetched = srv.post("/fetch",
                                   {"after": ver_before - 1})
        # 该批次只含 A1->B1 的差异：旧链独有 A1、新链独有 B1
        batch = fetched["batches"][-1]
        check([b["id"] for b in batch["removed"]] == ["A1"]
              and [b["id"] for b in batch["added"]] == ["B1"]
              and batch["removed"][0].keys() == {"id", "parent", "work", "txs"}
              and batch["added"][0].keys() == {"id", "parent", "work", "txs"},
              "切换批次 removed=[A1]（叶至根）、added=[B1]，项为完整块")

        # ---- 占位父环：X 父 Y（Y 不存在，先待定），再交 Y 父 X ----
        X = {"id": "X", "parent": "Y", "work": 1, "txs": []}
        status, _ = srv.post("/blocks", X)
        check(status == 200 and srv.block_status("X") == "pending",
              "X 引用缺失父 Y，待定保存")
        Y = {"id": "Y", "parent": "X", "work": 1, "txs": []}
        status, body = srv.post("/blocks", Y)
        expect_bad(status, body, "$.parent",
                   "Y 与占位父 X 成环，400 拒绝且不保存")
        check(srv.block_status("Y") is None, "Y 未落库")

        # 自环同样拒绝
        Z = {"id": "Z", "parent": "Z", "work": 1, "txs": []}
        status, body = srv.post("/blocks", Z)
        expect_bad(status, body, "$.parent", "自环块 Z 400")

        # ---- 非法请求矩阵：未知/重复/缺字段、类型、布尔混入 ----
        cases = [
            ({"id": "D1", "parent": "G", "work": 1, "txs": [],
              "extra": 1}, "$.extra", "未知字段"),
            ({"id": "D1", "parent": "G", "work": True,
              "txs": []}, "$.work", "布尔 work"),
            ({"id": "D1", "parent": "G", "work": 0,
              "txs": []}, "$.work", "零 work"),
            ({"id": "D1", "parent": "G", "work": 1.5,
              "txs": []}, "$.work", "浮点 work"),
            ({"id": "", "parent": "G", "work": 1,
              "txs": []}, "$.id", "空 id"),
            ({"id": "d-1", "parent": "G", "work": 1,
              "txs": []}, "$.id", "含连字符 id"),
            ({"id": "G", "parent": "G", "work": 1,
              "txs": []}, "$.id", "重定义 G"),
            ({"id": "D1", "parent": "G", "work": 1,
              "txs": [[[], [-1]]]}, "$.txs[0][1][0]", "负金额"),
            ({"id": "D1", "parent": "G", "work": 1,
              "txs": [[[["G", 0, 0]], []]]}, "$.txs[0][0][0]",
             "越界引用（语义外的结构合法但 G 无该输出，属语义无效，这里结构 200）"),
        ]
        # 最后一例结构合法，单独按语义无效处理
        for payload, path, label in cases[:-1]:
            status, body = srv.post("/blocks", payload)
            expect_bad(status, body, path, label)

        oob = {"id": "D1", "parent": "G", "work": 1,
               "txs": [[[["G", 0, 0]], []]]}
        status, body = srv.post("/blocks", oob)
        check(status == 200 and srv.block_status("D1") == "invalid",
              "越界引用块 200 保存但标记 invalid")

        # 缺字段与重复键（原始 JSON）
        status, body = srv.post("/blocks",
                                {"id": "D2", "parent": "G", "work": 1})
        expect_bad(status, body, "$", "缺少 txs 字段 400")
        status, body = srv.post_raw(
            "/blocks",
            '{"id":"D3","parent":"G","work":1,"txs":[],"work":2}')
        expect_bad(status, body, "$.work", "重复 work 键 400 且带路径")
        status, body = srv.post_raw("/blocks", "{not json")
        expect_bad(status, body, "$", "JSON 语法错误路径为 $")
        status, body = srv.post_raw("/blocks", "[1,2,3]")
        expect_bad(status, body, "$", "非对象请求体 400")

        # fetch 参数校验
        status, body = srv.post("/fetch", {"after": -1})
        expect_bad(status, body, "$.after", "after 为负 400")
        status, body = srv.post("/fetch", {"after": True})
        expect_bad(status, body, "$.after", "after 为布尔 400")
        status, body = srv.post("/fetch", {"after": 9999})
        expect_bad(status, body, "$.after", "after 超过当前版本 400")
        status, body = srv.post("/fetch", {"after": 0, "tail": 1})
        expect_bad(status, body, "$.tail", "fetch 未知字段 400")
        status, body = srv.post("/fetch", {})
        expect_bad(status, body, "$", "fetch 缺 after 400")

        # after 等于当前版本：返回空批次
        cur = srv.get("/snapshot")[1]["version"]
        status, fetched = srv.post("/fetch", {"after": cur})
        check(status == 200 and fetched["batches"] == []
              and fetched["version"] == cur, "after=当前版本返回空批")

        # ---- 并发串行等价：20 个互不冲突的块并发提交，最终全部 valid ----
        # 挂在高 work 父 R 下，保证这些同 work 块之间的平局决定全局 tip
        blocks = [{"id": "P%02d" % i, "parent": "R", "work": 4, "txs": []}
                  for i in range(20)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda b: srv.post("/blocks", b),
                                    blocks))
        check(all(r[0] == 200 for r in results), "20 个并发收块全部 200")
        check(all(srv.block_status(b["id"]) == "valid" for b in blocks),
              "并发提交后所有块状态一致为 valid（串行等价）")
        check(srv.get("/snapshot")[1]["tip"] == "P00",
              "等 work 平局取 ASCII 最小 id（P00）")

        # 未知路由与方法
        import urllib.request
        try:
            urllib.request.urlopen(srv.base + "/nope", timeout=5)
            check(False, "未知路径应 404")
        except urllib.error.HTTPError as exc:
            check(exc.code == 404, "未知路径 404")
    finally:
        srv.stop()
    finish("父晚到/分叉撤回")


if __name__ == "__main__":
    main()
