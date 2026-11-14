#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验收四：POST /trace 混币来源追踪。

覆盖：题目示例（来源合并拆分）、销毁槽、反向约束（下游 known 影响上游
UTXO 界）、交错收块（待定/分叉撤回后定链查询）、多 seed 耦合、已知约束
取交集、不可行判定、已花费引用、常量行（未花费铸造/零金额）、400/409
矩阵、405/404、查询无副作用与重启一致性。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from acceptance_helper import Server, check, expect_bad, finish  # noqa: E402


def post(srv, block):
    status, body = srv.post("/blocks", block)
    assert status == 200, (status, body)
    return body


def trace(srv, base, seeds, known):
    return srv.post("/trace", {"base": base, "seeds": seeds,
                               "known": known})


def version(srv):
    return srv.get("/snapshot")[1]["version"]


def main():
    srv = Server()
    srv.start()
    try:
        # ---- 空链：只有创世块 ----
        status, body = trace(srv, 0, [], [])
        check(status == 200 and body == {"base": 0, "status": "ok",
                                         "rows": []},
              "空链 trace：ok 且 rows 为空")
        status, body = trace(srv, 1, [], [])
        check(status == 409 and body.get("path") == "$.base",
              "base 与当前版本不符返回 409")

        # ---- 题目示例：来源2 与 其他来源2 合并再拆为 3 和 1 ----
        post(srv, {"id": "M1", "parent": "G", "work": 1,
                   "txs": [[[], [2]]]})                 # M1.0.0 = 2 (seed0)
        post(srv, {"id": "M2", "parent": "M1", "work": 1,
                   "txs": [[[], [2]]]})                 # M2.0.0 = 2 (其他)
        post(srv, {"id": "T", "parent": "M2", "work": 1,
                   "txs": [[[["M1", 0, 0], ["M2", 0, 0]], [3, 1]]]})
        v = version(srv)
        status, body = trace(srv, v, [["M1", 0, 0]], [])
        check(status == 200 and body["base"] == v
              and body["status"] == "ok", "示例链 trace 返回 ok")
        check(body["rows"] == [[["T", 0, 0], [[1, 2]]],
                               [["T", 0, 1], [[0, 1]]]],
              "来源2在金额3中范围[1,2]，在金额1中范围[0,1]（题目示例）")

        # ---- 结构非法 400 矩阵 ----
        cases = [
            ({"seeds": [], "known": []}, "$", "缺 base 字段"),
            ({"base": v, "known": []}, "$", "缺 seeds 字段"),
            ({"base": v, "seeds": []}, "$", "缺 known 字段"),
            ({"base": v, "seeds": [], "known": [], "x": 1}, "$.x",
             "未知字段"),
            ({"base": True, "seeds": [], "known": []}, "$.base",
             "布尔 base"),
            ({"base": -1, "seeds": [], "known": []}, "$.base", "负 base"),
            ({"base": 1.5, "seeds": [], "known": []}, "$.base",
             "浮点 base"),
            ({"base": "3", "seeds": [], "known": []}, "$.base",
             "字符串 base"),
            ({"base": v, "seeds": {}, "known": []}, "$.seeds",
             "seeds 非数组"),
            ({"base": v, "seeds": [["M1", 0]], "known": []}, "$.seeds[0]",
             "seed 引用二元"),
            ({"base": v, "seeds": [["M1", 0, True]], "known": []},
             "$.seeds[0][2]", "seed 输出号布尔"),
            ({"base": v, "seeds": [["M1", -1, 0]], "known": []},
             "$.seeds[0][1]", "seed 交易号为负"),
            ({"base": v, "seeds": [["m-1", 0, 0]], "known": []},
             "$.seeds[0][0]", "seed 块 id 含连字符"),
            ({"base": v, "seeds": [["M1", 0, 0], ["M1", 0, 0]],
              "known": []}, "$.seeds[1]", "seed 引用重复"),
            ({"base": v, "seeds": [], "known": {}}, "$.known",
             "known 非数组"),
            ({"base": v, "seeds": [], "known": [[["T", 0, 0], 0, 0]]},
             "$.known[0]", "known 项三元"),
            ({"base": v, "seeds": [], "known": [[["T", 0], 0, 0, 0]]},
             "$.known[0][0]", "known 引用二元"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], True, 0, 0]]}, "$.known[0][1]",
             "来源号布尔"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], -1, 0, 0]]}, "$.known[0][1]",
             "来源号为负"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], 1, 0, 0]]}, "$.known[0][1]",
             "来源号超出 seeds 范围"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], 0, -1, 0]]}, "$.known[0][2]",
             "下界为负"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], 0, 0, True]]}, "$.known[0][3]",
             "上界布尔"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], 0, 2, 1]]}, "$.known[0][3]",
             "下界大于上界"),
        ]
        for payload, path, label in cases:
            status, body = srv.post("/trace", payload)
            expect_bad(status, body, path, "trace 400: " + label)

        # 语义 400：seed 非铸造输出 / 不在链内；known 引用越界、上界超金额
        sem_cases = [
            ({"base": v, "seeds": [["T", 0, 0]], "known": []},
             "$.seeds[0]", "seed 引用非铸造输出"),
            ({"base": v, "seeds": [["ZZ", 0, 0]], "known": []},
             "$.seeds[0]", "seed 引用不在链内"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["ZZ", 0, 0], 0, 0, 0]]}, "$.known[0][0]",
             "known 引用不在链内"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 9, 0], 0, 0, 0]]}, "$.known[0][0]",
             "known 交易号越界"),
            ({"base": v, "seeds": [["M1", 0, 0]],
              "known": [[["T", 0, 0], 0, 0, 4]]}, "$.known[0][3]",
             "known 上界超过输出金额 3"),
        ]
        for payload, path, label in sem_cases:
            status, body = srv.post("/trace", payload)
            expect_bad(status, body, path, "trace 语义 400: " + label)

        # 409：base 落后于当前版本
        status, body = trace(srv, v - 1, [], [])
        check(status == 409 and body.get("path") == "$.base",
              "base 落后当前版本返回 409")

        # 重复键与语法错误
        status, body = srv.post_raw(
            "/trace", '{"base":%d,"seeds":[],"known":[],"seeds":[]}' % v)
        expect_bad(status, body, "$.seeds", "trace 重复 seeds 键 400")
        status, body = srv.post_raw("/trace", "{not json")
        expect_bad(status, body, "$", "trace JSON 语法错误路径 $")
        status, body = srv.post_raw("/trace", "[1,2]")
        expect_bad(status, body, "$", "trace 非对象请求体 400")

        # 方法与路由
        import urllib.error
        import urllib.request
        try:
            urllib.request.urlopen(srv.base + "/trace", timeout=5)
            check(False, "GET /trace 应 405")
        except urllib.error.HTTPError as exc:
            check(exc.code == 405
                  and exc.headers.get("Allow") == "POST",
                  "GET /trace 返回 405 且 Allow: POST")

        # trace 查询无副作用
        snap_before = srv.get("/snapshot")[1]
        trace(srv, v, [["M1", 0, 0]], [])
        check(srv.get("/snapshot")[1] == snap_before,
              "trace 查询不改变链状态")

        # ---- 销毁槽：输入 15、输出 9、销毁 6 ----
        srv2 = Server()
        srv2.start()
        try:
            post(srv2, {"id": "M", "parent": "G", "work": 1,
                        "txs": [[[], [10]]]})           # seed0: 10
            post(srv2, {"id": "N", "parent": "M", "work": 1,
                        "txs": [[[], [5]]]})            # 其他: 5
            post(srv2, {"id": "T", "parent": "N", "work": 1,
                        "txs": [[[["M", 0, 0], ["N", 0, 0]], [9]]]})
            v2 = version(srv2)
            status, body = trace(srv2, v2, [["M", 0, 0]], [])
            check(body["rows"] == [[["T", 0, 0], [[4, 9]]]],
                  "销毁槽吸收来源：seed 在金额9中范围[4,9]（销毁6）")
            # known 钉死销毁槽之外的量：T.0.0 的 seed = 9 -> 可行
            status, body = trace(srv2, v2, [["M", 0, 0]],
                                 [[["T", 0, 0], 0, 9, 9]])
            check(body["status"] == "ok"
                  and body["rows"] == [[["T", 0, 0], [[9, 9]]]],
                  "known 钉到上界可行")
            # known 超过守恒允许的范围 -> 不可行
            status, body = trace(srv2, v2, [["M", 0, 0]],
                                 [[["T", 0, 0], 0, 0, 3]])
            check(body["status"] == "infeasible" and body["rows"] == [],
                  "known 与销毁槽容量冲突 -> infeasible 且 rows 为空")
        finally:
            srv2.stop()

        # ---- 反向约束：下游 known 改变上游 UTXO 的界 ----
        srv3 = Server()
        srv3.start()
        try:
            post(srv3, {"id": "M", "parent": "G", "work": 1,
                        "txs": [[[], [5]]]})             # seed0: 5
            post(srv3, {"id": "N", "parent": "M", "work": 1,
                        "txs": [[[], [5]]]})             # 其他: 5
            post(srv3, {"id": "T1", "parent": "N", "work": 1,
                        "txs": [[[["M", 0, 0], ["N", 0, 0]], [6, 4]]]})
            post(srv3, {"id": "T2", "parent": "T1", "work": 1,
                        "txs": [[[["T1", 0, 0]], [2, 4]]]})
            v3 = version(srv3)
            status, body = trace(srv3, v3, [["M", 0, 0]], [])
            check(body["rows"] == [[["T1", 0, 1], [[0, 4]]],
                                   [["T2", 0, 0], [[0, 2]]],
                                   [["T2", 0, 1], [[0, 4]]]],
                  "无 known 时各 UTXO 来源界")
            # 下游 T2.0.0 钉为 2 -> 上游 T1.0.0 的 seed >= 2 -> T1.0.1 <= 3
            status, body = trace(srv3, v3, [["M", 0, 0]],
                                 [[["T2", 0, 0], 0, 2, 2]])
            check(body["rows"] == [[["T1", 0, 1], [[0, 3]]],
                                   [["T2", 0, 0], [[2, 2]]],
                                   [["T2", 0, 1], [[0, 3]]]],
                  "反向约束：下游 known 钉死改变上游 UTXO 上界")
            # 已花费输出也可作 known 引用
            status, body = trace(srv3, v3, [["M", 0, 0]],
                                 [[["T1", 0, 0], 0, 3, 3]])
            check(body["rows"] == [[["T1", 0, 1], [[2, 2]]],
                                   [["T2", 0, 0], [[0, 2]]],
                                   [["T2", 0, 1], [[1, 3]]]],
                  "known 引用已花费输出并向上游/下游传播")
            # 重复约束取交集 [2,4]与[3,6] -> [3,4]
            status, body = trace(srv3, v3, [["M", 0, 0]],
                                 [[["T1", 0, 0], 0, 2, 4],
                                  [["T1", 0, 0], 0, 3, 6]])
            check(body["rows"] == [[["T1", 0, 1], [[1, 2]]],
                                   [["T2", 0, 0], [[0, 2]]],
                                   [["T2", 0, 1], [[1, 4]]]],
                  "重复 known 约束取交集 [2,4]与[3,6]->[3,4]")
            # 交集为空 -> 不可行
            status, body = trace(srv3, v3, [["M", 0, 0]],
                                 [[["T1", 0, 0], 0, 4, 6],
                                  [["T1", 0, 0], 0, 0, 2]])
            check(body["status"] == "infeasible" and body["rows"] == [],
                  "重复约束交集为空 -> infeasible")
        finally:
            srv3.stop()

        # ---- 交错收块：待定 + 分叉撤回后定链查询 ----
        srv4 = Server()
        srv4.start()
        try:
            post(srv4, {"id": "M", "parent": "G", "work": 1,
                        "txs": [[[], [4]]]})            # seed0: 4
            post(srv4, {"id": "N", "parent": "M", "work": 1,
                        "txs": [[[], [2]]]})            # 其他: 2
            post(srv4, {"id": "A", "parent": "N", "work": 1,
                        "txs": [[[["M", 0, 0], ["N", 0, 0]], [6]]]})
            # E 的父 D 未到，先待定
            status, _ = srv4.post("/blocks",
                                  {"id": "E", "parent": "D", "work": 1,
                                   "txs": [[[["B", 0, 1]], [3]]]})
            check(status == 200 and srv4.block_status("E") == "pending",
                  "交错收块：E 父未到先待定")
            # B 与 A 竞争（更高 work），触发分叉撤回
            snap = post(srv4, {"id": "B", "parent": "N", "work": 2,
                               "txs": [[[["M", 0, 0], ["N", 0, 0]],
                                        [3, 3]]]})
            check(snap["tip"] == "B", "分叉切换：B 成为 tip")
            # 中间态（E 仍待定、D 未到）：规范链只到 B
            v_mid = version(srv4)
            status, body = trace(srv4, v_mid, [["M", 0, 0]], [])
            check(body["rows"] == [[["B", 0, 0], [[1, 3]]],
                                   [["B", 0, 1], [[1, 3]]]],
                  "交错中间态：待定块不影响当前规范链的 trace")
            # D 到达，E 级联生效
            snap = post(srv4, {"id": "D", "parent": "B", "work": 1,
                               "txs": [[[["B", 0, 0]], [3]]]})
            check(srv4.block_status("E") == "valid"
                  and snap["tip"] == "E", "D 到达后 E 级联生效")
            # 中间态版本已过期
            status, body = trace(srv4, v_mid, [["M", 0, 0]], [])
            check(status == 409, "链演进后旧 base 返回 409")
            v4 = version(srv4)
            status, body = trace(srv4, v4, [["M", 0, 0]], [])
            check(body["rows"] == [[["D", 0, 0], [[1, 3]]],
                                   [["E", 0, 0], [[1, 3]]]],
                  "交错收块后定链：分叉+待定后的 UTXO 来源界")
            # 重启后 trace 结果一致（链状态从库中恢复）
            srv4.restart()
            status, body2 = trace(srv4, v4, [["M", 0, 0]], [])
            check(body2 == body, "重启后 trace 结果一致")
        finally:
            srv4.stop()

        # ---- 多 seed 耦合 + 常量行 + 空 seeds ----
        srv5 = Server()
        srv5.start()
        try:
            post(srv5, {"id": "M1", "parent": "G", "work": 1,
                        "txs": [[[], [3]]]})            # seed0: 3
            post(srv5, {"id": "M2", "parent": "M1", "work": 1,
                        "txs": [[[], [3]]]})            # seed1: 3
            post(srv5, {"id": "T", "parent": "M2", "work": 1,
                        "txs": [[[["M1", 0, 0], ["M2", 0, 0]], [4, 2]]]})
            post(srv5, {"id": "Z", "parent": "T", "work": 1,
                        "txs": [[[["T", 0, 1]], [0, 2]]]})
            v5 = version(srv5)
            # 多 seed：边容量迫使两种来源在金额4中互补（x+y=4）
            status, body = trace(srv5, v5,
                                 [["M1", 0, 0], ["M2", 0, 0]], [])
            check(body["rows"] == [
                [["T", 0, 0], [[1, 3], [1, 3]]],
                [["Z", 0, 0], [[0, 0], [0, 0]]],
                [["Z", 0, 1], [[0, 2], [0, 2]]],
            ], "多 seed 耦合 + 零金额常量行")
            # 空 seeds：每行 per-seed 数组为空
            status, body = trace(srv5, v5, [], [])
            check(status == 200 and body["status"] == "ok"
                  and all(r[1] == [] for r in body["rows"])
                  and len(body["rows"]) == 3,
                  "空 seeds：行内来源数组为空")
            # 未花费铸造输出是常量行
            status, body = trace(srv5, v5, [["M1", 0, 0]], [])
            utxo_map = {tuple(r[0]): r[1] for r in body["rows"]}
            check(utxo_map[("T", 0, 0)] == [[1, 3]]
                  and utxo_map[("Z", 0, 0)] == [[0, 0]]
                  and utxo_map[("Z", 0, 1)] == [[0, 2]],
                  "单 seed 下常量与变量行混合")
        finally:
            srv5.stop()

        # ---- 铸造输出上的 known 与未花费铸造常量 ----
        srv6 = Server()
        srv6.start()
        try:
            post(srv6, {"id": "M", "parent": "G", "work": 1,
                        "txs": [[[], [7]]]})            # seed0: 7 未花费
            post(srv6, {"id": "N", "parent": "M", "work": 1,
                        "txs": [[[], [3]]]})            # 其他: 3 未花费
            v6 = version(srv6)
            status, body = trace(srv6, v6, [["M", 0, 0]], [])
            check(body["rows"] == [[["M", 0, 0], [[7, 7]]],
                                   [["N", 0, 0], [[0, 0]]]],
                  "未花费铸造输出：seed 行常量 [7,7]，其他铸造 [0,0]")
            status, body = trace(srv6, v6, [["M", 0, 0]],
                                 [[["M", 0, 0], 0, 7, 7]])
            check(body["status"] == "ok", "known 与铸造常量一致可行")
            status, body = trace(srv6, v6, [["M", 0, 0]],
                                 [[["M", 0, 0], 0, 0, 6]])
            check(body["status"] == "infeasible",
                  "known 与铸造常量冲突 -> infeasible")
            status, body = trace(srv6, v6, [["M", 0, 0]],
                                 [[["N", 0, 0], 0, 0, 0]])
            check(body["status"] == "ok",
                  "known 钉其他来源铸造的 seed 量为 0 可行")
        finally:
            srv6.stop()
    finally:
        srv.stop()
    finish("混币来源追踪 /trace")


if __name__ == "__main__":
    main()
