# h2fwd — 本机 HTTP/2 帧转发练习器

一个**纯 Python 标准库**实现的 HTTP/2（h2c 明文 prior-knowledge）练习项目：
受控协议对端 + 帧转发器，用于观察和验证流控窗口、慢读者隔离、单流重置与
窗口临近上限时的行为。无第三方依赖，Python 3.11+ 即可运行。

## 练习目标与对应实现

| 练习目标 | 实现位置 | 验证方式 |
| --- | --- | --- |
| 慢读的流不能耗尽其他流的窗口 | 每流独立接收窗口 + 有界缓冲（默认 32 KiB），慢读者只占自己的流窗口，连接窗口不动 | `isolation` 场景 |
| 单流重置释放该流缓冲、保留连接 | `H2Forwarder.reset_stream()`：发 RST_STREAM、清空缓冲、丢弃字节经流 0 的 WINDOW_UPDATE 退回连接窗口，并容忍在途 DATA/HEADERS | `reset` 场景 |
| 窗口临近上限的正确处理 | 所有窗口更新做**饱和运算**：增量夹到剩余空间，余额排队；对端溢出更新回 `GOAWAY(FLOW_CONTROL_ERROR)` | `nearlimit` 场景 + 单元测试 |
| 展示并行下载的窗口与进度 | `dashboard.py`：TTY 下 ANSI 实时面板，非 TTY 下周期快照 | 所有场景默认开启 |

## 架构

```
┌──────────────┐   h2c/TCP（一条连接，多流复用）   ┌────────────────┐
│  H2Forwarder │ ───────────────────────────────▶ │ ControlledPeer │
│  （客户端）  │ ◀─────────────────────────────── │ （受控服务端） │
└──────────────┘   HEADERS/DATA/WINDOW_UPDATE/     └────────────────┘
                   RST_STREAM/SETTINGS/PING/GOAWAY
```

- `frames.py`：RFC 7540 帧线格式（9 字节帧头）、设置/窗口/GOAWAY 编解码、协议异常
- `hpack.py`：RFC 7541 最小 HPACK（静态表、整数/字符串原语、动态表维护；拒绝 Huffman）
- `flow.py`：`ReceivingWindow` / `SendingWindow` 纯簿记，全部饱和、可单测
- `peer.py`：受控对端，双窗口（连接 + 流）驱动的发送协程、确定性载荷、RST 取消
- `forwarder.py`：转发器主体，读循环分发、每流缓冲与消费协程、重置与窗口返还
- `dashboard.py`：实时窗口/进度面板
- `scenarios.py`：三个练习场景的编排与断言

### 流控模型

- **两级窗口**：连接接收窗口（初始 64 KiB，上限 2³¹−1）与每流接收窗口
  （初始 32 KiB）。DATA 先同时占两级额度再入缓冲；上层消费后由
  `take_update()` 产出 WINDOW_UPDATE 增量。
- **慢读者隔离**：慢流的消费协程睡在 sink 上时，读循环仍在服务其他流；
  慢流窗口归零后对端在该流上停发，它在连接窗口里占的字节在被消费后立即
  经流 0 返还，其他流始终有额度可用。
- **RST 的窗口处理**：流级窗口随流消亡；重置时缓冲中已占连接额度但尚未
  返还的字节，通过流 0 的连接级 WINDOW_UPDATE 退回。重置流上迟到的
  DATA/HEADERS 按 RFC 7540 §5.1 容忍并丢弃。
- **临近上限**：`ReceivingWindow.take_update()` 的增量为
  `min(pending, limit − available)`，绝不越过 2³¹−1；未返还余额保留在
  `pending`，等 DATA 腾出空间后继续返还。对端任何导致溢出的更新都触发
  `FLOW_CONTROL_ERROR`。

## 使用

```bash
# 三个场景依次演示（默认带实时仪表盘）
python3 -m h2fwd all

# 单独运行
python3 -m h2fwd isolation     # 慢读流 + 3 条快流
python3 -m h2fwd reset         # 中途 RST 单流，再开新流证明连接存活
python3 -m h2fwd nearlimit     # 初始窗口 20 亿，饱和夹取 + 溢出拒绝探针
python3 -m h2fwd serve --port 8443   # 只启动受控对端，供外部 h2c 客户端连接

# 非交互环境只打印最终快照
python3 -m h2fwd all --no-dashboard
```

落盘载荷在 `runtime-output/`，内容为确定性字节
（第 i 字节 = `(i*7 + stream_id) & 0xFF`），场景结束时逐字节校验。

## 测试

```bash
python3 -m unittest discover -v
```

- `tests/test_protocol.py`：帧线格式、HPACK 编解码、窗口饱和/溢出/负 delta
- `tests/test_integration.py`：真实 TCP 链路上的三个场景、对端 GOAWAY 裁决、
  RST 仅影响目标流、重置时连接窗口返还
