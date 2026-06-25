# USP-Kubo 升级计划 — 四周冲刺

> 目标：把 USP 从「CLI 存储工具」升级为「本地 P2P 共享空间平台」
> 起始日期：2026-06-26
> 节奏：每周一个可演示的里程碑

---

## 第一周：修 Bug + P2P 数据检索（基础可用性）

**目标**：让 P2P 后端真正能跨节点取数据，CLI 能跑通端到端流程。

### 任务清单

- [ ] **T1-1** 修复 `with_retry_config` OOM 风险  
      `router.rs` L42: `HybridCache::new(100_000_000)` → `new(1000)`  
      → 预计 10 分钟

- [ ] **T1-2** 修复 `HybridCache::get` 用 `write().await`  
      `cache.rs` L18: 改为 `self.cache.read().await.get(key).cloned()`  
      → 预计 10 分钟

- [ ] **T1-3** 【核心】实现 P2P `get` 跨节点数据拉取 ⭐⭐⭐  
      当前：`p2p.rs` `get()` 找到 providers 后直接返回 None  
      需要：定义 `/usp-kubo/fetch/1.0.0` Request/Response 协议  
      - Request：发送 key → 对方本地查 LocalBackend → 返回 Bytes  
      - Response：对方把数据通过 libp2p 直接发回来  
      → 预计 3 天

- [ ] **T1-4** 实现 `StorageOptions.replicas` 副本机制（MVP）  
      `BackendRouter::store`：根据 `replicas` 数写 N 个后端  
      `BackendRouter::retrieve`：任一后端有数据即返回  
      → 预计 1 天

- [ ] **T1-5** 策略引擎支持配置文件加载  
      `PolicyEngine::from_config()` 实现，加载 `.usp.toml` `[policy.rules]`  
      → 预计 1 天

### 本周交付物

```
✅ `usp put mykey file.txt --backend p2p` 另一台设备能 `usp get mykey` 取到
✅ `usp put mykey file.txt --replicas 2` 数据写两个后端
✅ `.usp.toml` 配置策略规则生效
```

### 本周风险

- libp2p Request/Response 协议调试耗时（Swarm 事件循环容易踩坑）
- 如果卡住，先跳过 T1-3，用 LocalBackend + S3 验证副本机制

---

## 第二周：`usp serve` 命令 + 最简 SPA

**目标**：`usp serve` 启动本地 HTTP 服务，浏览器打开能看到「共享空间」界面。

### 任务清单

- [ ] **T2-1** 新增 `usp-server` crate（axum HTTP 服务）  
      依赖：`axum` + `tokio` + `serde` + `serde_json`  
      API 设计：
      - `GET /` → serve `index.html`（SPA 静态文件）
      - `POST /api/v1/store` → `hub.put()`
      - `GET /api/v1/get/{key}` → `hub.get()`
      - `GET /api/v1/list` → `hub.list_keys()`
      - `WS /ws` → WebSocket 推送新消息通知  
      → 预计 2 天

- [ ] **T2-2** 最简 SPA（Vue 3 + TypeScript）  
      功能：消息列表 + 发送消息 + 文件上传  
      打包：build 后嵌到 `usp-server` 的 `static/` 目录  
      → 预计 3 天

- [ ] **T2-3** SPA WebSocket 连接本地节点  
      连 `ws://localhost:8080/ws`，收到新消息推送刷新列表  
      → 预计 1 天

### 本周交付物

```
✅ `usp serve` 启动，浏览器打开 localhost:8080 看到界面
✅ 在页面发一条消息，另一台设备刷新后能看到
✅ 文件上传后可通过 P2P 下载
```

### 架构决策（本周需确认）

> **SPA 怎么分发？**  
> 选项 A：SPA build 产物直接打进 `usp-server` 二进制（用 `include_dir` crate）  
> 选项 B：SPA 独立部署，通过 IPFS 分发 CID  
> **推荐 A**：简单，单二进制部署，适合本地网络场景

---

## 第三周：P2P 数据同步层（Gossipsub + CRDT）

**目标**：不需要手动 `usp get`，数据自动在设备间同步。

### 任务清单

- [ ] **T3-1** 新增 `usp-sync` crate — 同步引擎  
      依赖：`libp2p-gossipsub` + `serde_cbor`  
      设计：
      - 每个共享空间是一个 Gossipsub topic：`/usp/space/{space_id}`
      - 消息格式：`{key, value_hash, timestamp, peer_id, signature}`
      - 收到消息 → 检查本地是否有 `value_hash` 对应数据 → 没有就通过 P2P fetch  
      → 预计 3 天

- [ ] **T3-2** 本地存储用 `sled` CRDT（或 `automerge`）  
      目标：多设备同时写同一 key，能合并而不丢失数据  
      MVP：用最后写入赢（LWW），加向量时钟  
      → 预计 2 天

- [ ] **T3-3** `usp serve` 集成同步引擎  
      SPA 发消息 → 写本地 + 广播 Gossipsub → 其他设备自动同步  
      → 预计 2 天

### 本周交付物

```
✅ 设备 A 发消息，设备 B 自动出现，无需手动刷新
✅ 设备 A 掉线后重新上线，自动同步掉线期间的新消息
✅ 多设备同时发消息，数据不丢失
```

### 本周风险

- CRDT 选型：`automerge` 功能强但重，`sled` + LWW 简单但合并能力弱
- **推荐**：第一版用 LWW + 向量时钟，后续升级到 `automerge`

---

## 第四周：热点网络 + 「任意运行」MVP

**目标**：开热点 → 别人连 → 自动发现 → 可用。「应用」功能最简实现。

### 任务清单

- [ ] **T4-1** mDNS 自动发现 + 热点网络引导  
      `libp2p-mdns` 自动发现局域网设备  
      SPA 显示「在线联系人」列表  
      → 预计 1 天

- [ ] **T4-2** 【可选】互联网 Relay 支持  
      用 IPFS Relay v2 或自部署 relay 节点  
      让不在同一局域网的设备也能同步  
      → 预计 2 天（可选，视需求）

- [ ] **T4-3** 「应用」功能 MVP — WASM 插件系统  
      设计：共享空间里的「应用」= WASM 模块  
      用 `wasmer` 或 `wasmtime` 在沙箱里跑  
      SPA 里增加一个「应用商店」页面，列出可用 WASM 应用  
      → 预计 3 天（最有探索性，可延到下个月）

- [ ] **T4-4** 整体联调 + 文档  
      写 `README.md` 快速开始：「开热点 → 连 → 打开浏览器 → 开始用」  
      → 预计 1 天

### 本周交付物

```
✅ 开热点，别人连，浏览器打开 localhost:8080 直接用
✅ 自动发现联系人，无需手动配置 PeerId
✅ （可选）WASM 应用能在共享空间里跑起来
✅ 完整的「5 分钟上手」文档
```

---

## 依赖关系图

```
Week 1: P2P 数据检索 ─┐
                              ├─→ Week 2: usp serve + SPA ─┐
Week 1: 副本机制 ─────┐    │                              ├─→ Week 3: 同步引擎
                         └─→ Week 2                        │
                                            Week 3 ────────┘
                                                        ↓
                                                  Week 4: 热点网络 + WASM
```

---

## 每周演示脚本（给自己的验收标准）

### Week 1 Demo
```bash
# 设备 A
USP_P2P_ENABLED=true ./usp init --backend p2p
./usp serve  # 或 ./usp put testkey ./hello.txt --backend p2p

# 设备 B（同一网络）
USP_P2P_ENABLED=true ./usp init --backend p2p
./usp get testkey ./received.txt  # ← 必须能拿到！
cat received.txt  # 输出 "Hello from device A"
```

### Week 2 Demo
```bash
# 设备 A
./usp serve &
# 浏览器打开 http://localhost:8080
# 发一条消息：「大家好，我是 A」

# 设备 B
./usp serve &
# 浏览器打开 http://localhost:8080
# 看到 A 发的消息
```

### Week 3 Demo
```
设备 A 发消息 → 设备 B 页面自动出现新消息（无需刷新）
设备 A 掉线 10 分钟 → 重新上线 → 掉线期间的消息自动同步到 A
```

### Week 4 Demo
```
手机开热点 → 电脑连热点 → 电脑浏览器打开 localhost:8080 → 能用
（截图发给你自己，算完成）
```

---

## 工具/依赖清单（提前准备）

| 用途 | Crate | 版本 |
|------|-------|------|
| HTTP 服务 | `axum` | 0.7 |
| WebSocket | `axum-ws` / `tokio-tungstenite` | — |
| SPA 构建产物嵌入 | `include_dir` | 0.7 |
| CRDT（LWW） | `sled` + 自写向量时钟 | — |
| Gossipsub | `libp2p-gossipsub` | 0.53 |
| WASM 运行时 | `wasmer` | 4.x |

---

## 备注

- 每周五做「演示日」：必须能跑通 Demo 脚本，否则下周继续补
- P2P 功能调试困难时，先用 LocalBackend + 多进程模拟多设备
- WASM 应用系统是最有想象力的部分，但也可以第一版不做，先让用户手动传文件
