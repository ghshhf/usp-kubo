# USP - Unified Storage Platform

<div align="center">

[![Rust](https://img.shields.io/badge/Rust-1.76%2B-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-MIT%2FApache--2.0-blue.svg)](./LICENSE)
[![CI](https://github.com/ghshhf/usp-kubo/workflows/CI/badge.svg)](https://github.com/ghshhf/usp-kubo/actions)

**一个统一的存储平台，通过单一 API 将数据存储到多种后端（本地、P2P、云存储、去中心化存储）。**

[English](./README.md) | [中文](./README_zh.md)

</div>

---

## 特性

- **多后端支持** - 本地文件系统、libp2p Kademlia DHT、S3 兼容存储、IPFS
- **统一 API** - 通过 `StorageHub` 接口操作所有后端，无需关心底层实现
- **策略引擎** - 基于键名、文件大小、标签自动选择最优后端
- **离线降级** - 去中心化存储支持网络不可用时本地缓存
- **CLI 工具** - 简洁的命令行界面管理存储
- **配置灵活** - 支持 `.usp.toml` 配置文件和环境变量覆盖
- **S3 兼容** - 完整 AWS SigV4 签名，支持 MinIO 等 S3 兼容服务

---

## 快速开始

### 安装

```bash
# 从源码编译
git clone https://github.com/ghshhf/usp-kubo.git
cd usp-kubo
cargo build --release

# 二进制文件位于 target/release/usp
```

### 基本使用

```bash
# 初始化存储（默认使用本地后端）
./usp init

# 存储文件
./usp store mykey /path/to/file.txt

# 读取文件
./usp get mykey /path/to/output.txt

# 列出所有键
./usp list

# 查看统计
./usp stats

# 删除键
./usp delete mykey
```

### 使用其他后端

```bash
# 初始化 P2P 后端
USP_P2P_ENABLED=true ./usp init --backend p2p

# 初始化 S3 后端（需要设置环境变量）
export USP_S3_ENDPOINT=https://s3.amazonaws.com
export USP_S3_REGION=us-east-1
export USP_S3_BUCKET=my-bucket
export USP_S3_ACCESS_KEY=AKIAIOSFODNN7EXAMPLE
export USP_S3_SECRET_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
./usp init --backend s3

# 初始化 IPFS 去中心化后端
./usp init --backend decentralized
```

---

## 架构

```
┌─────────────────────────────────────────────────────────────┐
│                         StorageHub                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Router    │  │   Policy    │  │  Pin/Unpin Tracker  │ │
│  │             │  │   Engine     │  │                     │ │
│  └──────┬──────┘  └──────┬──────┘  └─────────────────────┘ │
│         │                │                                  │
│         ▼                ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              BackendRouter (策略路由)                  │  │
│  └──────────────────────────────────────────────────────┘  │
│                              │                              │
│         ┌────────────────────┼────────────────────┐        │
│         ▼                    ▼                    ▼        │
│  ┌────────────┐      ┌────────────┐      ┌────────────┐ │
│  │   Local    │      │    P2P     │      │  CloudS3   │ │
│  │  Backend   │      │  Backend   │      │  Backend   │ │
│  └────────────┘      └────────────┘      └────────────┘ │
│                             │                    │        │
│                             ▼                    ▼        │
│                      ┌────────────┐      ┌────────────┐  │
│                      │ Decentralized│     │            │  │
│                      │  (IPFS)     │      │            │  │
│                      └────────────┘      └────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 核心组件

| 模块 | 文件 | 描述 |
|------|------|------|
| `StorageHub` | `hub.rs` | 主入口，协调各组件，管理后端注册 |
| `BackendRouter` | `router.rs` | 路由决策，选择最优后端 |
| `PolicyEngine` | `policy.rs` | 策略规则引擎，匹配条件后返回后端类型 |
| `LocalBackend` | `backends/local.rs` | 本地文件系统存储 |
| `P2PBackend` | `backends/p2p.rs` | libp2p Kademlia DHT 分布式存储 |
| `CloudS3Backend` | `backends/cloud.rs` | AWS S3 / MinIO 兼容存储 |
| `DecentralizedStorage` | `backends/decentralized.rs` | IPFS HTTP API 去中心化存储 |

---

## 配置

### 配置文件 (.usp.toml)

在项目根目录创建 `.usp.toml`：

```toml
[storage]
data_dir = ".usp-data"

[backends.local]
enabled = true
data_dir = ".usp-data"

[backends.p2p]
enabled = false
listen_addresses = ["/ip4/0.0.0.0/tcp/0"]

[backends.s3]
enabled = false
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
bucket = "my-bucket"
# access_key_id 和 secret_access_key 建议通过环境变量设置

[backends.decentralized]
enabled = false
gateway_url = "https://ipfs.io/ipfs/"
api_url = "http://127.0.0.1:5001"

[policy]
default_backend = "local"
```

### 环境变量

| 变量 | 描述 | 默认值 |
|------|------|--------|
| `USP_DATA_DIR` | 数据目录 | `.usp-data` |
| `USP_S3_ENDPOINT` | S3 端点 | - |
| `USP_S3_REGION` | AWS 区域 | `us-east-1` |
| `USP_S3_BUCKET` | S3 桶名 | - |
| `USP_S3_ACCESS_KEY` | 访问密钥 | - |
| `USP_S3_SECRET_KEY` | 秘密密钥 | - |
| `USP_IPFS_GATEWAY_URL` | IPFS 网关 | `https://ipfs.io/ipfs/` |
| `USP_IPFS_API_URL` | IPFS API | `http://127.0.0.1:5001` |

---

## API 示例

### Rust 使用

```rust
use usp_core::{StorageHub, StorageOptions, BackendConfig};
use usp_core::backends::{LocalBackend, StorageBackend};
use std::sync::Arc;
use bytes::Bytes;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 创建存储 Hub
    let hub = StorageHub::new();

    // 注册本地后端
    let local = Arc::new(LocalBackend::new("./data".into()));
    local.init(BackendConfig::Default).await?;
    hub.register_backend(local.clone()).await;

    // 存储数据
    let receipt = hub.put(
        "my-file",
        Bytes::from("Hello, USP!"),
        StorageOptions::default(),
    ).await?;

    println!("Stored: {} ({} bytes) on {:?}",
        receipt.content_hash,
        receipt.size_bytes,
        receipt.backend
    );

    // 读取数据
    if let Some(data) = hub.get("my-file").await? {
        println!("Retrieved: {}", String::from_utf8_lossy(&data));
    }

    // 查看统计
    let stats = hub.stat().await?;
    for (backend, stat) in &stats.backends {
        println!("{:?}: {} items, {} bytes used",
            backend, stat.item_count, stat.used_space);
    }

    Ok(())
}
```

### 策略路由

```rust
use usp_core::policy::PlacementRule;
use usp_core::types::StorageOptions;

let rule = PlacementRule {
    key_pattern: "cache/*".into(),
    backend_hint: Some(BackendType::Local),
    required_tags: [(("tier", "hot"))].into(),
    min_size: None,
    max_size: Some(1024 * 1024), // < 1MB
    priority: 10,
};

// 检查是否匹配
let opts = StorageOptions {
    tags: [(("tier", "hot"))].into(),
    ..Default::default()
};

if rule.matches("cache/test.txt", &opts, 512) {
    // 规则匹配
}
```

---

## 存储后端详情

### LocalBackend
- 存储到本地文件系统
- 使用 `libc::statvfs` 获取真实磁盘空间
- 支持目录递归遍历

### P2PBackend
- 基于 libp2p 0.53 Kademlia DHT
- 支持节点发现和内容路由
- 后台任务管理 Swarm 生命周期

### CloudS3Backend
- 完整 AWS SigV4 签名实现
- 支持 MinIO 等 S3 兼容服务
- 支持自定义端点和路径前缀
- `init` 时验证连接性

### DecentralizedStorage
- IPFS HTTP API 集成
- 离线降级：IPFS 不可用时本地缓存
- CID 映射持久化到 JSON 文件
- 从 IPFS Gateway 检索数据

---

## 测试

```bash
# 运行所有测试
cargo test --workspace

# 运行特定 crate 的测试
cargo test --package usp-core

# 运行带日志的测试
RUST_LOG=debug cargo test

# 运行 clippy 检查
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

---

## 项目结构

```
usp-kubo/
├── Cargo.toml              # Workspace 配置
├── usp-core/              # 核心库
│   ├── src/
│   │   ├── lib.rs         # 入口，导出公共 API
│   │   ├── hub.rs         # StorageHub 主协调器
│   │   ├── router.rs      # 路由决策
│   │   ├── policy.rs      # 策略引擎
│   │   ├── config.rs      # 配置文件加载
│   │   ├── types.rs       # 类型定义
│   │   ├── error.rs       # 错误类型
│   │   └── backends/      # 存储后端实现
│   │       ├── mod.rs
│   │       ├── local.rs
│   │       ├── p2p.rs
│   │       ├── cloud.rs
│   │       └── decentralized.rs
│   └── tests/
│       └── integration_tests.rs
└── usp-cli/               # CLI 工具
    ├── src/
    │   └── main.rs
    └── Cargo.toml
```

---

## 依赖

### 运行时

| 依赖 | 版本 | 用途 |
|------|------|------|
| `tokio` | 1.x | 异步运行时 |
| `libp2p` | 0.53 | P2P 网络 |
| `reqwest` | 0.11 | HTTP 客户端 |
| `chrono` | 0.4 | 时间处理 |
| `tracing` | 0.1 | 日志追踪 |

### 开发

| 依赖 | 用途 |
|------|------|
| `cargo clippy` | Lint 检查 |
| `cargo fmt` | 代码格式化 |
| `cargo test` | 单元/集成测试 |

---

## 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

---

## 许可证

本项目采用 MIT 或 Apache-2.0 许可证，详见 [LICENSE](./LICENSE) 文件。
