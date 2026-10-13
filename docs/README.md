# RasterKV 文档

RasterKV 是 FASTER 的 Rust 实现，按 FASTER C++ 契约重写，性能目标为不低于 C++。

| 文档 | 内容 |
| --- | --- |
| [项目结构与构建](01-项目结构与构建.md) | Rust 构建、模块、示例与测试入口，以及 C++ 调研记录 |
| [核心抽象与架构](02-核心抽象与架构.md) | 索引、变长混合日志、设备、会话、epoch、检查点与协作压缩 |
| [功能与 API 契约](03-功能与API契约.md) | 当前 Rust trait、操作选项、维护与 TOML 配置，以及 C++ API 对照 |
| [Rust 改写关键约束](04-Rust改写关键约束.md) | 从 C++ 源码归纳的移植风险与需保留的语义 |
| [FASTER 契约重写计划](05-FASTER契约重写计划.md) | 当前执行计划：模块结构、阶段 A–E、验收门槛 |
| [变长键值与收尾计划](06-变长键值与收尾计划.md) | V0–V5、D+、E 的范围、执行记录与验收状态 |
| [ADR 0001](adr/0001-adopt-faster-cpp-contract.md) | 采用 FASTER C++ 契约并重写引擎的决策 |
| [ADR 0002](adr/0002-variable-length-records.md) | KeyContext、ValueSlot、变长记录与默认操作快路径 |

01–04 首先说明当前 Rust 实现，再保留对 FASTER C++ 固定提交 `321d872eabda6a0345c8bd76419f89723ed864ae` 的历史源码调研；其中 `cc/` 路径相对于 FASTER 根目录。
