# RasterKV 文档

RasterKV 是 FASTER 的 Rust 实现，按 FASTER C++ 契约重写，性能目标为不低于 C++。

| 文档 | 内容 |
| --- | --- |
| [项目结构与构建](01-项目结构与构建.md) | FASTER C++ 目录、构建目标、平台依赖、示例与测试入口 |
| [核心抽象与架构](02-核心抽象与架构.md) | 索引、混合日志、设备、会话、异步上下文、epoch、检查点 |
| [功能与 API 契约](03-功能与API契约.md) | FASTER C++ 公开 API、参数、状态码与回调契约 |
| [Rust 改写关键约束](04-Rust改写关键约束.md) | 从 C++ 源码归纳的移植风险与需保留的语义 |
| [FASTER 契约重写计划](05-FASTER契约重写计划.md) | 当前执行计划：模块结构、阶段 A–E、验收门槛 |
| [ADR 0001](adr/0001-adopt-faster-cpp-contract.md) | 采用 FASTER C++ 契约并重写引擎的决策 |

01–04 是对 FASTER C++ 固定提交 `321d872eabda6a0345c8bd76419f89723ed864ae` 的源码调研，`cc/` 路径相对于 FASTER 根目录。
