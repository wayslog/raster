# RasterKV

RasterKV 是 Rust 嵌入式键值存储，以固定 FASTER C++ FasterKv 为功能与行为基线，采用独立磁盘格式。已实现共享存储与线程会话、四种操作、混合日志及磁盘 Pending、三类检查点与恢复、在线索引扩容、读缓存、物理扫描、压缩回收和自动维护。

**P0—P8 已验收，P9 总验收仍进行中。** 上游原始随机轨迹仍有两类删除返回差异待确定最终契约；旧内存原型对照的性能复核已完成，但吞吐和部分尾延迟差距明确保留，不承诺达到旧原型或 C++ 的性能。

- [第一期实现计划地图](docs/14-RasterKV第一期实现计划地图.md)：任务状态与后续执行入口。
- [一期验收报告](docs/acceptance/一期验收报告.md)：上游对照、完整历史、故障及尚存差异。
- [性能复核与限制](docs/acceptance/P9.2性能复核结论.md)：完整同机比较及处理依据。
- [公开接口](docs/10-RasterKV公开接口.md)与[实际使用流程](docs/16-公开接口使用流程.md)：签名、结果、错误和可运行示例。
- [配置与诊断](docs/15-配置与诊断.md)、[架构](docs/09-RasterKV模块设计.md)、[模块接入地图](docs/13-模块基础骨架.md)、[状态协议](docs/11-RasterKV内存与状态协议.md)。
- [文档索引](docs/README.md)、[一期范围及实际依赖](docs/05-RasterKV第一阶段与支持库选型.md)、[领域术语](CONTEXT.md)。

工具链和 MSRV 为 **Rust 1.98.1**。正式基础文件后端覆盖 Linux 与 macOS；Windows、io_uring、远端设备、F2/ColdIndex、跨 store 压缩及外部运行时 async 外观后置。全部特性可编译不表示 io_uring 可用，Cargo 当前禁止发布 crates.io。

```sh
cargo test --locked --all-features
cargo run --locked --release --example memory
cargo run --locked --release --example interface
cargo run --locked --release --example disk_lifecycle
cargo run --locked --release --example automatic
```

内存示例使用 Null 后端，不提供磁盘溢出或持久化。磁盘示例使用本次独立临时目录；显式传入的目录必须尚不存在，成功后保留。完整工程检查含格式、Clippy、单元/集成测试、示例及严格 rustdoc，由 Linux/macOS 默认、config-toml、全部特性六组 CI 验证；正式验收另运行故障、资源、上游和性能矩阵。

请求 Ready/Pending 表示结果交付方式；完成、排空和关闭均不代替检查点持久化。Index 检查点不承诺会话持久化进度，Log 须绑定已提交的索引材料；恢复成功后原会话需显式续接。扫描返回拥有型物理记录，可含旧版本与重复键；日志跨度不是有效键数。v1 独立格式与恢复保留规则见[格式规范](docs/acceptance/磁盘格式规范.md)和[恢复安全记录](docs/acceptance/P5.4恢复安全交付记录.md)。
