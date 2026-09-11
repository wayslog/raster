# RasterKV

RasterKV 是 raster 仓库的 Rust 嵌入式键值存储引擎。第一阶段目标是完整复刻 FASTER C++ `FasterKv` 的功能与行为，使用独立持久化文件格式；F2 等高级功能列为后续 TODO。

当前已实现创建与会话、内存四操作、混合日志与磁盘 Pending，以及 Linux/macOS 基础文件后端。Full、Index、Log 检查点已接入真实材料写入和同步提交；恢复入口和会话续接已接通，支持恢复后继续写入与再次检查点。在线扩容、读缓存、物理扫描、压缩回收和自动维护也已通过双平台交付检查。

- [第一期实现计划地图（后续执行入口）](docs/14-RasterKV第一期实现计划地图.md)
- [文档入口](docs/README.md)
- [P0 验收报告](docs/acceptance/P0验收报告.md)
- [P1.1 类型与键验收报告](docs/acceptance/P1.1验收报告.md)
- [第一阶段范围与支持库选型](docs/05-RasterKV第一阶段与支持库选型.md)
- [Rust 抽象与模块设计](docs/09-RasterKV模块设计.md)
- [领域术语](CONTEXT.md)
- [各模块基础骨架与接入说明](docs/13-模块基础骨架.md)

开发工具链与 MSRV：Rust **1.98.1**。基础检查：`cargo test --locked --all-features`。完整交付检查覆盖格式、Clippy、测试、示例和 rustdoc，并由 Linux/macOS 三组特性 CI 验证。

P0—P7 及 P8.1 已验收，下一项是 **P8.2 完整公开使用示例与 rustdoc**。检查点仅索引模式不承诺会话持久化进度；仅日志模式需要绑定本引擎已提交的索引检查点。独立磁盘格式 v1 已通过 P5.4 验收冻结。

- [读缓存交付记录](docs/acceptance/P6.2读缓存交付记录.md)
- [在线索引扩容交付记录](docs/acceptance/P6.1索引扩容交付记录.md)
- [恢复安全与格式冻结交付记录](docs/acceptance/P5.4恢复安全交付记录.md)
- [检查点交付记录](docs/acceptance/P5.2检查点交付记录.md)
- [混合日志交付记录](docs/acceptance/P4.3混合日志交付记录.md)

第一期正式验收 Linux + macOS 基础本地文件后端，Windows、io_uring 和 F2 后置。物理扫描、压缩搬迁、检查点保留集合释放及自动维护均已接通；完整配置、预分配及真实诊断统计也已接通；示例与最终验收继续按实现计划地图推进。

- [P7.3 自动维护交付记录](docs/acceptance/P7.3自动维护交付记录.md)

- [配置与诊断](docs/15-配置与诊断.md)与 [P8.1 交付记录](docs/acceptance/P8.1配置诊断交付记录.md)
