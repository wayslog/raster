# RasterKV

RasterKV 是 raster 仓库计划实现的 Rust 嵌入式键值存储引擎。第一阶段目标是完整复刻 FASTER C++ `FasterKv` 的功能与行为，使用独立持久化文件格式；F2 等高级功能列为后续 TODO。

当前已建立可编译的 Rust 模块骨架，包含公开接口、内部协议和基础约束测试。存储算法与真实设备尚未实现，创建和恢复会明确返回未实现错误。

- [第一期实现计划地图（后续执行入口）](docs/14-RasterKV第一期实现计划地图.md)
- [文档入口](docs/README.md)
- [P0 验收报告](docs/acceptance/P0验收报告.md)
- [第一阶段范围与支持库选型](docs/05-RasterKV第一阶段与支持库选型.md)
- [Rust 抽象与模块设计](docs/09-RasterKV模块设计.md)
- [领域术语](CONTEXT.md)
- [各模块基础骨架与接入说明](docs/13-模块基础骨架.md)

开发工具链与 MSRV：Rust **1.98.1**。基础检查：`cargo test --locked --all-features`。当前只验证骨架、参考模型与基础契约，不表示键值存储、检查点或恢复已经可用。

第一期正式验收 Linux + macOS 基础本地文件后端，Windows 和 io_uring 后置。按计划从 P0.1 开始，依次完成验收基线、编码与内存协议、内存读写、磁盘与 Pending、检查点恢复、扩容/缓存/扫描、压缩回收和总验收；任务状态与完成证据统一在计划地图中维护。
