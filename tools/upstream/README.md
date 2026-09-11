# 固定上游行为执行器

这是 P9 验收工具，不是 RasterKV 的运行依赖或对外产品接口。当前验收状态与实测差异见 [一期验收报告](../../docs/acceptance/一期验收报告.md)。执行器使用 MIT 许可的 Microsoft FASTER；核心源码来自固定提交，保留在独立上游检出目录，不向本仓库复制引擎实现。变长键上下文遵循上游 `cc/test/in_memory_test.cc` 的浅键/深键 API。

在 Linux 安装 g++、libaio-dev、libtbb-dev、uuid-dev，并检出上游提交 `321d872eabda6a0345c8bd76419f89723ed864ae` 后：

```sh
sh tools/upstream/build.sh ../FASTER target/p9-build
python3 tools/upstream/compare.py --cpp target/p9-build/faster-replay --output target/p9-comparison
```

结果目录必须是本次全新目录，已有材料不自动清理。程序保存输入、Rust/C++ 输出、构建和运行日志及逐行差异。当前已有删除相关差异，compare.py 会返回失败；这表示兼容性尚未验收。不能仅因为执行器自身正常退出就声称两端行为一致。

直接执行 `faster-replay 输入.trace 输出.results` 使用 NullDisk；添加 `--disk 全新目录` 使用 Linux QueueIoHandler/libaio 文件后端。上游 ThreadPoolIoHandler 仅存在于 Windows，Linux 对照不能使用该类型；它与 RasterKV 的 Linux/macOS 工作线程后端不是同一个实现。

上游页固定为 32 MiB，文件对照配置为 256 MiB、可变比例 0.4，满足至少两个可变页和四个不可变页。小轨迹在文件后端运行不代表触发了磁盘 Pending，必须检查实际输出和超内存工作量。所有外部执行由比较脚本设置进程截止时间，等待原有 Pending，不重新提交已接受请求。

Rust 执行端是 `cargo test --locked --test p9_upstream -- --nocapture`；默认运行固定边界输入。环境变量仅用于验收工具选择输入、结果和恢复位置，不进入 RasterKV 公开 API：

| 变量 | 含义 |
| --- | --- |
| RASTER_UPSTREAM_GENERATED | 设为 1 时还要求输入逐字节等于固定 SplitMix64 生成轨迹 |
| RASTER_UPSTREAM_TRACE | 输入轨迹；未提供则使用 p0.trace |
| RASTER_UPSTREAM_RESULT | 输出真实引擎结果的位置 |
| RASTER_UPSTREAM_ROOT | 原生文件后端的全新根目录；未提供则使用 Null |
| RASTER_UPSTREAM_SPLIT | 完成指定步数后进行检查点、关闭、恢复和会话续接 |
| RASTER_UPSTREAM_CHECKPOINT | full 或 pair；后者使用 Index+Log 配对 |

上游相同业务轨迹、Rust 参考模型、并发历史和协议故障是不相互替代的证据层次。P9 完成前仍需补充原生结果、磁盘/恢复对照及故障交叉矩阵。

独立 CI“P9 上游执行环境基线”先原生验证 libaio 可用、Null/文件两种执行端的固定原始结果。它保留已观察的两处状态差异，明确不是跨实现兼容性通过；`probe.json` 的 compatibility_acceptance 为 not_completed。
