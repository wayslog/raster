# 固定上游行为执行器

这是 P9 验收工具，不是 RasterKV 的运行依赖或对外产品接口。当前验收状态与实测差异见 [一期验收报告](../../docs/acceptance/一期验收报告.md)。执行器使用 MIT 许可的 Microsoft FASTER；核心源码来自固定提交，保留在独立上游检出目录，不向本仓库复制引擎实现。变长键上下文遵循上游 `cc/test/in_memory_test.cc` 的浅键/深键 API。

在 Linux 安装 g++、libaio-dev、libtbb-dev、uuid-dev，并检出上游提交 `321d872eabda6a0345c8bd76419f89723ed864ae` 后：

```sh
sh tools/upstream/build.sh ../FASTER target/p9-build
python3 tools/upstream/compare.py --cpp target/p9-build/faster-replay --corrected-cpp target/p9-build/faster-replay-retained --output target/p9-comparison
```

结果目录必须是本次全新目录，已有材料不自动清理。程序保存输入、Rust/C++ 输出、构建和运行日志及逐行差异。原始上游的强制墓碑差异仍保留；提供修正执行端时以其完整结果作为已确认契约的精确对照。不能仅因为执行器自身正常退出就声称两端行为一致。

`compare.py --disk` 让 Rust 与 C++ 都使用各自原生文件后端。自动工作流和手动工作流“P9 随机契约对照（保留原始差异）”均执行 Null 和文件两组，每组使用相同的固定边界及五个种子，共 7,508 步。原始及修正 C++ 输出、Rust 输出和全部差异完整归档。

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
| RASTER_UPSTREAM_TIMEOUT | 单次检查点及边界关闭的等待秒数，默认 60，上限 600；不改变引擎默认恢复预算 |
| RASTER_UPSTREAM_SEGMENT_BYTES | Rust 文件段大小，默认 1 MiB；大生命周期使用 32 MiB，页窗口仍是 4×32 KiB |

上游相同业务轨迹、Rust 参考模型、并发历史和协议故障是不相互替代的证据层次。原生随机、磁盘/恢复对照及两平台故障矩阵已执行；指定生命周期通过，删除契约已确认，修改后的证据和未完成门槛见一期验收报告。

CI“P9 上游契约及生命周期对照”构建原始可执行文件 `faster-replay`，以及 `faster-replay-retained`：后者只在构建目录副本的索引移除条件中增加 `!force_tombstone`。`forced-tombstone.diff/json` 记录唯一修改、源文件与补丁 SHA-256；上游检出保持不变。

固定边界保留原始上游的一处强制墓碑可达性差异。随机对照要求 Rust 与修正副本逐行完全相等，原始上游输出及所有差异同时归档；没有按操作类型忽略结果。省略 `--corrected-cpp` 仍运行原始严格对照，任何差异失败。`probe.json` 只报告固定边界的 `approved_contract_passed`，不能代替随机或生命周期验收。

## 检查点与超内存生命周期

```sh
python3 -B tools/upstream/lifecycle.py --cpp target/p9-build/faster-replay --output target/p9-lifecycle
```

同一文件分别交给两个真实执行端，在指定边界执行 Full 或 Index+Log，关闭实例、恢复并续接三个原会话。C++ 检查每个持久化回调的身份、次数、状态与序号，所有会话回到 REST 后才发起下一动作。每个操作仍比较原始结果，任何差异均失败。

检查点场景的上游索引为 2,048 个桶；原始短轨迹保留 128 桶。上游把索引拆成 256 块，每块又必须满足 512 字节扇区对齐，因此 128 桶不满足其检查点前提。首次原生运行在该断言失败；修正测试配置，不修改上游算法或断言。

`scenarios.py` 固定三组场景：141 步小轨迹分别覆盖 Full、Index+Log；大轨迹固定 32,768 次至少 16 KiB 的追加 Upsert，加上冷键、变长 RMW、墓碑、恢复后读写。大轨迹必须实际输出超过 512 MiB 的上游日志跨度，以及非零 Read、RMW Pending。生成器记录步数、恢复边界、写入负载及 SHA-256，不能用输入大小代替实际日志跨度。

脚本预先固定每个进程 1,200 秒截止、Rust 检查点等待 600 秒、C++ 检查点 120 秒及单个 Pending 60 秒；运行 Rust 发布模式。输出保存完整输入、结果和日志；失败时保留该次合成存储目录，由 CI 上传，成功后清理。上游小轨迹没有触发 Pending 也须如实记录。生命周期通过只覆盖这些固定场景，不能替代独立的 7,508 步随机契约对照。
