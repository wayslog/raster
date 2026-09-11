# 故障与完整历史验收

`fault-matrix.json` 明确列出 40 个测试入口及其验证边界；每个入口包含的内部组合由测试代码决定。它覆盖接收、用户回调、I/O 短传输/取消/关闭、检查点发布与中断、恢复、GC、恢复集释放、多工作者和自动维护，同时执行 32 组完整磁盘历史及恢复，并区分内部值许可争用与用户 Busy 错误。

```sh
python3 tools/acceptance/run_faults.py --output target/p9-faults
```

输出目录必须全新。脚本先构建全部特性测试，再通过 `--list` 确认每个名称唯一存在，以 `--exact --nocapture` 运行，并要求实际通过一个测试、零失败和零忽略；过滤为空不会算通过。产物包含每项原始输出、测试清单、矩阵结果和 32 组完整历史。构建与子进程均设置截止时间；错误会返回非零退出码。

“40 个入口”不是 40 个故障点：用户回调矩阵包含 11 个合法错误/恐慌组合，三个检查点矩阵按运行时实际 I/O 事件逐点中断，释放矩阵也逐点运行。输出中的预期恐慌属于被测输入，测试与命令的最终退出结果决定验收。进程中断和同步数据/命名空间的掉电模型分别验证；不能称为真实硬件断电。

该矩阵不替代独立模型、上游相同业务轨迹、性能或长期资源检查。具体状态见 [一期验收报告](../../docs/acceptance/一期验收报告.md)。独立 CI 在 Linux/macOS 原生执行并保存逐项证据。

## 同进程资源循环

`resource_cycles` 在同一进程预热 8 轮、测量 64 轮；每轮直接运行完整磁盘生命周期和两次恢复，并核对固定的分配、RSS、句柄及线程增长门槛。两平台原生工作流保留环境、日志和逐轮 CSV。下载指定运行的制品后复核：

```sh
gh run download RUN_ID --dir target/p9-resources-native
python3 tools/acceptance/audit_resources.py --sha FULL_SHA --run-id RUN_ID target/p9-resources-native
```

将 `RUN_ID` 和 `FULL_SHA` 替换为同一次运行的编号与完整提交。脚本核对提交、原生 target、两个任务最终状态、真实测试和业务次数、连续轮次及所有预设门槛，生成全新的 `audit.json`；已存在时拒绝覆盖。资源分配峰值只记录，不据此提高运行前固定的增长门槛。范围与限制见 [资源验收](../../docs/acceptance/P9.2资源与性能验收.md)。

## Miri 内存安全检查

本机先准备带 Miri 的 nightly；项目开发和正式验收仍使用 Rust 1.98.1。

```sh
python3 tools/acceptance/run_miri.py --output target/p9-miri
```

脚本记录解释器版本、提交和工作区是否包含未提交改动；逐组运行并验证真实通过数量。页范围及预分配、内建值布局、记录生命周期、缓存、验收计数分配器和结果预算所有权共 40 项使用默认泄漏检测；另一个故意 `forget` 租约的用例单独以 `-Zmiri-ignore-leaks` 检查访问安全。不能把这个例外扩大到其他用例。此矩阵不解释原生文件后端，不代替双平台故障/恢复测试或并发交错证据。

## 完整性能矩阵

```sh
RASTER_BENCH_OUTPUT=target/p9-benchmark cargo run --locked --release --all-features --example benchmark
python3 tools/acceptance/audit_benchmark.py target/p9-benchmark
```

矩阵参数在[性能记录](../../docs/acceptance/P9.2完整性能矩阵.md)中于运行前固定。默认 48 组涵盖原子整数/变长字节、均匀/每线程热点、1/4 线程、纯内存/超内存，各三轮；每组逐步验证 16,384 次业务操作，再检查 Full 恢复后的全部键及会话进度。独立审计会重新解释八份输入、核对完整矩阵、I/O 和放大分母，单组输出不能通过完整审计。

`benchmark.yml` 通过 workflow_dispatch 在 Linux/macOS 原生运行，避免将长矩阵混入每次普通检查；正式验收必须取得指定提交的两份完整制品。基准退出成功仅表示业务与测量结构通过，不表示 P3 既有性能回退已处理。输出和 audit.json 都拒绝覆盖；失败保留合成存储目录。

下载完整运行后，另以 `python3 tools/acceptance/audit_benchmark.py --sha FULL_SHA --run-id RUN_ID target/p9-benchmark-native` 审计两平台制品。它核对运行及任务最终状态、提交、原生 target、三项真实测试、48 条逐组成功记录、全部 CSV 和八份固定输入，并重新计算结果与原生保存的 audit.json 比较。


## 同步写入分配对照

`audit_ready_comparison.py` 重算[同步写入优化归档](../../docs/acceptance/P9.2同步写入分配优化.md)的八组中位数，核对全部 192 行真实样本、固定操作数和文件哈希。首次触发复核的样本必须保留，不把局部收益作为 P3 旧基准整体通过。

```sh
python3 tools/acceptance/audit_ready_comparison.py docs/acceptance/data/p9-ready-experiment
```


## 结果预算复用对照

[结果预算复用记录](../../docs/acceptance/P9.2结果预算复用.md)分别保留完整短矩阵、同二进制校准及更长热点对照。审计不会合并这些样本或删除触发复核的结果。

```sh
python3 tools/acceptance/audit_result_pool.py docs/acceptance/data/p9-result-pool-experiment
```


## 接受序号预检查对照

```sh
python3 tools/acceptance/audit_serial_precheck.py docs/acceptance/data/p9-serial-precheck
```

完整八组和两批单线程热点补充共 228 行全部重算，保留负向场景；与旧 P3 的总体复核分开。


## 同步路径成本诊断

```sh
python3 tools/acceptance/audit_cost_attribution.py docs/acceptance/data/p9-cost-attribution
```

诊断仅归因单线程驻留内存循环；验证真实最终值、接受进度和各组中位数，不把移除协议的变体当成功能验收。采样计时与普通计时分开。


## 可变记录边界查询对照

```sh
python3 tools/acceptance/audit_mutable_boundary.py docs/acceptance/data/p9-mutable-boundary
```

复用同一矩阵格式审核全部 228 行，重新计算中位数；与旧 P3 的总体回退复核分开。


## 原 P3 性能复核材料

```sh
python3 tools/acceptance/audit_performance_review.py docs/acceptance/data/p9-performance-review
```

复算 192 行八组同机对照、56 个受限循环、配置与分配输出，以及三个移除协议后失败的真实测试。审核器保持原 25%/30% 复核线，明确打印仍触发的组数；数据一致与配套报告中的验收解释分别呈现。


## 删除变更的原生配对审核

`paired_benchmark.py` 在同主机以 AB/BA/BA/AB 顺序执行两个已构建的真实二进制；`--interleaved-control` 加入同一个旧二进制的第三标签，并用六种排列平衡运行位置。固定输入、逐操作结果和全键恢复继续校验；任何吞吐比小于 0.75 或 P99 比大于 1.30 都返回非零，不能用一次绿色运行替换早先红色结果。

```sh
python3 tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/focus
python3 tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/remaining
python3 tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/control
python3 tools/acceptance/audit_paired_benchmark.py docs/acceptance/data/p9-delete-performance/interleaved
```

审核器从每轮 CSV、业务日志和去重压缩的完整输入重算中位数及原复核判定，并要求 A/A 的二进制 SHA-256 相同。审核通过仅说明材料和计算一致；数值触线的原始失败状态仍保留。固定诊断工作流源码归档在各组内，主线完整 48 组工作流保持不变。
