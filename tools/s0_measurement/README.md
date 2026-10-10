# S0 后续 W0 测量驱动

本目录只提供临时驱动、协议和离线自测。未 dispatch、push 或修改仓库运行时，未运行正式性能矩阵。旧 S0 目录与失败证据未改动。

## 固定协议

W0 为 Read/Upsert/RMW × uniform/worker-hot/shared-hot × 1/4 threads，共 18 case。每个 case 分别运行 2,000,000 与 400,000,000 操作，形成 36 个独立测量 scope。一个 scope 的所有角色必须在同一 runner，不能跨机器拼接分母。

每个 scope 先按 baseline、candidate、control、cpp 各执行一次**完整相同 count** 的预热，再执行三 Rust 角色的六种排列。C++ 每轮紧邻 candidate，偶数轮先执行 C++，奇数轮后执行，三 Rust 的相对排列不变：

| round | 执行顺序 |
| --- | --- |
| 0 | baseline, cpp, candidate, control |
| 1 | baseline, control, candidate, cpp |
| 2 | cpp, candidate, baseline, control |
| 3 | candidate, cpp, control, baseline |
| 4 | control, baseline, cpp, candidate |
| 5 | control, candidate, cpp, baseline |

因此每 scope 为 4 个预热加 24 个正式进程，共 28 个；36 scope 共 1,008 个进程。控制角色固定引用 baseline 的**同一路径和同一二进制摘要**。没有按结果追加轮次、跳过失败轮次或选择成功子集的入口。

吞吐由各角色六轮 elapsed 中位数作比，P99 由各角色六轮抽样 P99 中位数作比；另保留逐轮的候选/基线、控制/基线及候选/C++ 比值。既有门槛固定：Rust 历史回归吞吐 ≥0.98、P99 ≤1.10；控制吞吐 [0.98, 1/0.98]、P99 [1/1.10, 1.10]；Rust/C++ 吞吐 ≥0.90、P99 ≤1.10。控制无效时 `status=control_invalid`，每个比较分别记录数值门槛 `threshold_passed`、证据有效性 `evidence_valid` 和二者同时满足的 `passed`，防止局部绿色误用。

## 冻结与命令

`contract_template.py` 生成完整 `driver_contract`，包括测量脚本/launcher/overlay SHA、36 个 trace spec 摘要、所有允许参数、全 28 顺序、门槛、600 秒进程超时和 10 秒回收上限。主预注册必须包含这个对象；测量器会在第一个样本之前与当前文件、协议逐项比较，不只是把“当前哈希”抄进输出。

```sh
python3 driver/contract_template.py --output driver-contract.json
python3 driver/overlay_driver.py \
  --source frozen-source/examples/parity.rs \
  --output temporary-checkout/examples/parity.rs
```

overlay 仅接受 SHA-256 `5d2215505fd94e7717a24abd16cbb1b2af635c7de6a98a0a456e73c489bac4c3` 的原始驱动，唯一改变为业务 Deadline 120→600 秒，输出 SHA 为 `9f6989096f5105c6dcf6b7643af8dd0b50667a553756ebd0bcc24f73da39af7e`。两 Rust 版本、短/长两 count 必须覆盖完全一样的临时驱动；`src/` 不变。原文件不能原地覆盖，错误源或二次覆盖会拒绝。

构建流程先验证冻结源码、编译命令和 overlay，再生成并冻结 build receipt：至少包含 `registration_sha256` 和 `binary_sha256` 的 baseline/candidate/control/cpp 四角色平面映射；其他源码、工具链、构建信息保留在该 receipt 与外围证据中。测量器将实际二进制摘要与 receipt 比较，并确认 receipt 绑定所选总预注册。

```sh
python3 driver/interleaved_w0.py \
  --baseline build/reference/parity \
  --candidate build/observed/parity \
  --cpp build/faster-memory \
  --case rmw/shared-hot/4 \
  --count 400000000 --timeout 600 \
  --registration preregistration.json --registration-sha256 REGISTERED_SHA256 \
  --build-receipt build-receipt.json --build-receipt-sha256 BUILD_RECEIPT_SHA256 \
  --output evidence/rmw-shared-hot-4-long
```

不指定 `--case` 可以顺序遍历 18 case，但本轮基础设施应按 case×count 分 36 jobs，避免跨 case 汇总预算超过 job 时限。输出目录必须不存在。CLI 只接受 2M/400M count 与 600 秒超时，正式执行必须为原生 Linux；不提供允许无亲和性或假二进制验收的开关。

## CPU 与超时

默认从允许的 CPU mask 中选择最先的 N 个 CPU，N=1 或4；`--cpus` 可显式选择恰好 N 个互异允许 CPU。不足4个 CPU 时拒绝四线程 case，绝不会把4线程都 pin 到1核。四个逻辑 CPU 可能只有两个物理核，完整 `lscpu` 原文保留，不把 SMT 称为四个物理核心。

所有角色使用同一 mask。父进程每次 spawn 前核对 mask，`affinity_exec.py` 子进程再记录实际 mask、PID 和将执行的完整命令，然后以**同一个 PID** exec 原生程序，后续线程继承它。计时业务循环内不持续轮询线程或插桩。

外层600秒限时涵盖 Python launcher、引擎初始化、计时业务和最终全量核验；内部600秒 Deadline 从 worker 业务开始计，主要在 Busy/Pending 时观察。两者同值不表示语义相同：外部硬上限优先，可能直接杀进程，**不保证正常 close**。两端原生驱动的最终 expected 构造仍是 O(count)，Python 闭式 oracle 没有消除该 wall time。

每个程序在独立进程组启动。超时向整组发送 SIGKILL，最多再等待10秒回收，保留已经输出的 stdout/stderr、命令、状态和哈希，然后继续原计划。非零退出、错误输出、错误亲和性同样保留并继续。任何计划进程无效，该 scope 不从剩余成功样本计算比值。若 SIGKILL 后仍不能回收，停止该驱动，明确隔离已丢失；不能让失控进程污染下一样本。人工中断也在 finally 终止仍活跃的组、保存状态，再传播中断。

## Oracle 与证据

所有 warmup 和正式结果逐项检查 engine、case、count、完整键数、最终 digest、wrapping checksum、精确采样数及正数计时。Rust 还检查非负 retries/Pending，不替 C++ 填造零重试。每 worker n 次、stride=64 的样本数为 `n//64 + int((n//64)%64 < n%64)`，再乘 worker 数；2M/4 必须为31,252。

uniform oracle 用17在mod256的逆元241求每个完整键的首次访问序号；digest 按全部预装键顺序计算，包含热点未访问键。RMW checksum 用每个键的完整递增序列求和。`trace_spec_sha256` 明确只哈希规范化生成规则、配置与参数，**不冒称展开并哈希了400M条操作流**；同 case/count 四角色共用该摘要。每次具体命令、角色、round、binary、child mask 独立记录。

每 scope 输出 environment、trace_spec、全部逐进程原始文件、records 和 comparison。原始stdout/stderr从spawn起直接落盘，所以即使timeout也不会因`subprocess.run`抛异常而丢失 partial output。output目录根保留总预注册和build receipt原字节及其SHA。构建前后源码清单、全二进制、编译版本和工作流日志由外层基础设施归档。

## 执行预算

每 scope 最坏进程预算为 `28 × (600 + 10) = 17,080秒`，约284.7分钟；建议为测量步骤预留290分钟。共享热点以历史2Mops/s估算，21个Rust进程约70分钟，再加7个C++、初始化、最终核验和调度开销；该估计不是完成时间承诺。

主协调方案为每 job 360分钟：环境与checkout20、构建25、测量290、打包5、上传10、余量10，36 jobs、最多18个并行。两个count不塞进同一个job。每个case/count的对照只能在本runner解释；不同runner的绝对时长或CPU型号不能拼成比值。

## 离线自测

```sh
python3 -m unittest discover -s driver -p test_interleaved_w0.py
```

本轮15项测试通过，包括90个小规模展开轨迹与闭式oracle交叉验证、400M精确采样、错输入/少轮/乱序、同结果单位比、控制过快/过慢、CPU不足、注册与receipt篡改、overlay唯一变更、真实假进程timeout杀后代与partial日志、中断清理，以及一次失败后继续全部28进程并完成下一case。测试中假输出和假执行器只存在于明确标记的 `selftest-output` 临时夹具与测试函数，不能当作正式性能证据。

正式执行前还需独立审查冻结脚本、准备流程、总预注册和工作流。自测通过不表示新矩阵已经运行或 S0 性能门已通过。
