# 重构职责与状态归属说明

本次只调整职责与依赖边界，不改变任何业务结果、事件轨迹、每次 `next` 的交付位置或
异常优先级；`checks.py`、`workspace.py`、`samples.json`、`expected.json`、
`CONTRACT.md` 均保持原样。真实业务只有一条执行链：`spill.aggregate` →
`pipeline.aggregate` → `Rules` + `order` + `Driver`。

## 模块职责

- `reduction.py` 的 `Rules`：纯规则。`lift` 校验行并生成段单元，`key` 给出可排序
  键（`None` 设备排在所有字符串设备之前），`combine` 合并同键单元并返回新值，
  `finish` 把已按键归并的单元流式归约成公开输出。只读值、返回新值或惰性生成器，
  无任何导入、文件、句柄、I/O 回调或可变状态，不负责调度与清理。
- `sorter.py` 的 `order(records, driver, rules, rows_per_run, fanin)`：只做限额
  切块、内存排序、分轮分批与堆归并。所有取值判断走 `rules.lift/key/combine`，所有
  段访问走 `driver` 的不透明令牌端口；自身不解释字段，不调用 `finish`，不接触
  Workspace/Handle，也不持有物理句柄。缓冲至多 `rows_per_run` 个单元，每轮归并至多
  `fanin` 个待读头加一个写句柄，除段名列表外不驻留随数据量增长的状态。
- `driver.py` 的 `Driver(workspace)`：独占真实 `Handle`、活动句柄登记与本作业段
  所有权。`allocate/open_read/open_write/read/write/close/remove/cleanup` 为全部
  端口；对外令牌是可散列不透明整数，不是 Handle，调度层无法探测其属性。驱动只执行并
  记录单次段操作和资源清理，不比较键、不归约、不遍历单元流。
- `pipeline.py` 的 `aggregate`：与旧入口同签名，负责参数校验、装配三个协作者，
  以及输入迭代器、最终输出的生命周期和错误优先级，通过 `Rules.finish` 消费 `order`
  的结果；不解释业务字段，也不含排序或归并循环。
- `spill.py`：仅保留 `from pipeline import aggregate` 的兼容转发，无旧闭包、无复制
  执行器、无条件回退（`spill.aggregate is pipeline.aggregate`）。

## 状态与所有权

- 段名所有权：`Driver.allocate` 即登记到本作业的 `_owned`；显式 `remove` 即使工作区
  注入故障，也在 `finally` 中解除登记，且不重试（故障设施中 remove 先删除再记事件）。
- 句柄所有权：打开即登记令牌，`close` 先弹出令牌再关句柄，因此句柄关闭抛错时令牌也已
  释放，故障句柄不会残留在活动表中。
- `Driver.cleanup` 逆序处理仍登记的句柄，再逆序删除仍登记的段，逐项继续，按发生顺序
  收集异常并返回；它不替换主异常，主异常优先级完全由 `pipeline` 决定。

## 最后一组交付后的资源交接

`order` 在产出最后一个归并单元后自然结束，但既不关闭输入迭代器，也不删除最终段，更不
调用 `driver.cleanup`。因此最后一组结果交付后，输入仍处于已到 EOF 但未关闭的状态，
最终段仍归本作业所有，直到下一次拉取或显式 `close`：

- 下一次 `next` 触发 `StopIteration`，进入 `pipeline` 的清理：先关输入，再关闭尚在
  执行的最终归并（其读句柄在最后一组交付前可能已关闭，不会因此提前关输入或删最终段），
  然后由 `Driver.cleanup` 逆序关闭遗留句柄、逆序删除剩余段。
- 显式 `close` 走 `GeneratorExit` 路径，清理故障逐项继续但一律不外抛。
- 普通异常时，已有处理异常优先，清理异常只在无主异常（正常结束）时抛出其中第一个；
  单个清理动作失败不阻断其余清理。公开注入故障范围内，作业结束、失败或 close 后
  工作区的 `live` 与 `remaining` 均为空。

## 验证

- `python3 checks.py verify`：冻结的业务参照、完整轨迹、每次 `next` 的 marks 与故障
  优先级，全部通过且未重录期望值。
- `python3 checks.py disk`：真实临时文件上结果、轨迹摘要一致且目录无残留。
- `python3 structure_checks.py`：Rules 纯度与流式结束；不透明单元 + 仅端口替身驱动下
  `order` 的顺序、同键合并、单遍访问、缓冲/句柄上限及输入与最终段的外层归属；故障设施下
  Driver 的所有权释放、逆序清理与错误收集；基于 AST 与运行时导入的依赖边界核查。
- `python3 verify_all.py`：统一执行上述原 verify、真实磁盘对照和结构验证；
  `.pointwise-runtime.json` 的 `verify` 已指向该入口，`demo` 仍为真实文件汇总。
- 另在隔离目录用与基线逐字一致的旧实现做差分，近万组随机场景（内存与真实磁盘、各类
  操作故障、提前 close、输入故障、多轮归并、不同块大小与扇入）结果、轨迹、marks、
  错误类型/消息、句柄峰值与残留完全一致。
