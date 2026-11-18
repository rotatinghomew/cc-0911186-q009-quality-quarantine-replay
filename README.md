# 分层质量隔离记录的闭环重放与发布门槛

ODS 质量隔离、补正重放、事实幂等和分区发布门槛基线。

仓库内的实现使用 Python 3.11 标准库和 SQLite，数据只写入命令行指定的本地数据库及导出目录。`lakeapp.storage` 保存原始接收、业务状态、隔离记录、重放历史和版本清单；`lakeapp.contract` 负责输入约定与结构化失败原因；`lakeapp.ingest` 接收来源批次并把未通过的行分流到隔离区；`lakeapp.derive` 生成当前基线支持的事实；`lakeapp.quarantine` 负责隔离状态机、补正重放、引用等待与分区质量门槛；`lakeapp.publish` 读取并写出可追溯版本；`lakeapp.cli` 提供本地操作入口。

## 运行

在 Python 3.11 环境中执行 `python3 -m unittest discover -s tests -v` 运行测试，执行 `python3 -m compileall -q lakeapp tests` 检查构建和语法。示例数据是本地合成记录，不依赖外部服务或联网。

## 现有契约

输入批次包含来源名、批次标识、游标和记录数组。原始记录以 JSON 保存，重复批次不会再次产生事实；业务键相同但内容不同的记录会保留冲突信息。事实查询按业务日期和版本清单读取，清单中的文件摘要与行数用于发现导出内容被替换。数据库初始化由运行入口完成，已有数据不会因重新打开而丢失（旧库自动补齐新增列与隔离表）。

## 质量闭环

1. **隔离而不是丢弃**：字段缺失、未知枚举、数值/业务时间不合法的行以状态 `隔离` 落 `quarantine_records`，保留原始行、稳定原因码（`missing_field`、`unknown_enum`、`invalid_value`、`invalid_key`、`invalid_reference`）和业务日期分区；结构合法但跨表引用（记录的 `ref` 字段，按业务键指向事实表）尚未到达的行以 `等待引用` 落库。未通过的行不会进入事实表。
2. **只重放受影响的数据**：`replay_quarantine` 针对单条隔离记录应用源侧补正，每次尝试（成功、失败、等待、空操作）都追加到 `quarantine_replays`。成功重放与正常入仓共用同一条事实写入路径，继续遵守 upsert 版本递增、retract 删除和业务键冲突规则；整批重跑不会重复写入已通过的记录，已入仓的隔离记录保持终态。
3. **引用不到达就不算成功**：引用缺失时重放结果只能是 `等待引用`；引用事实到达后等待记录自动转为 `可重放`，再次重放才会写入事实。
4. **分区发布门槛反映真实缺口**：`partition_gate` 按分区统计未解决记录（隔离/等待引用/可重放）——无缺口为 `可用`；有缺口时严格策略 `阻断`（版本登记为 `blocked`，不开放读取），`allow_delay` 策略为 `延迟`（版本登记为 `delayed` 并标注缺口数）。缺口清零后的新版本恢复 `ready`，可按就绪版本读取。
5. **可追踪**：`trace_record` 汇总原始行、失败缘由、每次补正输入与结果、当前事实版本，运营可判断当前分区是可用、延迟还是受阻。

## CLI 入口

`receive`/`facts`/`progress`/`publish`/`read` 为基线操作；新增 `quarantine`（按来源/状态/分区查隔离记录）、`replay`（单条补正重放）、`replays`（重放历史）、`gate`（分区质量与门槛）、`trace`（单记录全链路）、`version`（带发布状态的版本读取）。`publish` 与 `facts` 支持 `partition`，`publish` 支持 `policy=allow_delay`。

## 目录职责

`lakeapp/storage.py` 负责事务和表结构，`lakeapp/contract.py` 负责记录约束与原因码，`lakeapp/ingest.py` 负责接收、来源进度和隔离分流，`lakeapp/derive.py` 负责从原始记录生成事实，`lakeapp/quarantine.py` 负责隔离状态机、重放与发布门槛，`lakeapp/publish.py` 负责版本清单与回读，`lakeapp/cli.py` 负责把这些业务操作组合给本地使用者。测试数据覆盖正常接收、重复批次、隔离补正、引用等待和分区门槛。
