# StrataLeaf

StrataLeaf 是供 Python 服务嵌入的版本化文档数据库。主要入口是 `strataleaf.Database` 库 API，数据保存在单个 SQLite WAL 数据库文件中；无需远程数据库、密钥或在线依赖。

## 开始使用

需要 Python 3.11+，运行时只用标准库 `sqlite3`。从源码根目录可直接导入；也可运行 `python3 -m pip install .` 安装为包。调用方负责创建数据库文件的父目录。

```python
from strataleaf import Database

with Database("strataleaf.db") as db:
    db.catalog.create("segments", {
        "fields": {"region": "string", "sequence": "integer"},
        "required": ["region", "sequence"],
    })
    written = db.batch("seed_segments").put(
        "segments", "block_a", {"region": "east", "sequence": 3}, expected=0
    ).commit()
    db.retention.pin("reader_a", written.revision)
    db.patch("segments", "block_a", {"sequence": 4}, expected=written.revision)
    print(db.get("segments", "block_a", revision=written.revision))
    db.retention.unpin("reader_a")
```

集合创建不是幂等操作；再次运行示例应使用新文件。`expected=0` 仅允许从未出现过的键；删除后保留墓碑版本，再创建必须指定墓碑版本或不带前置版本。没有 `expected` 的写入采用最后提交覆盖，读后修改应携带读取到的版本。

## 工作流

`python3 -m compileall -q strataleaf examples` 检查源码可编译。`python3 -m examples.workflows commit` 在独立临时目录运行多集合提交、重复请求和回滚示例，打印实际结果后自动清理。可用示例名：

`catalog schema commit snapshot query index feed backup replica compact expire aggregate patch inspect transfer`

每个示例约一秒以内，不需要联网。它们是生产 API 的有限工作流演示；自动化测试仍为 **deferred**，仓库不含测试套件，也不声明测试命令。`examples.observations` 另外提供写锁竞争与保留水位的可复现观测。

## 语义边界

- 一个批次 1–10000 个操作，每个集合/键只能出现一次，多集合版本与索引、变更日志一同原子提交。唯一索引逐操作立即约束，交换两个已占用唯一值需先释放；基线不提供延迟唯一约束。
- 当前目录模式约束未来写入；模式收紧先扫描当前可见文档，不重写历史。目录变动独立于文档修订，使用目录 epoch；旧文档读取消费其原始 JSON。
- 读取固定修订号，删除写入墓碑。TTL 是显式 `retention.sweep(now)` 产生删除提交；时间经过本身不会改变读取结果。到期前版本仍能读取。
- 查询支持嵌套布尔谓词与等值、范围、集合成员、存在性判断，按文档键升序分页。签名游标固定集合、谓词、读版本与键边界，可调整页大小。索引只服务当前版本；历史读取回退版本扫描，最终谓词始终复核。
- `None` 和缺失字段不同，比较缺失字段不匹配；数值比较不把布尔值当数字。分组跳过缺失分组字段，显式空值保留；非数值度量不计入平均分母。
- 读保留点和订阅确认位置限制回收水位；分页游标本身不自动注册保留点。需要跨长时操作的调用方应先 pin；水位已超过游标时明确拒绝。回收保留水位及其后完整读视图。已登记请求的幂等回执不随历史回收删除。
- 变更流一次交付完整提交，消费方负责成功处理后确认；确认可前进到当前 head，系统不替消费方证明处理成功。释放消费者即放弃它的历史保护。
- 备份使用 SQLite 在线备份 API，发布到未占用目标；校验摘要后恢复到新文件，禁止覆盖现有文件。恢复为新主库会生成新的数据库标识及游标签名密钥，保留文档版本；恢复为副本则绑定原主库。进程间不得直接复制活动 WAL 主文件代替备份。
- 副本只能接收同一上游、连续的提交包。重复范围不重复写入；校验错误和缺口整包失败。分段追赶时使用版本扫描，追到导出端 head 才安装最新索引。目录 epoch 防止过时的同版本包倒退目录。副本不接收主库写入；基线不提供提升接口、双向复制或网络传输。导出校验和检测意外损坏，不代替对端身份认证。
- SQLite 保证进程间单写者；同一句柄的锁协调线程操作，多个句柄通过 SQLite 锁协调。长读可能延缓 WAL checkpoint；忙超时由连接 timeout 控制，调用方处理 `sqlite3.OperationalError`。
- 当前实现适合本地中小型嵌入式数据集，版本扫描、分组和逻辑导出会物化输入；不承诺常数内存。单文档上限 1 MiB、深度 32；筛选按键排序。磁盘加密、租户鉴权、分布式共识均不在现有承诺内。

见 [完整规格](PROJECT_SPEC.md)、[数据与兼容边界](docs/storage-semantics.md)、[恢复与观测](docs/operations.md)。
