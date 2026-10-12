# KV 存储引擎（Redis 子集 · 纯标准库）

从零实现的「带持久化的键值存储引擎」：内存 KV + TTL 过期 + AOF/快照持久化 + 多线程安全 +
RESP 协议 TCP 服务端。**只使用 Python 标准库**，不引入任何第三方依赖。

## 特性一览

- 命令：`SET` / `GET` / `DEL` / `EXISTS` / `KEYS` / `TTL` / `EXPIRE`（另有 `PING`、`ECHO`、`COMMAND`、`QUIT`）
- key 与 value 都是字符串；`KEYS` 支持 `*` 通配符，如 `KEYS user:*`
- TTL：`SET key value EX <秒>` 或 `EXPIRE key <秒>`；过期 key 惰性删除
- 持久化：每次写操作**先追加 AOF、再改内存**；每 60 秒或每 1000 次写做一次全量快照（可配置）
- 重启恢复：先加载最近一次快照，再重放之后的 AOF，保证已提交的写不丢失
- 并发：多线程读写安全
- 网络：实现 RESP 协议，`redis-cli` / `netcat` 可直接连接

## 环境要求

Python 3.8+（开发与验证环境为 Python 3.14），无第三方依赖。

## 目录结构

```
kvengine/
  __init__.py     包入口
  config.py       配置：端口、数据目录、快照策略、fsync
  store.py        内存 KV：数据结构、TTL、通配符匹配、线程安全
  persistence.py  AOF 追加日志 + 快照「代数」管理
  engine.py       命令层：日志先写、过期语义、快照调度、RESP 回复
  resp.py         RESP 协议编解码
  server.py       TCP 服务端 + 命令行入口
  __main__.py     python -m kvengine
tests/            单元测试与端到端测试
```

## 快速开始

```bash
# 默认：监听 127.0.0.1:6379，数据目录 ./data，每 60 秒或每 1000 次写做一次快照
python -m kvengine

# 自定义端口与数据目录
python -m kvengine --port 6380 --data-dir ./mydata
```

### 命令行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--host` | `127.0.0.1` | 监听地址 |
| `--port` | `6379` | 监听端口 |
| `--data-dir` | `data` | 快照与 AOF 的存放目录（不存在会自动创建） |
| `--snapshot-interval` | `60` | 每隔多少秒做一次快照 |
| `--snapshot-every` | `1000` | 每多少次写命令做一次快照 |
| `--no-fsync` | 未启用 | 开启后跳过「每条写命令后 fsync」，更快但持久性更弱 |
| `--log-level` | `INFO` | 日志级别 |

## 用 redis-cli 连接

```bash
redis-cli -p 6379
127.0.0.1:6379> SET user:1 alice
OK
127.0.0.1:6379> SET session:abc token EX 60
OK
127.0.0.1:6379> GET user:1
"alice"
127.0.0.1:6379> TTL session:abc
(integer) 60
127.0.0.1:6379> KEYS user:*
1) "user:1"
127.0.0.1:6379> DEL user:1
(integer) 1
```

非交互式同样可用：

```bash
redis-cli -p 6379 SET user:2 bob
redis-cli -p 6379 GET user:2
```

## 用 netcat 连接

服务端同时接受 RESP 数组和「行内命令」（纯文本，以 CRLF 或 LF 结尾），所以可以直接用 netcat 手敲：

```bash
$ nc 127.0.0.1 6379
SET user:1 alice
+OK
GET user:1
$5
alice
TTL user:1
:-1
KEYS user:*
*1
$6
user:1
QUIT
+OK
```

响应里的 `+`、`$5`、`:-1` 等前缀就是 RESP 协议的类型标记，`redis-cli` 会自动解析它们。

## 支持的命令

| 命令 | 说明 | 返回 |
| --- | --- | --- |
| `SET key value [EX seconds] [PX ms]` | 写入字符串，可附带过期时间 | `+OK` |
| `GET key` | 读取 | 值 / `nil` |
| `DEL key [key ...]` | 删除 | 实际删除的个数 |
| `EXISTS key [key ...]` | 判断是否存在 | 存在的个数 |
| `EXPIRE key seconds` | 设置或更新过期时间；`seconds <= 0` 立即删除 | `1` 成功 / `0` key 不存在 |
| `TTL key` | 剩余存活秒数 | `>= 1` 存活；`-1` 表示无过期时间或 key 不存在 |
| `KEYS pattern` | 匹配 key（只有 `*` 是通配符，其余字符按字面匹配） | 数组，按字典序 |
| `PING` / `ECHO` / `COMMAND` / `QUIT` | 辅助命令 | — |

## 运行测试

```bash
# 全部测试
python -m unittest discover -s tests -t .

# 单个文件
python -m unittest tests.test_persistence -v
```

测试覆盖：增删改查、通配符匹配、TTL 过期与 `EXPIRE`、持久化恢复（优雅关闭与「崩溃镜像」两条路径）、
快照与 AOF 尾部截断、并发读写、RESP 协议与 socket 端到端。
若本机装有 `redis-cli`，套件还会自动跑一个 `redis-cli` 互操作测试（未安装则跳过）。

## 设计说明

### 内存存储与 TTL

`KVStore` 用 `dict[str, str]` 存数据，另用 `dict[str, float]` 存绝对过期时间（epoch 秒）。
读取路径（`GET` / `EXISTS` / `KEYS` / `TTL`）遇到过期 key 时做**惰性删除**：先把键值从内存摘掉，
再返回「不存在」，因此过期 key 不会被 `KEYS` 列出。`TTL` 返回向上取整的剩余秒数，
计算前先把剩余时间对齐到毫秒，避免日志的毫秒精度把结果顶多出一秒。

### 先写日志，再改内存

每个写方法都在**持有锁**时先把规范化后的命令交给 AOF，落盘（默认 `fsync`）成功后才修改内存；
日志写入失败则内存保持不变。这样 AOF 只可能领先于内存、绝不会落后，「已确认的写」不会因崩溃而丢失。

日志记录的是**绝对时间**：`EXPIRE k 10` 写为 `PEXPIREAT k <毫秒时间戳>`，
`SET k v EX 10` 写为 `SET k v PXAT <毫秒时间戳>`，因此重放时机不影响过期结果。

### 快照与「代数」切换

数据目录按递增代数 `n` 组织文件：

```
data/snapshot-00000001.json   全量状态，原子写入
data/aof-00000001.aof         该快照之后的所有写
```

做快照时（全程持锁，写命令无法插入）：

1. 先把 AOF flush + fsync；
2. 把当前状态写入 `snapshot-<n+1>.json.tmp`，fsync 后 `os.replace` 原子改名；
3. 切换到新的 `aof-<n+1>.aof`，后续写命令都写进新文件；
4. 删除第 `n` 代文件。

任何时刻崩溃，磁盘上要么是完整的第 `n` 代，要么是完整的第 `n+1` 代。

### 重启恢复流程

1. 找到序号最大的、能成功解析的快照（最新快照若损坏则回退到上一代）；
2. 载入快照状态；
3. 重放**同一代号**的 AOF；若 AOF 末尾不完整（进程死在 append 中途），
   丢弃最后一条不完整记录，保留之前所有已完整落盘的命令。

### 并发安全

`KVStore` 用一把 `threading.RLock` 同时保护数据与日志，「追加日志 + 修改内存」在锁内整体完成，
读线程永远看不到半个命令。TCP 服务端基于 `socketserver.ThreadingTCPServer`，每个连接一个线程。
快照由后台 daemon 线程（按时间）与写命令计数（按次数）共同触发，两者共用一把快照锁，不会重复快照。
自上次快照以来没有任何写入时，定时触发会被跳过（不重复生成内容相同的快照）。

### 磁盘格式示例

`aof-00000000.aof` 就是 RESP 命令流，可以直接阅读或解析：

```
*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n
*3\r\n$10\r\nPEXPIREAT\r\n$1\r\nk\r\n$13\r\n1735689600000\r\n
```

## 已知限制（有意取舍）

- `KEYS` 只把 `*` 当作通配符，不支持 `?` 与 `[]`，与题目要求一致；
- 未实现 `SET` 的 `NX` / `XX` / `KEEPTTL` 等选项，也没有多数据库、事务、发布订阅；
- `KEYS` 是全量扫描，适合中小数据量；
- AOF 会随写入持续增长，直到下一次快照把它滚动换代；
- 快照是全量重写（没有增量或后台子进程），大数据量下会产生一次较长的停顿。
