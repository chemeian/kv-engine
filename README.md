# kv-engine-b

一个从零实现的「带持久化的键值存储引擎」——Redis 的一个小子集，**仅使用 Python 标准库，无任何第三方依赖**。

特性一览：

- 内存 KV：`SET` / `GET` / `DEL` / `EXISTS` / `KEYS` / `TTL` / `EXPIRE`
- TTL 惰性过期：`GET` 返回空、`EXISTS` 返回 0、`KEYS` 不再列出
- 持久化：AOF 追加日志（写前日志）+ 定期全量快照，重启后先加载快照再重放 AOF
- 并发安全：多线程读写安全，网络层每个连接一个线程
- 网络接口：TCP server + RESP 协议，可直接用 `redis-cli` / `netcat` 连接

要求 Python 3.8+（已在 Python 3.14 上验证）。

## 目录结构

```
kvs/
  store.py       内存 KV（线程安全、惰性 TTL），可独立单元测试
  resp.py        RESP 协议：请求解析（multibulk / inline）、回复编解码
  aof.py         追加日志 AOF：写入、重放、压缩重写
  snapshot.py    全量快照：二进制格式 + CRC32 + 原子落盘
  engine.py      引擎：组合 store + AOF + snapshot，负责恢复与后台维护
  commands.py    命令分发（与网络层解耦，便于按字节测试）
  server.py      TCP 服务端与命令行入口
  client.py      极简 RESP 客户端（测试用，也可当 mini redis-cli）
tests/           单元测试与端到端测试
```

## 启动服务端

```bash
python -m kvs.server
# kv-engine listening on 127.0.0.1:6379
# data dir: ./data | snapshot every 60s or 1000 writes | aof fsync: always
```

可配置项：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `--host` | `127.0.0.1` | 监听地址 |
| `--port` | `6379` | 监听端口 |
| `--dir` | `./data` | 数据目录（存放 `appendonly.aof` 与 `snapshot.kvs`） |
| `--snapshot-interval` | `60` | 每 N 秒做一次快照 |
| `--snapshot-writes` | `1000` | 每 M 次写操作做一次快照 |
| `--aof-fsync` | `always` | AOF 落盘策略：`always` / `everysec` / `no` |

示例：

```bash
python -m kvs.server --port 7000 --dir ./data --snapshot-interval 30 --snapshot-writes 500
```

## 连接与操作

### 方式一：redis-cli

```bash
redis-cli -h 127.0.0.1 -p 6379 set hello world   # OK
redis-cli -h 127.0.0.1 -p 6379 get hello         # "world"
redis-cli -h 127.0.0.1 -p 6379 set greeting hi ex 60
redis-cli -h 127.0.0.1 -p 6379 ttl greeting      # (integer) 60
redis-cli -h 127.0.0.1 -p 6379 keys 'user:*'
redis-cli -h 127.0.0.1 -p 6379 del hello         # (integer) 1
```

直接 `redis-cli -p 6379` 进入交互模式也可以。

### 方式二：netcat（发送 inline 命令）

```bash
printf 'SET hello world\r\nGET hello\r\nKEYS *\r\n' | nc 127.0.0.1 6379
```

Windows 上没有 `nc` 时，可用 PowerShell：

```powershell
$c = New-Object System.Net.Sockets.TcpClient('127.0.0.1', 6379)
$s = $c.GetStream()
$w = New-Object System.IO.StreamWriter($s); $w.NewLine = "`r`n"; $w.AutoFlush = $true
$r = New-Object System.IO.StreamReader($s)
$w.WriteLine('SET hello world'); $r.ReadLine()   # +OK
$w.WriteLine('GET hello');       $r.ReadLine()   # $5
```

### 方式三：内置客户端

```bash
python -m kvs.client --port 6379 set hello world   # OK
python -m kvs.client --port 6379 get hello         # world
python -m kvs.client --port 6379                   # 进入交互式 REPL
```

## 支持的命令

| 命令 | 说明 | 返回 |
| --- | --- | --- |
| `SET key value [EX seconds] [PX ms] [EXAT ts] [PXAT ms]` | 写入，可带过期时间 | `+OK` |
| `GET key` | 读取 | bulk / nil |
| `DEL key [key ...]` | 删除 | 删除个数 |
| `EXISTS key [key ...]` | 是否存在 | 存在个数 |
| `EXPIRE key seconds` | 设置/更新过期时间（`<=0` 表示立即删除） | `1` / `0` |
| `TTL key` | 剩余秒数；不存在或无过期时间返回 `-1` | 整数 |
| `KEYS pattern` | 通配符匹配，支持 `*`（同时支持 `?`、`[]`） | 数组 |

另外提供了 `PING` / `ECHO` / `DBSIZE` / `SELECT` / `COMMAND` / `QUIT` 等辅助命令，方便 `redis-cli` 正常连接。

> 说明：`TTL` 对「键不存在」和「键无过期时间」都返回 `-1`，遵循本项目的需求约定（与 Redis 的 `-2` 略有不同）。

## 持久化机制

1. **AOF 写前日志**：每次 `SET` / `DEL` / `EXPIRE` 都先把命令追加进 `appendonly.aof`（记录带自增序号，采用 RESP 数组编码），**再**应用到内存。因此已确认的写在崩溃后也能恢复。
2. **定期快照**：每 `--snapshot-interval` 秒，或每 `--snapshot-writes` 次写操作，触发一次全量快照。快照记录它覆盖到的最大序号，并采用「临时文件 + `os.replace`」原子替换，附带 CRC32 校验。
3. **AOF 压缩**：快照落盘后，AOF 中被快照覆盖的记录会被安全重写掉（同样是原子替换），避免 AOF 无限增长。
4. **重启恢复**：先加载最近一次快照，再从快照序号之后开始重放 AOF。截断/损坏的 AOF 尾部会被安全跳过。

数据一致性保证：快照与 AOF 压缩都发生在进程内锁内，且都是原子替换；如果在“快照已落盘、AOF 尚未压缩”之间崩溃，恢复时会跳过 AOF 中已被快照覆盖的记录，不会丢数据也不会重复应用。

## Python API

```python
from kvs import Engine

with Engine("./data", snapshot_interval=60, snapshot_writes=1000) as engine:
    engine.set("user:1", "alice")
    engine.set("session:1", "token", ex=300)   # 300 秒后过期
    engine.get("user:1")        # 'alice'
    engine.exists("user:1")     # 1
    engine.expire("user:1", 60) # 1
    engine.ttl("user:1")        # 60
    engine.keys("user:*")       # ['user:1']
    engine.delete("user:1")     # 1
```

只想用内存存储时可以直接使用 `kvs.store.KVStore`。

## 运行测试

```bash
# 从项目根目录运行全部测试
python -m unittest discover -s tests -t . -v

# 也可以只跑某个模块
python -m unittest tests.test_persistence -v
```

测试覆盖：

- `tests/test_store.py`：增删改查、`EXISTS`、`KEYS` 通配符
- `tests/test_ttl.py`：TTL 倒计时、惰性删除、`EXPIRE` 更新（用假时钟，快速且确定）
- `tests/test_persistence.py`：AOF 写前日志、模拟崩溃恢复、快照 + AOF 尾部重放、TTL 跨重启、快照校验和
- `tests/test_concurrency.py`：多线程并发 `SET`/`GET`/`DEL`、并发读一致性
- `tests/test_resp.py`：RESP 解析/编码、粘包与半包
- `tests/test_server.py`：真实 TCP 端到端、`redis-cli`/netcat 风格的 inline 命令、流水线、二进制安全、并发客户端

## 设计取舍与已知限制

- 单机单进程，键空间只有一个（`SELECT` 仅作兼容性应答）。
- 键与值按字符串存储（UTF-8 + `surrogateescape`，因此二进制内容也能无损往返）。
- AOF 默认 `always` 策略（每次写都 `fsync`），用吞吐换「已提交的写不丢失」；追求吞吐可改为 `everysec`。
- 未实现 Redis 的 `INCR`、`HSET`、`MULTI` 等命令，也不支持主从复制与集群。