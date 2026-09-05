下面说最重要的。

1. P0：AKShare quote 有潜在的“串合约”风险

你现在是：

for row, subscription in zip(rows, subscriptions, strict=True):

然后直接用这个 subscription.instrument_id 给该 row 定身份。代码只检查返回行数是否相等，没有验证：

row["symbol"]

是否真的等于：

subscription.provider_symbol

这意味着假设请求：

RB2610
ZN2610
CU2610

上游返回顺序突然变成：

ZN2610
RB2610
CU2610

你可能得到：

ZN 的价格 → SHFE.rb2610   ❌
RB 的价格 → SHFE.zn2610   ❌

这是行情系统里非常严重的问题。

应该改成 symbol-based join

逻辑应该是：

subscriptions_by_symbol = {
    normalize_symbol(s.provider_symbol): s
    for s in subscriptions
}

for row in rows:
    native_symbol = normalize_symbol(row["symbol"])

    subscription = subscriptions_by_symbol.get(native_symbol)

    if subscription is None:
        raise MappingError(...)

最后还要验证：

requested symbols == returned symbols

不能依赖 DataFrame 行顺序。

这个我建议第一个修。

2. P0：LiveEventIngress 破坏了你原来的“双数据平面隔离”

你 Phase 0–9 一直坚持：

Persistence Path
    durability-first

Live Path
    freshness-first

两者不能互相阻塞。

但现在 Python AKShare 路径：

for transport in self.transports:
    await transport.publish(event)

是串行的。

假设 transports：

QuestDbLivePersistence
ZmqLivePublisher

就可能变成：

AKShare quote
   ↓
await QuestDB HTTP /write
   ↓
QuestDB 卡 8 秒
   ↓
8 秒后才 ZeroMQ publish

而 QuestDbLivePersistence 确实直接做 HTTP POST /write。

这和你 C++ 架构：

Dispatcher
  ├── persistence
  └── live

的设计原则不一致。

应该改成独立 transport worker

更合理：

                   ┌→ PersistenceQueue → QuestDB worker
QuoteSnapshot → ingress
                   └→ LiveQueue → ZMQ worker

而不是简单：

await asyncio.gather(...)

gather() 虽然并发，但一个慢 transport 仍可能导致当前 emit() 等待全部完成。

最好是：

persistence_queue.put_nowait(...)
live_queue.put_nowait(...)

然后各自 worker 消费。

其中：

Persistence queue
→ 不允许静默丢失

Live queue
→ bounded / latest-wins / observable drops

这样才真正符合你原始架构。

3. P1：API 把“未知”强行变成了 0

现在：

volume=tick["volume"] or 0
turnover=tick["turnover"] or 0
open_interest=tick["open_interest"] or 0

并且模型定义也是非 nullable：

volume: int
turnover: float
open_interest: float

这个对 CTP 也许问题不大，但对 AKShare/IBKR 非常危险。

因为：

open_interest = None

语义是：

上游没提供 / 不知道。

而：

open_interest = 0

语义是：

当前未平仓量确实为 0。

完全不同。

应该改成
volume: int | None
turnover: float | None
open_interest: float | None

并：

volume=tick.get("volume")
turnover=tick.get("turnover")
open_interest=tick.get("open_interest")

这是你 Phase 11B 自己定义的：

unavailable fields remain NULL; do not fabricate zero.

所以当前实现与 requirement 有冲突。

4. P1：quote poller 实际没有 exponential backoff

现在：

while not self._stop.is_set():
    try:
        await self.poll_once()
    except Exception:
        pass

    delay = max(
        0.0,
        self.poll_interval - elapsed
    )

也就是说：

成功 → 5 秒后请求
失败 → 也是 5 秒后请求
失败 → 还是 5 秒
失败 → 还是 5 秒

但你的 Phase 11B 要求的是：

1s
2s
4s
8s
...
max 30–60s
+jitter

目前没有实现。

建议：

success:
consecutive_failures = 0
delay = poll_interval

failure:
consecutive_failures += 1
delay = min(
    max_backoff,
    base_backoff * 2**(failures-1)
) + jitter

同时：

RATE_LIMIT
SCHEMA_CHANGED
MAPPING_ERROR

不一定都应该采用相同 retry 策略。

5. poll_lag_seconds 当前算法基本永远是 0

现在：

delay = max(
    0.0,
    self.poll_interval - (time.monotonic()-started)
)

self.poll_lag_seconds = max(0.0, -delay)

因为：

delay >= 0

所以：

-delay <= 0

于是：

max(0, -delay) == 0

基本永远为 0。

真正应该：

elapsed = time.monotonic() - started

poll_lag = max(
    0.0,
    elapsed - self.poll_interval
)

例如：

poll interval = 5s
本轮请求耗时 = 8s

lag = 3s

这样指标才有意义。

6. 一个 poll batch 失败，会提前终止整个 poll cycle

目前结构是：

try:
    for market:
        for batch:
            await fetch()
            ...
except Exception:
    ...
    raise

假设：

SHFE batch 1 ✅
SHFE batch 2 ❌
DCE batch 1
CZCE batch 1

那么 batch 2 一失败：

DCE / CZCE

这一轮都不会再请求。

对于 best-effort provider，更好的设计通常是：

batch A failure
→ 记录失败

batch B
→ 继续

batch C
→ 继续

最后这一轮 health：

DEGRADED

而不是让一个 symbol/batch 拖掉所有订阅。

7. written=219 这个名称其实不够准确

你的 QuestDB historical repository：

response = await client.post("/write", ...)
response.raise_for_status()

return len(bars)

所以：

"written": 219

实际上严格来说是：

219 rows were submitted successfully to QuestDB HTTP write endpoint.

而不一定意味着：

QuestDB 最终新增了 219 个 canonical rows。

因为表本身：

DEDUP UPSERT KEYS(
    bar_start,
    provider,
    instrument_id,
    interval
)

第二次抓完全相同的 219 条：

submitted = 219
canonical new rows = 0

但代码依然可能返回：

written=219
我建议指标改名

比如：

rows_submitted

或者：

rows_accepted_for_write

如果真的想要：

rows_inserted
rows_updated

则需要另外做 canonical comparison/查询，不能靠 HTTP POST 成功推断。

8. PostgreSQL revision metadata 和 QuestDB canonical write 存在一致性窗口

现在顺序：

revisions = await metadata.record_versions(batch.rows)

written = await bars.upsert(batch.rows)

也就是：

PostgreSQL historical_bar_versions
        ↓ 成功

QuestDB
        ↓
        ❌ 写失败

此时 PostgreSQL 已经认为：

latest historical version = 新版本

但 QuestDB canonical data 仍然是旧版本。

这是典型跨数据库一致性问题。

不建议试图搞分布式事务

对你个人行情系统没必要。

更实际：

provider_ingestion_runs
status = PENDING_CANONICAL_WRITE

raw archive ✅
↓
normalize
↓
record candidate version
↓
QuestDB write
↓
verify/accept
↓
mark version ACCEPTED
↓
run SUCCESS

至少让 metadata 能区分：

OBSERVED
ACCEPTED
FAILED_TO_PERSIST

这样重启后可以 reconciliation。

9. combined benchmark 现在“通过”的条件太宽松

当前退出条件：

return
    wm.healthy &&
    wm.accepted_rows == generated &&
    persistence.push_failed == 0
        ? 0 : 1;

它打印：

live_sent
live_failures
live_dropped

但是 不拿它们决定 benchmark 是否失败。

所以极端情况下：

generated = 30000
QuestDB = 30000

live_sent = 0
live_failures = 30000

benchmark 仍然可能返回 0。

这不符合名字：

combined_throughput
至少应该要求

在正常 benchmark profile：

wm.accepted_rows == generated
pm.send_failures_total == 0
lm.dropped_total == 0
pm.messages_sent_total == generated

不过 ZeroMQ PUB 的：

send success

也只意味着消息进入 ZMQ，不意味着 SUB 真收到。

所以更完整的 e2e benchmark 应该有真实 subscriber 统计：

generated
↓
QuestDB accepted
↓
ZMQ PUB sent
↓
ZMQ SUB received
↓
Cache updated

你 Python api_throughput.py 已经部分在做这个。

10. API benchmark 也可以改进

现在它直接写死：

window = 3.0

然后：

generated / 3.0

但 generator 实际启动、shutdown、PUB slow joiner 等都有时间偏差。

你同时已经有：

started = time.monotonic()
elapsed = ...

更准确应该分别记录：

generation_duration
receive_window_duration
e2e_duration

不能所有 rate 都除以固定 3.0。

否则 benchmark 趋势还能看，但数字不够严谨。

11. QuestDbHistoricalBarRepository 每次 upsert 都新建 HTTP client

现在：

async with httpx.AsyncClient(...) as client:
    await client.post(...)

每次调用都会新建/关闭连接池。

手动抓 219 条没问题。

但未来：

1m backfill
几十万行
多 chunk

就会产生重复：

TCP connection
HTTP setup
client creation
close

建议 repository 生命周期持有一个：

self.client = httpx.AsyncClient(...)

跟：

QuestDBQuoteRepository

一样，在 worker shutdown 时：

await repo.close()
12. 代码格式本身值得重构

你现在很多 Python/C++ 都是：

a=...;b=...;c=...

甚至整个函数挤一行。

功能没问题，但对于金融行情系统这种需要长期检查：

线程
异常
数据语义
重试
状态机

的代码，我不建议这样。

尤其像：

if provider:return ...

这种代码导致你前面调 API 时也更难快速发现运行路径。

我会建议：

Python：

ruff
black

C++：

clang-format
clang-tidy

并加入 CI。

不是为了“漂亮”，而是为了降低这种系统发生隐蔽错误的概率。