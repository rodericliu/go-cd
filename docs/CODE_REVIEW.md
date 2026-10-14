# 代码审查与修复记录

| 项目 | 内容 |
|------|------|
| 审查对象 | `4462394` Initial commit: Go rewrite of Consumer Dispatcher |
| 修复范围 | `9339e2c` … `215720b`（共 7 个 commit） |
| 日期 | 2026-10-14 |
| 目标 Go 版本 | 1.27 |
| 参考规范 | [go-guidelines](https://github.com/mhmtszr/go-guidelines) |

---

## 1. 背景与结论

对 Go 重写版做了一次全量代码审查，代码总量约 2300 行，并逐项对照 go-guidelines 检查。

**结论：在原代码基础上修改，不重写。** 理由如下：

- 模块划分（config / broker / worker / manager / admin / forward）清晰、依赖是单向的；关键设计也是对的：连接按 server 复用、并发用 channel、全局共用一个 HTTP Client、失败进 DLQ 而不是 Redis。
- 问题集中在 **AMQP 语义细节**和**测试缺失**，都能在局部修掉，不需要推翻架构。
- 现有代码已经对齐旧版 Java 的行为（表单字段、`ok` 判定、默认命名规则、xml 迁移），重写容易把这些细节弄丢。

审查时的基线：`go vet` 通过；`go test -race` 通过，但只有 3 个包有测试，worker、manager、admin、broker 都没有测试；`gofmt` 不通过；没有 lint 配置。

---

## 2. 发现的问题与处理

"位置"一栏的行号都指审查时的版本 `4462394`。

### 2.1 严重（P0 / P1）：会丢消息、错投 DLQ 或运维误操作

#### S-1 停机、缩容、改配置时，正在处理的消息被错投 DLQ

- **位置**：`internal/worker/worker.go:174-200`
- **问题**：worker 停止时会取消 `baseCtx`，HTTP 请求随之中断，`forwardWithRetry` 返回 false，于是走写 DLQ 的分支。`amqp091-go v1.10.0` 的 `PublishWithContext` **完全不看 ctx**（`channel.go:1589`），所以写 DLQ 一定成功，然后原消息被 ack。
- **影响**：每次发版、调 `SetWorkers` 缩容，或者调 `SetURL` / `SetBlockPatterns` / `SetDiscardMatchJobs`（这几个会重启全部 worker），正在处理的消息都会被**错投 DLQ**。代码里那个 10 秒的 publish 超时实际不起作用。
- **处理**：停止改成两步。
  - `requestStop`：停止拉取新消息，等手上这条处理完。
  - `abort`：超过停机期限后中断处理中的消息，并 **nack 放回原队列**，新增指标 `outcome=requeued`，**不再进 DLQ**。
  - 每个 worker 只创建一个 ctx，不再每条消息起一个 goroutine。
- **验证**：单元测试 `TestHandleAbortRequeues` 覆盖"HTTP 调用中被中断"和"重试等待中被中断"两种情况；集成测试 `TestIntegrationShutdownRequeuesInFlight` 在真实 RabbitMQ 上验证。
- **提交**：`cdd9a4f`

#### S-2 写 DLQ 和重放 DLQ 都没有发布确认，会悄悄丢消息

- **位置**：`internal/worker/worker.go:99, 247-262`；`internal/manager/manager.go:295-323`
- **问题**：
  1. `publishDLQ` 没开 publisher confirms，也没设 mandatory。"发布成功"只说明数据写进了 socket。
  2. `declare_topology: false` 时只被动检查主队列，**不检查 DLQ 是否存在**。DLQ 不存在时，消息发到默认交换机会因为无法路由被直接丢掉，原消息照样被 ack，等于**永久丢失**。
  3. `ReplayDLQ` 用 `Get(autoAck=true)` 先取出再发布，发布失败的话这条消息就丢了。
- **处理**：
  - 新增 `broker.Publisher`：开启 confirm 模式并设置 `mandatory=true`，等 broker 确认，能识别 `ErrNacked` 和 `ErrUnroutable`。basic.return 一定先于对应的 basic.ack 分发（已核对库源码），所以收到确认后检查一次 return 通道就够了。
  - `declare_topology: false` 时也被动检查 DLQ 是否存在。
  - `ReplayDLQ` 改为 `Get(autoAck=false)`，确认重新发布成功后再 ack；失败的消息在 channel 关闭时自动回到 DLQ。
- **验证**：集成测试 `TestPublisherConfirmsAndDetectsUnroutable` 和 `TestIntegrationFailureGoesToDLQ`；端到端验证了重放 DLQ 不丢消息也不重复。
- **提交**：`c96908d`、`cdd9a4f`

#### S-3 环境变量展开会改坏配置

- **位置**：`internal/config/config.go:131, 139`
- **问题**：展开是在 YAML 解析**之前**直接对原文做字符串替换，而且裸 `$VAR` 也会被展开。已用临时测试复现：

  | 输入 | 实际得到 |
  |------|----------|
  | `password: pa$word` | `"pa"` |
  | `password: ${P2}`，其中 `P2="abc #tail"` | `"abc"`（`#` 后面被当成注释） |

  环境变量的值里有 `:`、换行等字符时，还可能改掉 YAML 结构。
- **处理**：先解析成 `yaml.Node`，再只对标量值展开 `${VAR}` 和 `${VAR:-default}`，裸 `$` 原样保留。展开后的值按实际内容重新推断类型，这样 `${PORT}` 也能填进 int 字段；推断结果是 null（如 `null`、`~`、空串）时强制按字符串处理，避免被吞掉。
- **验证**：`TestExpandEnvValuesAreLiteral`（10 个用例）、`TestExpandEnvTypedScalar`、`TestExampleConfigLoads`。
- **提交**：`19fbe17`

#### S-4 用 job 名操作多 server 的 job 时，结果是随机的

- **位置**：`internal/manager/manager.go:136-148`
- **问题**：配置 `server: [a, b]` 会展开成 `mail-a` 和 `mail-b` 两个 job，**两个都会正常消费**。但通过 Admin API 用裸名字 `mail` 操作时，`resolveLogicName` 遍历 map 并返回第一个匹配项，而 map 的遍历顺序是随机的，所以**每次只会随机改到其中一个**，接口照样返回 200。
- **影响**：比如运维调一次 `/api/jobs/mail/url` 想切到新地址，结果只有一个 server 生效，另一个还在往旧地址投递。
- **处理**：裸名字匹配到多个 job 时返回 **409**（`ErrAmbiguousJob`），并列出可用的 `name-server`。只绑定一个 server 的 job 仍然可以用裸名字。`List` 结果按名字排序，保证输出稳定。
- **验证**：`TestResolveJob`（每个用例重复 20 次，用来发现随机行为）、`TestRoutes`。
- **提交**：`cdd9a4f`

#### S-5 Admin 接口缺少输入校验

- **位置**：`internal/admin/server.go`、`internal/manager/manager.go:252-292`
- **问题与处理**：

  | 问题 | 处理 |
  |------|------|
  | `peek`/`take`/`dlq?count=` 的值直接用来 `make` 切片，传大数会 OOM | count 限制在 1–100，缺省 10，越界返回 400 |
  | `workers` 没有上限，1e6 会起 100 万个 goroutine | 限制在 0–256 |
  | `SetURL` 不校验，打错一个字就让所有流量进 DLQ | 应用前先校验：必须是 http/https 的绝对 URL |
  | `SetBlockPatterns` 不校验，正则写错时返回 200，但消费从此停住 | 应用前先编译校验，非法返回 400 |
  | `GET /take` 是破坏性操作 | 改成 **POST** |
  | 请求体没有大小限制 | 加 1 MiB 的 `MaxBytesReader`，超限返回 413 |
  | 解析错误被忽略（`replay-dlq` 的 body、`count` 参数） | 都返回 400 |
- **验证**：`TestAdminInputValidation`、`TestRoutes`（22 个用例）。
- **提交**：`cdd9a4f`

### 2.2 中等

| # | 位置 | 问题 | 处理 | 提交 |
|---|------|------|------|------|
| M-1 | `broker/hub.go:70-73` | 开 channel 失败（如达到 channel_max）时会关掉整条共享连接，同一 server 的所有 worker 都断开 | 只有连接本身已关闭时才重连，否则只返回错误 | `cdd9a4f` |
| M-2 | `broker/hub.go:61-66` | 持锁拨号且没有超时 | 拨号加 10 秒超时（`amqp.DefaultDial`） | `cdd9a4f` |
| M-3 | `worker/worker.go:293-355` | Pool 持锁等待 worker 退出，期间 `/readyz` 和 `/api/jobs` 都被卡住 | 先在锁内摘出要停的 worker，再到锁外后台回收；`Shutdown` 会连同正在回收的 worker 一起等 | `cdd9a4f` |
| M-4 | `worker/worker.go:329-337` | 统计只累加当前 worker，缩容或改配置后计数清零 | 计数器挪到 Pool 级别 | `cdd9a4f` |
| M-5 | `cmd/dispatcher/main.go:68` | `StopAll` 没有超时，15 秒的限制只作用于 HTTP | 一个 15 秒期限覆盖 HTTP 和消费者的全部停机 | `cdd9a4f` |
| M-6 | `cmd/dispatcher/main.go:57` | Admin 端口监听失败后以退出码 0 退出 | 改为退出码 1 | `cdd9a4f` |
| M-7 | `admin/server.go:64-79` | readiness 的 2 秒超时不生效，因为 Ping 不受 ctx 控制 | `Hub.Ping(ctx, …)` 会在 ctx 结束时返回 | `cdd9a4f` |
| M-8 | `forward/client.go:28` | 用零值 `&http.Transport{}`，丢了代理设置、拨号和 TLS 超时、HTTP/2 | 改为 `DefaultTransport.Clone()` 后再调参 | `62528a9` |
| M-9 | `config/config.go` | `retry_limit: 0` 和 `count: 0` 被当成没配置，改成 1，所以没法关掉重试 | 改用 `*int` 区分"没配置"和"配置为 0" | `19fbe17` |
| M-10 | `config/config.go` | `type`、`url`、`block_patterns` 不做校验 | 加载配置时就校验 | `19fbe17` |
| M-11 | `worker/worker.go:206` | 随机抖动只在开头算一次 | 每次等待都按 `[d/2, d]` 重新取随机值，上限 5 秒 | `cdd9a4f` |
| M-12 | `worker/worker.go:167-198` | `Ack`/`Nack` 的错误被丢弃 | 打 Warn 日志（ack 失败意味着消息会被重复投递） | `cdd9a4f` |
| M-13 | `broker/hub.go:89` | 用 `%s:%d` 拼地址，IPv6 不能用 | 改用 `net.JoinHostPort`（Admin 地址也一样） | `cdd9a4f`、`19fbe17` |
| M-14 | `cmd/xml2yaml/main.go:36` | 输出文件可能含明文密码，权限却是 0644 | 改为 0600 | `982ab49` |
| M-15 | `worker/worker.go:216` | 每条成功消息都打一条 Info 日志 | 降为 Debug | `cdd9a4f` |
| M-16 | `config/config.go:305` | `AMQPURL()` 没人调用，也没做转义 | 删除 | `19fbe17` |

### 2.3 对照 go-guidelines

| 规范 | 原状态 | 处理 |
|------|--------|------|
| 改完代码后跑 lint、`go test -race`、`go fix`（1.26 及以上） | `gofmt` 不通过，没有 lint 配置 | 新增 `.golangci.yml`；gofmt、lint、vet、`go fix -diff` 全部干净 |
| 不要静默忽略错误 | ack/nack、JSON 解析、count 解析的错误都被丢弃 | 记日志，或者返回 400 |
| 不在 struct 里存 ctx；Background 只在入口使用 | 每条消息起一个 goroutine 生成 ctx；Admin 操作用 `context.Background()` | 每个 worker 一个 ctx，struct 里只存 `CancelFunc`；`ReplayDLQ`/`Try`/`readiness` 都用 `r.Context()` |
| 不持锁做阻塞操作 | Pool 和 session 都持锁等待 | 见 M-2、M-3 |
| 优雅停机：先停接收新工作、设超时 | 直接中断处理中的消息，消费者停机没有超时 | 两步停止，统一 15 秒期限，超时后放回队列 |
| HTTP Server/Client 最佳实践 | 只有 `ReadHeaderTimeout`；Transport 是零值 | 加 Read、Write、Idle 超时（Write 超时按最长 job 超时计算）；见 M-8 |
| recover 只放在边界，并且不吞掉 `ErrAbortHandler` | 会吞掉 | 对 `ErrAbortHandler` 重新 panic |
| 接口定义在使用方，保持小 | 没有任何抽象，worker 没法测 | 在 worker 包里定义 `ChannelSource`、`Forwarder`、`publisher` 三个小接口 |
| 错误类型选择 | 无占位参数也用 `fmt.Errorf` | 改用 `errors.New`；新增 `ErrAmbiguousJob`、`ErrInvalidArgument`、`ErrNacked`、`ErrUnroutable`；用 `errors.AsType`（1.26） |
| 测试：表驱动、`t.Helper`、不在其他 goroutine 里调 `t.Fatal`、不用 sleep | `client_test.go` 在 handler 里调 `t.Fatalf`，有空 `if` 和死代码 | 全部重写；并发相关测试用 `testing/synctest`，模拟时间，同时检测 goroutine 泄漏 |
| 新语法 | 已经用得不错（`wg.Go`、`for range n`、`cmp.Or`、`min`/`max` 等） | 继续沿用；新增用到 `new(expr)`、`errors.AsType`、`rand.N`、`slog.DiscardHandler` |

---

## 3. 行为变化（对调用方有影响）

1. **`take` 从 GET 改成 POST**，调用方需要修改。
2. 多 server 的 job 用裸名字调 Admin 接口，现在返回 **409**，需要改用 `name-server`。
3. Admin 参数有了上限：`workers` 0–256，`count` 1–100，请求体 1 MiB。
4. 在 flow 风格 YAML（`{...}`、`[...]`）里，`${VAR}` 需要加引号；block 风格不受影响。
5. `retry_limit: 0` 现在表示不重试，原来会被当成 1。
6. 新增指标 `cd_jobs_processed_total{outcome="requeued"}`，表示停机时被中断、放回队列的消息。
7. 每条成功消息的 `forward ok` 日志降到了 Debug 级别。

---

## 4. 验证

### 4.1 静态检查与单元测试

| 检查项 | 结果 |
|--------|------|
| `gofmt` / `go vet`（包括 `-tags=integration`） | 干净 |
| `golangci-lint run`（包括 `--build-tags=integration`） | 0 个问题（审查时是 7 个） |
| `go fix -diff ./...` | 没有改动 |
| `go test -race -count=3 ./...` | 全部通过 |
| 拆分后的每个 commit 单独检查 build、vet、test | 全部通过 |

覆盖率：admin 73%、config 82%、forward 90%、worker 71%、manager 43%、xmlconv 86%。审查时 worker、manager、admin 都是 0。

### 4.2 集成测试（RabbitMQ 4.3.6，用 brew 安装）

| 测试 | 结果 |
|------|------|
| `TestPublisherConfirmsAndDetectsUnroutable` | 通过 |
| `TestIntegrationShutdownRequeuesInFlight` | 通过 |
| `TestIntegrationFailureGoesToDLQ` | 通过 |

加 `-race` 连跑 3 轮全部通过，测试结束后没有残留队列。

### 4.3 端到端验证（真实二进制 + RabbitMQ + 模拟 consumer）

| 场景 | 实际表现 |
|------|----------|
| `/readyz` | `{"status":"ready"}` |
| 正常消息 | 投递 1 次，返回 ok，ack |
| 失败消息（`retry_limit: 1`） | 共投递 2 次，进入 DLQ，`outcome=dlq` |
| `replay-dlq` | 消息回到主队列，又失败 2 次，回到 DLQ，不丢也不重复 |
| SIGTERM 时有一条 3 秒的慢消息 | 等它处理完才退出（2.5 秒），主队列和未确认数都是 0 |
| 没有 MQ 时启动 | `/readyz` 返回 503，worker 按 `reconnect_seconds` 重连，SIGTERM 后立即退出 |

### 4.4 运行方式

```bash
go test -race ./...
golangci-lint run ./...

# 需要 RabbitMQ（默认 guest@127.0.0.1:5672，可用 RABBIT_HOST 等变量覆盖）
go test -tags=integration ./internal/broker/ ./internal/worker/
```

---

## 5. 遗留事项与建议

| 事项 | 说明 | 建议 |
|------|------|------|
| RabbitMQ 4.x 禁止"非持久化且非独占"的队列 | 集成测试时发现：配置 `queue_durable: false` 的 job 在 4.x 上声明队列会失败；从旧版 Java 迁移过来的 `queueDurable="false"` 也一样 | 在配置校验阶段给出提示，或者在迁移文档里注明 |
| 拨号不能被中断 | amqp091 的拨号不支持 ctx，worker 卡在拨号时，强制中断最多要等 10 秒的拨号超时 | 先接受；需要的话改成自定义 `Dial` 函数 |
| `encoding` 字段没有用到 | 表单固定按 UTF-8 编码。旧版如果有非 UTF-8 的 consumer，行为会不一致 | 确认是否还有非 UTF-8 的 consumer |
| Admin 没有鉴权 | PRD 里列为 P1（F-10） | 上线前至少做网络隔离，之后加 Token |
| DLQ 持续写入失败时会循环 | 消息被 nack 放回后马上又被投递，会反复重试（HTTP 重试本身带退避） | 观察 `outcome=failed` 指标，需要时加熔断 |
| manager 覆盖率偏低（43%） | purge、peek、replay 的成功路径要靠真实 MQ，目前只有端到端验证覆盖 | 补充 manager 的集成测试 |

---

## 6. 提交列表

| commit | 说明 |
|--------|------|
| `9339e2c` | 新增 golangci-lint 配置 |
| `19fbe17` | config：解析 YAML 后再展开环境变量，支持显式 0，补充校验（S-3、M-9、M-10、M-16） |
| `62528a9` | forward：Transport 改为复制默认配置，重写测试（M-8） |
| `c96908d` | broker：新增带发布确认和 mandatory 的 Publisher（S-2） |
| `cdd9a4f` | 核心修复：停机时放回队列、DLQ 等待确认、Pool 不再持锁等待、Admin 歧义处理和参数校验、停机超时（S-1、S-2、S-4、S-5、M-1 至 M-7、M-11 至 M-13、M-15） |
| `982ab49` | xml2yaml 输出文件权限改为 0600（M-14） |
| `215720b` | 同步 README 和 PRD |
