# Consumer Dispatcher (Go)

RabbitMQ → HTTP 转发调度器。从队列取消息，以 `application/x-www-form-urlencoded` POST 到消费者；响应体为 `ok`（忽略大小写）视为成功。

本目录为 Go 重写实现。需要 **Go 1.27+**。原版 Java 在仓库根目录。

产品需求与设计说明见 [docs/PRD.md](docs/PRD.md)，代码审查与修复记录见 [docs/CODE_REVIEW.md](docs/CODE_REVIEW.md)。

## 快速开始

```bash
cp configs/config.example.yaml configs/config.yaml
cp .env.example .env
# 编辑 .env / config.yaml，密钥用环境变量注入

set -a && source .env && set +a
go run ./cmd/dispatcher -config configs/config.yaml
```

构建：

```bash
go build -o bin/dispatcher ./cmd/dispatcher
```

Docker：

```bash
docker build -t consumer-dispatcher .
docker run --rm -p 8888:8888 --env-file .env \
  -v "$PWD/configs/config.yaml:/app/configs/config.yaml" \
  consumer-dispatcher
```

## 从旧 job.xml 迁移

```bash
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml
# 可选：密码改成环境变量占位
go run ./cmd/xml2yaml -in ../conf/job.xml -out configs/config.yaml -env-secrets
# 不合并同名同配置 job：
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml -merge=false
```

同名且除 `server` 外配置相同的 job 会合并为 `server: [a, b, ...]`。

job.xml 里没有鉴权和日志配置，生成的 YAML 会按示例配置补上 `admin.tokens: ["${ADMIN_TOKEN:-}"]` 和 `log`（`level: ${LOG_LEVEL:-info}`，`file.path: ${LOG_FILE:-}`，失败 body 默认不记录）。上线前设置 `ADMIN_TOKEN`；需要 HTTPS / mTLS 时手动加 `admin.tls`。

## 配置

- YAML：`configs/config.example.yaml`
- 支持 `${VAR}`、`${VAR:-default}` 展开（解析 YAML 后按字面替换标量值；裸 `$` 不展开；flow 风格 `{...}` / `[...]` 里需写成 `"${VAR}"`）
- 密码等密钥放环境变量 / Secret，不要写进仓库
- `retry_limit: 0` 表示不重试，`count: 0` 表示启动时不起 worker
- 日志文件：`log.file.path`（或环境变量 `LOG_FILE`）设置后写入该文件，目录不存在会自动创建，路径不可写时启动失败。按 `max_size_mb` 切割，保留 `max_backups` 个 / `max_age_days` 天，`compress` 控制旧文件 gzip。配置了文件时默认不再输出到终端，`log.stdout: true` 可同时输出。为空时只输出到 stdout
- 日志：`log.level` 设置级别（debug/info/warn/error）。消息进 DLQ 时会打 `message dead-lettered`；`log.failed_body.enabled: true` 时，这条日志会附带 body（按 `max_bytes` 截断，按 `sample_rate` 采样）。默认不记录，因为 body 可能含敏感数据

约定（与旧版一致）：

| 字段 | 缺省 |
|------|------|
| `queue` | `name` |
| `exchange` | `{name}_router` |
| `type` | `direct` |
| `dlq` | `{queue}.dlq` |

`server` 可写单个名或列表。列表时其它字段共用，按每个 server 展开成独立消费者（逻辑名 `name-server`）：

```yaml
- name: text_mail
  server: [site_a, site_b]
  queue: text_mail
```

HTTP 表单字段：`queueName`（job name）、`bodyData`（消息体）。

失败重试耗尽后写入 DLQ（publisher confirm 确认后），再 ack 原消息（不再用 Redis 存失败任务）。停机时先等处理中的消息完成；超过 15s 则中断并放回原队列，不会进 DLQ。

## Admin API

默认监听 `admin.host:admin.port`（示例 `0.0.0.0:8888`）。

### 鉴权

`/api/*` 受保护；`/healthz`、`/readyz`、`/metrics` 始终免鉴权（给探针和 Prometheus 用）。

- **Bearer Token**：配置 `admin.tokens`（建议写 `${ADMIN_TOKEN}`，每个至少 16 个字符），请求时带 `Authorization: Bearer <token>`。可以配置多个，方便轮换。
- **mTLS（可选）**：配置 `admin.tls.cert_file` / `key_file` 启用 HTTPS；再配置 `client_ca_file` 后，`/api/*` 必须携带该 CA 签发的客户端证书。探针不带证书也能访问。
- 两者同时配置时，两个条件都要满足。都没配置时 `/api/*` 不鉴权，启动时打 Warn。
- 写操作（非 GET）都会记录审计日志 `admin api`，包括 method、path、status、identity（`token#N` 或 `cert:<CN>`）、remote；GET 请求在 debug 级别记录。token 本身不会写进日志。

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" localhost:8888/api/jobs
```

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/healthz` | 存活探针 |
| GET | `/readyz` | 就绪探针（含 AMQP ping） |
| GET | `/metrics` | Prometheus |
| POST | `/api/reload` | 重新加载配置文件（`?dry_run=true` 只预览），见下文 |
| GET | `/api/jobs` | 任务列表 |
| GET | `/api/jobs/{id}` | 任务详情（`name-server`；job 只绑定一个 server 时也可用 `name`） |
| POST | `/api/jobs/{id}/workers` | `{"count":N}` 扩缩容（0–256） |
| POST | `/api/jobs/{id}/url` | `{"url":"..."}` |
| POST | `/api/jobs/{id}/block-patterns` | `{"patterns":"..."}` |
| POST | `/api/jobs/{id}/discard-match` | `{"discard":true}` |
| POST | `/api/jobs/{id}/try` | `{"body":"..."}` 试投 |
| POST | `/api/jobs/{id}/purge` | 清空主队列 |
| POST | `/api/jobs/{id}/purge-dlq` | 清空 DLQ |
| GET | `/api/jobs/{id}/peek?count=10` | 窥视（不 ack） |
| POST | `/api/jobs/{id}/take?count=10` | 取出（ack） |
| GET | `/api/jobs/{id}/dlq?count=10` | 窥视 DLQ |
| POST | `/api/jobs/{id}/replay-dlq` | `{"count":10}` 重放到主队列 |

`count` 取值 1–100，缺省 10。`{id}` 用多 server job 的裸 `name` 时返回 **409**，需改用 `name-server`。其他错误：404 不存在、400 参数非法、413 请求体超过 1 MiB。

通过 Admin API 做的修改只保存在内存里，重启后恢复为配置文件的值。

### 配置热加载

修改配置文件后，不用重启就能生效：

```bash
curl -XPOST -H "Authorization: Bearer $ADMIN_TOKEN" 'localhost:8888/api/reload?dry_run=true'  # 先预览
curl -XPOST -H "Authorization: Bearer $ADMIN_TOKEN" localhost:8888/api/reload                # 生效
kill -HUP <pid>                                                                              # 或发信号
```

- 新配置先完整校验；有错误时返回 400，**继续使用当前配置**（SIGHUP 时错误写入日志）。
- 按逻辑名（`name-server`）对比新旧 job：

| 变化 | 处理 |
|------|------|
| 新增 job | 创建并开始消费 |
| 删除 job | 处理完手上的消息后停止（最多等 15 秒，超时的消息退回队列） |
| 只改了 `count` | 直接扩缩容 |
| 改了 `url`、`url_host`、`timeout_ms`、`retry_limit`、`prefetch`、`block_patterns`、`discard_match_jobs`、`encoding` | 平滑替换 worker |
| 改了队列、exchange、路由、durable、`declare_topology` 或 server 连接参数 | 停掉后按新配置重建 |

- 通过 Admin API 做的临时修改：只有文件里**同一项**改了才会被覆盖，其他保留。重建的 job 完全以文件为准。
- `admin.tokens`、`log.level`、`log.failed_body` 也会热加载；`admin.host`/`port`/`tls`、`log.file`、`log.stdout` 需要重启，返回结果的 `restart_required` 里会列出来。
- 不再被任何 job 使用的 RabbitMQ 连接会被关闭。
- 多副本部署时每个实例要分别触发。

返回示例：

```json
{"dry_run":false,"jobs":{"added":["sms-a"],"removed":["pay-a"],"updated":["mail-a"],"scaled":[],"recreated":[],"unchanged":3},
 "settings":["log.level"],"restart_required":[]}
```

## 测试

```bash
go test -race ./...
golangci-lint run ./...

# 需要 RabbitMQ（默认 guest@127.0.0.1:5672，可用 RABBIT_HOST 等变量覆盖）
go test -tags=integration ./internal/broker/ ./internal/worker/
```
