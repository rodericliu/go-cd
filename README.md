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

## 配置

- YAML：`configs/config.example.yaml`
- 支持 `${VAR}`、`${VAR:-default}` 展开（解析 YAML 后按字面替换标量值；裸 `$` 不展开；flow 风格 `{...}` / `[...]` 里需写成 `"${VAR}"`）
- 密码等密钥放环境变量 / Secret，不要写进仓库
- `retry_limit: 0` 表示不重试，`count: 0` 表示启动时不起 worker
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

## 测试

```bash
go test -race ./...
golangci-lint run ./...

# 需要 RabbitMQ（默认 guest@127.0.0.1:5672，可用 RABBIT_HOST 等变量覆盖）
go test -tags=integration ./internal/broker/ ./internal/worker/
```
