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

### 清空队列（purge）

`purge` 会删除队列里所有**等待中**的消息，**不可恢复**。

- 队列在 RabbitMQ 上由所有副本共享，所以在任意一个副本上 purge 都对整个队列生效。
- 已经被 worker 取走、还没 ack 的消息不受影响，会照常处理完。
- 如果只是想清掉某一类脏数据，优先用 `block-patterns` 加 `discard-match` 精确丢弃，不用清空整个队列。

关键是 **purge 之前先让所有副本停止消费这个 job**，否则其他副本在 purge 前后还会继续投递消息。`/workers` 只对收到请求的那个副本生效，所以多副本部署时要按下面的方法操作（程序不提供自动的跨副本协调，原因见 PRD F-13）。

下面的 `JOB` 要写逻辑名（`name-server`）。多 server 的 job 对应不同 server 上的不同队列，每个逻辑名要分别操作。

```bash
JOB=tnc_forum_post_dfv2-n
API=localhost:8888/api/jobs/$JOB
AUTH="Authorization: Bearer $ADMIN_TOKEN"
```

#### 单副本

```bash
# 1. 确认要清的是哪些消息，并记下当前 worker 数（返回结果里的 "workers"）
curl -s -H "$AUTH" "$API/peek?count=5"
curl -s -H "$AUTH" "$API"

# 2. 暂停消费：worker 设为 0（每个 worker 会先处理完手上那条消息）
curl -s -XPOST -H "$AUTH" -d '{"count":0}' "$API/workers"

# 3. 确认已暂停："workers" 为 0
curl -s -H "$AUTH" "$API"

# 4. 清空队列，返回 {"purged":N}
curl -s -XPOST -H "$AUTH" "$API/purge"

# 5. 恢复消费：改回第 1 步记下的 worker 数
curl -s -XPOST -H "$AUTH" -d '{"count":2}' "$API/workers"
```

#### 多副本（Podman 多容器）

假设多个容器的名字都带 `consumer-dispatcher`（如 `consumer-dispatcher-1`、`consumer-dispatcher-2`）。可以任选下面一种方法：

**方法一：只留一个容器，停掉其他的**（最简单，但停止期间其他 job 的消费能力会下降）

```bash
podman ps --filter name=consumer-dispatcher --format '{{.Names}}'   # 查看所有实例

# -t 20：给程序留出优雅停机时间（程序最多用 15 秒处理完手上的消息）。
# podman stop 默认只等 10 秒，超时会强杀。
podman stop -t 20 consumer-dispatcher-2 consumer-dispatcher-3

# 在剩下的 consumer-dispatcher-1 上按「单副本」的步骤 1–5 操作（API 用它映射到宿主机的端口）

podman start consumer-dispatcher-2 consumer-dispatcher-3
```

**方法二：逐个容器把 worker 设为 0**（不影响其他 job）

在容器内部用 `wget` 调用 Admin API，不用关心每个容器映射到宿主机的哪个端口：

```bash
CTRS=$(podman ps --filter name=consumer-dispatcher --format '{{.Names}}')
api()  { podman exec "$1" wget -qO- --header "$AUTH" "http://127.0.0.1:8888/api/jobs/$JOB$2"; }
post() { podman exec "$1" wget -qO- --header "$AUTH" --post-data "$2" "http://127.0.0.1:8888/api/jobs/$JOB/$3"; }

# 1. 记下每个容器当前的 worker 数
for c in $CTRS; do echo "$c $(api "$c" | grep -o '"workers":[0-9]*')"; done

# 2. 暂停所有容器
for c in $CTRS; do post "$c" '{"count":0}' workers; echo " <- $c"; done

# 3. 确认全部为 "workers":0
for c in $CTRS; do echo "$c $(api "$c" | grep -o '"workers":[0-9]*')"; done

# 4. 在任意一个容器上 purge 一次即可，返回 {"purged":N}
post "$(echo "$CTRS" | head -1)" '{}' purge

# 5. 恢复所有容器（改成第 1 步记下的 worker 数）
for c in $CTRS; do post "$c" '{"count":2}' workers; echo " <- $c"; done
```

容器分布在**多台宿主机**上时，到每台机器上执行方法二的第 1–3、5 步（purge 只需执行一次）。也可以在运维机上直接用 `curl` 调各实例映射出来的端口：

```bash
INSTANCES="host1:8888 host1:8889 host2:8888"   # 每个实例的 Admin 地址
for i in $INSTANCES; do curl -s -XPOST -H "$AUTH" -d '{"count":0}' "http://$i/api/jobs/$JOB/workers"; echo " <- $i"; done
```

注意：

- 容器名过滤条件 `name=consumer-dispatcher` 换成实际的名字；也可以按 label 过滤，比如 `--filter label=app=consumer-dispatcher`。
- 如果容器是用 systemd 或 Quadlet 管理的，方法一要用 `systemctl stop` / `systemctl start`，否则 systemd 可能会把 `podman stop` 掉的容器重新拉起来。
- 如果容器配置了 `--restart` 策略，操作期间有容器重启的话，它会按配置文件里的 `count` 重新开始消费。purge 前再确认一遍所有容器的 `workers` 都是 0。

#### 多副本（Kubernetes）

**方法一：先缩容到 1 个副本**

```bash
kubectl scale deployment/consumer-dispatcher --replicas=1
kubectl get pods -l app=consumer-dispatcher   # 等多余的 Pod 完全退出（优雅停机最多 15 秒）
# 按「单副本」的步骤 1–5 操作
kubectl scale deployment/consumer-dispatcher --replicas=<原副本数>
```

**方法二：逐个 Pod 把 worker 设为 0**：和 Podman 的方法二一样，只是把容器列表和 exec 命令换成：

```bash
CTRS=$(kubectl get pods -l app=consumer-dispatcher -o name)
api()  { kubectl exec "$1" -- wget -qO- --header "$AUTH" "http://127.0.0.1:8888/api/jobs/$JOB$2"; }
post() { kubectl exec "$1" -- wget -qO- --header "$AUTH" --post-data "$2" "http://127.0.0.1:8888/api/jobs/$JOB/$3"; }
```

#### 通用说明

- 镜像基于 alpine，自带 `wget`。启用了 Admin TLS 时地址要改成 `https://`；如果用的是自签证书，还需要加 `--no-check-certificate`。
- 示例里的 `8888` 是容器内的 Admin 端口（`admin.port`），如果改过要相应替换。

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
