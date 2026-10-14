# Consumer Dispatcher (Go) — 产品需求文档（PRD）

| 字段 | 内容 |
|------|------|
| 产品名称 | Consumer Dispatcher（Go 版，目录 `go-cd`） |
| 版本 | 0.1（相对原 Java `consumer-dispatcher` 的重写） |
| 状态 | 已实现可运行 MVP |
| 技术基线 | Go 1.27+、RabbitMQ、YAML 配置 |
| 原项目 | [jackyhung/consumer-dispatcher](https://github.com/jackyhung/consumer-dispatcher)（Java） |
| 文档范围 | 产品目标、功能、架构、接口、配置、迁移、非目标与后续 |

---

## 1. 背景与问题

### 1.1 业务背景

在多站点、多队列场景下，业务消费逻辑常以 HTTP 服务（如 PHP MVC）实现。直接让各语言脚本长驻消费 RabbitMQ，存在：

- 扩缩容与运维成本高（按队列分别启停进程）
- 坏消息阻塞好消息时难以隔离
- 多站点共用一套运维手段困难

原 **Consumer Dispatcher** 作为 **RabbitMQ ↔ HTTP** 代理：从队列取消息，POST 到业务 URL，按约定响应决定 ack / 重试。

### 1.2 原版痛点

| 痛点 | 说明 |
|------|------|
| 技术栈老化 | JDK 8、JMX/MX4J、过时依赖 |
| 连接模型粗放 | 近似「一线程一 AMQP 连接」，高 job 数时连接膨胀 |
| 失败处理脆弱 | 重试后仍 ack，依赖可选 Redis；易丢消息或状态不清 |
| 配置与密钥 | XML + 明文密码入库风险 |
| 文档与代码漂移 | Master-Slave / ZooKeeper 等能力不完整或已失效 |
| 可观测性弱 | 缺少现代 metrics / 探针体系 |

### 1.3 重写目标

用 Go 重写为云原生友好的薄代理，**保持与业务 HTTP 契约兼容**，同时升级连接模型、配置、失败路径与运维面。

---

## 2. 产品定位

**一句话**：多站点 RabbitMQ 任务的轻量 HTTP 转发调度器。

**是**：

- 配置驱动的多队列消费者进程
- 将消息可靠地转发给 HTTP 消费者
- 提供 Admin API、指标与 DLQ 运维能力
- 提供 `job.xml → YAML` 迁移工具

**不是**：

- 业务工作流引擎（非 Temporal / 编排平台）
- 通用消息总线或 Kafka 替代
- 业务消费代码本身（消费逻辑仍在 HTTP 侧）

---

## 3. 目标用户与使用场景

| 角色 | 诉求 |
|------|------|
| 平台 / 基础设施 | 低资源跑大量队列；易部署（二进制 / Docker / K8s） |
| 业务研发 | 继续用 HTTP 写消费者；契约不变 |
| 运维 / SRE | 动态调并发、purge、看指标、重放失败任务 |
| 迁移负责人 | 从 Java + `job.xml` 平滑迁到 Go + YAML |

**典型场景**：

1. 单实例服务多个 RabbitMQ vhost / 站点、数十～数百队列  
2. 某队列堆积时在线增加 worker  
3. 坏消息进 DLQ，人工 peek / replay  
4. 用 xml2yaml 从现网 `job.xml` 生成首版 `config.yaml`

---

## 4. 成功标准

| 指标 | 目标 |
|------|------|
| 协议兼容 | 与旧版 HTTP 表单及成功判定一致，现有消费者无需改协议 |
| 连接效率 | 同一 RabbitMQ 端点共享 1 条 AMQP Connection；worker 用 Channel |
| 可运维 | Admin REST + Prometheus + liveness/readiness |
| 可迁移 | `xml2yaml` 能转换现网级 `job.xml` 并被 dispatcher 加载 |
| 质量基线 | `go test ./...`（含 race）通过；Go 1.27 + go-guidelines 风格 |

---

## 5. 功能需求

### 5.1 核心消费与转发（P0）

| ID | 需求 | 验收 |
|----|------|------|
| F-01 | 按配置连接 RabbitMQ，对每个 resolved job 启动 N 个 worker | worker 数 = `count` |
| F-02 | `basicConsume`，手动 ack | 成功或入 DLQ 后 ack；DLQ 发布失败则 nack+requeue；停机中断的消息 nack+requeue（不进 DLQ） |
| F-03 | HTTP POST `application/x-www-form-urlencoded`：`queueName`=job name，`bodyData`=消息体 | 与旧版一致 |
| F-04 | 可选 `Host` / `url_host` | 请求 Host 头可指定 |
| F-05 | 成功条件：HTTP 200 且 body trim 后大小写不敏感等于 `ok` | 单测覆盖 |
| F-06 | 失败重试：首次失败后再重试 `retry_limit` 次（`0` = 不重试）；指数退避 + jitter | 日志可见 attempt |
| F-07 | 重试耗尽写入 DLQ（默认 `{queue}.dlq`），broker 确认（publisher confirm + mandatory）后再 ack 原消息 | DLQ 可 peek/replay |
| F-08 | `block_patterns` 正则匹配；`discard_match_jobs=true` 时直接 ack 丢弃 | 可 Admin 动态改 |
| F-09 | 默认声明 exchange / queue / bind / DLQ；可 `declare_topology: false` | passive 校验主队列与 DLQ |

### 5.2 配置（P0）

| ID | 需求 | 验收 |
|----|------|------|
| C-01 | YAML 配置（`defaults` / `admin` / `servers` / `jobs`） | 示例配置可启动 |
| C-02 | `${VAR}`、`${VAR:-default}` 环境变量展开 | 单测覆盖 |
| C-03 | job 字段缺省约定与旧版一致（queue/exchange/type/dlq） | 文档 + 单测 |
| C-04 | `server` 支持字符串或字符串列表；列表展开为多逻辑 job | `name-server` 唯一 |
| C-05 | 密钥不强制入库；支持 env 注入 | `.env.example` |

### 5.3 连接与资源模型（P0）

| ID | 需求 | 验收 |
|----|------|------|
| R-01 | **每 RabbitMQ 端点（host/port/vhost/user）一条 Connection** | Hub 复用 |
| R-02 | 每 worker 使用独立 Channel 消费 | 断线可重连 |
| R-03 | 全局复用一个 HTTP Client | 并发安全 |
| R-04 | 连接断开后按 `reconnect_seconds` 重试 | 日志 `consume session ended` |

### 5.4 Admin / 运维面（P0）

替代原 JMX/MX4J，提供 HTTP API（详见第 8 节）：

- 任务列表 / 详情  
- 动态调 worker 数、URL、block patterns、discard  
- try 试投、purge、peek/take、DLQ 运维与 replay  

### 5.5 可观测性（P0）

| ID | 需求 |
|----|------|
| O-01 | JSON 结构化日志（stdout，`log/slog`） |
| O-02 | Prometheus：`/metrics`（处理结果、耗时、worker 数、inflight） |
| O-03 | `/healthz` 存活；`/readyz` 就绪（含 AMQP ping） |

### 5.6 生命周期（P0）

| ID | 需求 |
|----|------|
| L-01 | 响应 SIGINT/SIGTERM |
| L-02 | 停机顺序：Admin HTTP drain → 停消费者（等待处理中消息完成）→ 关闭 AMQP Hub |
| L-03 | 停机总超时 15s（覆盖以上全部步骤）；超时后中断处理中消息并 requeue |

### 5.7 迁移工具 xml2yaml（P0）

| ID | 需求 |
|----|------|
| M-01 | 解析旧 `job.xml`（含 `xmlns="tnc"`） |
| M-02 | 映射到 Go YAML 字段命名 |
| M-03 | 默认合并「同名且除 server 外配置相同」的 job 为 server 列表 |
| M-04 | 可选 `-env-secrets` 改写密码占位符 |
| M-05 | 对 Redis failed-job 配置输出 NOTE（Go 版改用 DLQ） |

### 5.8 后续（P1 / 非本版必达）

| ID | 需求 | 优先级 | 状态 |
|----|------|--------|------|
| F-10 | Admin API 鉴权（Token / mTLS） | P1 | 已完成：`admin.tokens`（Bearer）+ 可选 `admin.tls`（HTTPS / 客户端证书），仅保护 `/api/*`；写操作审计日志 |
| F-11 | 下游熔断（连续失败暂停 job） | P1 | **不做**（决策）：重试耗尽的消息直接进 DLQ，可 replay |
| F-12 | 失败日志可选记录 body（采样 / 截断） | P1 | 已完成：`log.failed_body`（默认关闭，`max_bytes` 截断、`sample_rate` 采样）；`log.level` 可配置 |
| F-13 | 多副本协调 purge（原 ZK 主从诉求的现代替代） | P2 | **不做**（决策）：purge 需求很少；队列由 RabbitMQ 共享，任一副本 purge 即全局生效，缺的只是"先暂停所有副本"，用运维流程（缩容到 1 个副本或逐个 Pod 把 worker 设为 0）即可解决，不值得引入跨副本广播 / 确认的复杂度。操作流程见 README「清空队列（purge）」 |
| F-14 | 配置热加载 | P2 | 已完成：`POST /api/reload`（支持 `dry_run`）与 SIGHUP；校验失败保持旧配置；按 job 增 / 删 / 更新 / 扩缩容 / 重建，Admin API 的临时修改仅在文件同一项变化时被覆盖；监听地址、TLS、日志输出需重启 |

---

## 6. 非功能需求

| 类别 | 要求 |
|------|------|
| 性能 | 定位为 I/O 密集薄代理；瓶颈应在 HTTP 消费者或 MQ，而非 dispatcher 本身 |
| 资源 | 共享连接 + 共享 HTTP client，控制 FD 与内存 |
| 安全 | 密码与 Admin token 走环境变量 / Secret；生产环境应配置 `admin.tokens`（可加 mTLS），并做网络隔离 |
| 兼容 | HTTP 契约与旧版一致；拓扑约定兼容 |
| 可维护 | 模块边界清晰；遵循 go-guidelines（Go 1.27） |
| 部署 | 单二进制；提供 Dockerfile |

---

## 7. 总体架构

```text
                         cmd/dispatcher
              load YAML → Manager.Start → Admin HTTP
                        /                \
                       v                  v
              internal/manager      internal/admin
               (Pool per job)      (REST + metrics)
                       |
         +-------------+-------------+
         v             v             v
      worker×N      worker×N        ...
         | channel     | channel
         +------+------+
                v
         internal/broker Hub  ----->  RabbitMQ (queues + DLQ)
           (1 conn / server)
                |
                | shared http.Client
                v
         internal/forward  -------->  HTTP Consumers
```

### 7.1 包职责

| 包 / 命令 | 职责 |
|-----------|------|
| `cmd/dispatcher` | 进程入口、信号、优雅停机 |
| `cmd/xml2yaml` | `job.xml` → YAML |
| `internal/config` | YAML 加载、env 展开、job 解析与 server 列表展开 |
| `internal/broker` | AMQP Connection Hub |
| `internal/worker` | 消费循环、重试、DLQ、Pool 扩缩 |
| `internal/forward` | HTTP POST 契约 |
| `internal/manager` | 聚合 Pool、Admin 操作、就绪状态 |
| `internal/admin` | REST、探针、Prometheus、panic recovery |
| `internal/metrics` | 指标定义 |
| `internal/xmlconv` | XML→YAML 转换逻辑 |

### 7.2 关键设计决策

1. **Connection vs Channel**：并发用 Channel，连接按 server 复用。  
2. **失败进 DLQ 而非 Redis**：语义更清晰，可用 RabbitMQ 原生命令运维。  
3. **Admin 用 HTTP 而非 JMX**：适配 K8s / curl / 网关。  
4. **不在 struct 中长期存放 `context.Context`**：生命周期用 `chan struct{}`；请求级 context 临时派生。  
5. **不做嵌入式 ZK 主从，也不做跨副本协调**：多副本依赖外部编排；多副本下 purge 按 README 的运维流程操作（见 F-13）。

---

## 8. 接口与契约

### 8.1 与 HTTP 消费者的契约（兼容旧版）

**请求**

- Method: `POST`  
- Content-Type: `application/x-www-form-urlencoded`  
- 字段：  
  - `queueName`：job 的 `name`（历史命名，不是 queue 名）  
  - `bodyData`：MQ 消息体字符串  

**成功**

- Status `200`  
- Body（trim 后）大小写不敏感等于 `ok`

**其它**

- 均视为失败，进入重试 / DLQ 路径

### 8.2 Admin HTTP API

默认监听 `admin.host:admin.port`。

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/healthz` | 存活 |
| GET | `/readyz` | 就绪（AMQP ping） |
| GET | `/metrics` | Prometheus |
| POST | `/api/reload` | 重新加载配置文件（`?dry_run=true` 只预览）；也可发 SIGHUP |
| GET | `/api/jobs` | 任务列表 |
| GET | `/api/jobs/{id}` | 详情（`name-server`，或仅绑定一个 server 时的 `name`；有歧义返回 409） |
| POST | `/api/jobs/{id}/workers` | `{"count":N}`，0–256 |
| POST | `/api/jobs/{id}/url` | `{"url":"..."}` |
| POST | `/api/jobs/{id}/block-patterns` | `{"patterns":"..."}` |
| POST | `/api/jobs/{id}/discard-match` | `{"discard":true}` |
| POST | `/api/jobs/{id}/try` | `{"body":"..."}` |
| POST | `/api/jobs/{id}/purge` | 清空主队列 |
| POST | `/api/jobs/{id}/purge-dlq` | 清空 DLQ |
| GET | `/api/jobs/{id}/peek?count=10` | 窥视（不 ack） |
| POST | `/api/jobs/{id}/take?count=10` | 取出（ack） |
| GET | `/api/jobs/{id}/dlq?count=10` | 窥视 DLQ |
| POST | `/api/jobs/{id}/replay-dlq` | `{"count":10}` 重放（确认重新发布后才 ack DLQ 消息） |

`count` 取值 1–100，缺省 10。错误码：404 job 不存在；409 job 名有歧义；400 参数非法；413 请求体超过 1 MiB。

### 8.3 指标（Prometheus）

| 指标 | 含义 |
|------|------|
| `cd_jobs_processed_total{job,outcome}` | outcome: success / failed / discarded / dlq / requeued（停机中断） |
| `cd_job_duration_seconds{job}` | HTTP 转发耗时 |
| `cd_workers{job}` | 当前 worker 数 |
| `cd_inflight_jobs{job}` | 处理中消息数 |

### 8.4 日志

- 格式：JSON；默认 stdout，配置 `log.file.path` 后写入文件（按大小切割、保留个数 / 天数、gzip 压缩），`log.stdout` 控制是否同时输出到终端  
- 级别：默认 Info，`log.level` 可配置（debug / info / warn / error）  
- 消费成功：`forward ok`（Debug 级别，含 job、attempt、result）  
- 失败：每次尝试打 `forward failed`（含 result/err），进入 DLQ 打 `message dead-lettered`；**默认不打印 body**，`log.failed_body` 开启后附带截断、采样后的 body  
- 审计：`/api/*` 写操作打 `admin api`（method、path、status、identity、remote），鉴权失败打 `admin auth failed`；token 不写入日志  
- 与旧版差异：不再按 server 写滚动文件；坏任务以 DLQ 为主

---

## 9. 配置设计

### 9.1 结构

```yaml
defaults: { url, url_host, count, timeout_ms, encoding, retry_limit, prefetch, declare_topology }
admin:    { host, port, tokens, tls: { cert_file, key_file, client_ca_file } }
log:      { level, file: { path, max_size_mb, max_backups, max_age_days, compress, local_time }, stdout, failed_body: { enabled, max_bytes, sample_rate } }
servers:  { <name>: { host, port, username, password, vhost, reconnect_seconds } }
jobs:     [ { name, server, queue, exchange, ... } ]
```

### 9.2 约定缺省

| 字段 | 缺省 |
|------|------|
| `queue` | `name` |
| `exchange` | `{name}_router` |
| `type` | `direct` |
| `dlq` | `{queue}.dlq` |
| `declare_topology` | `true` |

### 9.3 多 server

```yaml
- name: text_mail
  server: [site_a, site_b]   # 或 server: site_a
```

展开为逻辑名：`text_mail-site_a`、`text_mail-site_b`。  
**连接数** = 去重后的 RabbitMQ 端点数（非 queue 数、非 worker 数）。

### 9.4 环境变量

支持 `${VAR}`、`${VAR:-default}`。示例见 `.env.example`、`configs/config.example.yaml`。

展开在 YAML 解析之后、只作用于标量值：环境变量的值按字面使用（可含 `#`、`:` 等），裸 `$` 不展开。flow 风格（`{...}`、`[...]`）中的 `${VAR}` 需加引号。

---

## 10. 消息处理流程

```text
Consume delivery
    │
    ├─ match block_patterns?
    │     └─ discard_match_jobs? → ack + outcome=discarded
    │
    ├─ HTTP forward (1 + retry_limit 次，退避)
    │     ├─ ok? → ack + outcome=success
    │     └─ 停机中断? → nack requeue + outcome=requeued
    │
    └─ publish DLQ（confirm + mandatory）
          ├─ ok → ack + outcome=dlq
          └─ fail → nack requeue + outcome=failed
```

**Ack 语义相对旧版的改进**：旧版重试失败后仍 ack（消息离开主队列，仅可能留在 Redis/日志）；Go 版显式落入 DLQ，便于重放。

---

## 11. 迁移设计（xml2yaml）

### 11.1 命令

```bash
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml -env-secrets
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml -merge=false
```

### 11.2 主要字段映射

| XML | YAML |
|-----|------|
| `request-pre` | `defaults.url` |
| `request-count` / `request-timeout` | `defaults.count` / `timeout_ms` |
| `monitor.@httpHost/Port` | `admin` |
| `queueserver` | `servers.<name>` |
| `job.@timeout` | `timeout_ms` |
| `job.@urlhost` | `url_host` |
| `job.@retryLimit` | `retry_limit` |
| `job.@routeKey` | `route_key` |
| `job.@blockPatterns` | `block_patterns` |

### 11.3 合并策略

指纹 = 除 `server` 外的 job 配置；相同则合并为 `server: [a,b,...]`。  
若同名但 URL 等不同（多站点常见），**不合并**，保留多条 job。

### 11.4 迁移检查清单

1. 转换并人工审阅密码与 URL  
2. `go run ./cmd/dispatcher -config ...` 确认 jobs/servers 数量  
3. 在预发验证一条真实队列 e2e  
4. 确认 DLQ 命名与运维手册  
5. Admin 端口网络策略  

---

## 12. 与旧版 Java 对照

| 维度 | Java 原版 | Go（go-cd） |
|------|-----------|-------------|
| 语言 | Java 8 | Go 1.27 |
| 配置 | XML | YAML + env |
| 运维 | JMX + MX4J | REST Admin |
| 连接 | 近一线程一连接 | 一 server 一连接 + 多 Channel |
| 失败 | 可选 Redis + 文件日志 | DLQ + stdout JSON |
| 主从 | ZK（文档/残缺） | 不实现；靠 K8s 多副本 |
| 迁移 | — | `xml2yaml` |

---

## 13. 目录与交付物

```text
go-cd/
├── cmd/dispatcher/     # 主进程
├── cmd/xml2yaml/       # 迁移工具
├── internal/...        # 业务包
├── configs/            # 示例配置
├── docs/PRD.md         # 本文档
├── Dockerfile
├── README.md           # 使用说明
├── .env.example
└── go.mod              # module consumer-dispatcher
```

**运行依赖**：RabbitMQ；下游 HTTP 消费者；Go 1.27+（构建时）。

---

## 14. 风险与约束

| 风险 | 缓解 |
|------|------|
| Admin 未配置鉴权 | 已支持 Token / mTLS；未配置时启动打 Warn，仍建议配合内网 / NetworkPolicy |
| 多副本下 purge 时其他副本仍在消费 | 按 README「清空队列（purge）」流程：先缩容到 1 个副本，或逐个 Pod 把 worker 设为 0，再 purge（F-13 决策不做自动协调） |
| 成功日志量过大 | 成功日志已降为 Debug；`log.level` 可调 |
| `declare_topology` 与现网拓扑冲突 | 支持关闭声明，改用 passive |
| 同名 job 不同 URL | 不合并；逻辑名带 server 后缀 |

---

## 15. 里程碑（回顾）

| 阶段 | 内容 | 状态 |
|------|------|------|
| M1 | 核心消费 + YAML + HTTP 契约 | 已完成 |
| M2 | Hub 连接复用、共享 HTTP、优雅停机、DLQ | 已完成 |
| M3 | Admin API、metrics、探针 | 已完成 |
| M4 | 多 server 列表、xml2yaml、目录独立 go-cd | 已完成 |
| M5 | Admin 鉴权、熔断、日志策略增强 | 已完成（熔断按决策不做） |

---

## 16. 附录：快速命令

```bash
cd go-cd

# 运行
cp configs/config.example.yaml configs/config.yaml
go run ./cmd/dispatcher -config configs/config.yaml

# 迁移
go run ./cmd/xml2yaml -in ../job.xml -out configs/config.yaml

# 测试
go test ./... -race

# 构建
go build -o bin/dispatcher ./cmd/dispatcher
go build -o bin/xml2yaml ./cmd/xml2yaml
```

---

## 17. 文档维护

- 使用说明以 `README.md` 为准（命令与 API 速查）  
- 产品与设计以本文 `docs/PRD.md` 为准  
- 行为变更时同步更新：契约、配置字段、Admin API、与旧版差异表  
