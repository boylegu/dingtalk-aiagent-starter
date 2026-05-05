# DingTalk AI Agent Starter
> 🚀 超轻量钉钉AI助理代理服务，基于 AgentFlowBus 消息总线，专注于消息转发，无冗余功能

## ✨ 核心特性
- 🎯 **极简设计**：仅做消息接收、转发、流量控制、日志记录、兜底响应，**无RAG、无记忆、无复杂会话逻辑**
- 📡 **消息总线架构**：通过 AgentFlowBus (NATS) 以 publish 模式投递消息，接入层不阻塞等待下游响应
- 🚀 **开箱即用**：支持命令行参数、环境变量、配置文件多种配置方式
- 🛡️ **高可用**：内置并发控制、超时机制、异常兜底，服务永不崩溃
- 🧪 **测试模式**：无需后端AI服务即可调试钉钉集成
- 📦 **部署友好**：支持Docker、K8s、裸机多种部署方式
- 📝 **完整日志**：全链路traceId追踪，便于问题排查

## 🏗️ 架构说明

```
钉钉用户 → 钉钉开放平台 → 本服务(接入层) → AgentFlowBus(NATS) → 下游AI Agent
                                              ↑                           ↓
                                              └──────── 自行回推钉钉 ───────┘
```

本服务作为**纯接入层**，职责单一：
1. 接收钉钉实时消息（Stream模式）
2. 解析并标准化消息格式
3. 通过 `bus.publish()` 将消息投递到 AgentFlowBus
4. 立即返回确认响应给钉钉用户

**下游AI Agent**需要自行：
- 订阅 AgentFlowBus 的 `agent.message.received` 事件
- 处理完成后，凭消息中的 `conversation_token` / `user_id` / `corp_id` 等信息，自行调用钉钉API回推结果

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置钉钉应用
> 先在 [钉钉开放平台](https://open-dev.dingtalk.com/) 创建AI助理应用，获取 `client_id` (app_key) 和 `client_secret` (app_secret)

### 3. 配置 AgentFlowBus
确保 NATS 服务器可访问，默认地址 `nats://localhost:4222`

### 4. 运行服务
#### 方式一：命令行参数
```bash
python main.py \
  --client_id <your_dingtalk_app_key> \
  --client_secret <your_dingtalk_app_secret> \
  --bus_url nats://localhost:4222 \
  --agent_id dingtalk-proxy
```

#### 方式二：使用 .env 文件
```bash
# 复制配置模板
cp .env.example .env
# 编辑 .env 填入你的配置
python main.py
```

#### 方式三：测试模式（无需后端AI服务）
```bash
python main.py \
  --client_id <your_dingtalk_app_key> \
  --client_secret <your_dingtalk_app_secret> \
  --test_mode
```
> 测试模式下会直接返回包含消息详情的测试回复，方便调试钉钉集成

## 🔌 AI Agent 接入协议

下游AI Agent 需要订阅 AgentFlowBus 的 `agent.message.received` 事件来接收消息。

### 事件格式

本服务会向 AgentFlowBus 发布 `agent.message.received` 事件，payload 结构如下：

```json
{
  "channel": "dingtalk",
  "tenant_id": "<corp_id>",
  "user_id": "<sender_id>",
  "thread_id": "<conversation_thread_id>",
  "conversation_token": "<dingtalk_conversation_token>",
  "session_hint": "<unique_session_identifier>",
  "text": "<user_input_text>",
  "message_type": "text",
  "trace_id": "<request_trace_id>",
  "metadata": {
    "source": "dingtalk-stream",
    "attr": "<raw_dingtalk_message_attributes>"
  }
}
```

#### 字段详细说明
| 字段 | 类型 | 说明 |
|------|------|------|
| `channel` | string | 消息来源渠道，固定为 `dingtalk` |
| `tenant_id` | string | 钉钉企业ID（corpId） |
| `user_id` | string | 钉钉用户ID（sender） |
| `thread_id` | string | 会话线程ID，同一对话上下文的消息会携带相同的 thread_id |
| `conversation_token` | string | 钉钉会话凭证，下游可凭此调用钉钉API（如发送消息卡片、@用户等）回推结果 |
| `session_hint` | string | 会话唯一标识，格式为 `{tenant_id}:{user_id}:{thread_id}` 或 `{tenant_id}:{user_id}` |
| `text` | string | 用户发送的原始消息文本内容 |
| `message_type` | string | 消息类型，默认 `text` |
| `trace_id` | string | 请求链路ID，全链路日志追踪使用 |
| `metadata` | object | 扩展元数据，包含钉钉原始消息的完整属性信息 |

### 下游回推钉钉

下游AI Agent 处理完消息后，需要**自行调用钉钉API**将结果推送给用户。推荐使用 `conversation_token` 调用钉钉开放平台的机器人消息发送接口。

> ⚠️ **注意**：接入层不再等待下游响应，也不提供回调接口。下游必须自行实现钉钉消息推送能力。

## ⚙️ 配置选项
所有配置都支持命令行参数和环境变量两种方式：

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--client_id` | `DINGTALK_CLIENT_ID` | 必填 | 钉钉应用app_key |
| `--client_secret` | `DINGTALK_CLIENT_SECRET` | 必填 | 钉钉应用app_secret |
| `--agent_id` | `AGENT_ID` | `dingtalk-proxy` | 本服务在AgentFlowBus中的Agent ID |
| `--bus_url` | `BUS_URL` | `nats://localhost:4222` | 消息总线地址 |
| `--subject_prefix` | `SUBJECT_PREFIX` | `acp.v1` | AgentFlowBus subject前缀 |
| `--tenant` | `TENANT` | `` | 租户标识，用于多租户场景下的subject隔离 |
| `--max_concurrency` | `MAX_CONCURRENCY` | `100` | 最大并发publish请求数 |
| `--total_timeout` | `TOTAL_TIMEOUT` | `8.0` | publish超时时间（秒） |
| `--fallback_text` | `FALLBACK_TEXT` | `当前请求较多或服务暂时不可用，请稍后再试。` | 服务异常时的兜底回复 |
| `--log_level` | `LOG_LEVEL` | `INFO` | 日志级别 |
| `--test_mode` | `TEST_MODE` | `false` | 测试模式开关 |

## 📦 部署建议
### Docker 部署
```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
CMD ["python", "main.py"]
```

### systemd 部署
```ini
[Unit]
Description=DingTalk AI Agent Service
After=network.target

[Service]
User=www-data
WorkingDirectory=/path/to/project
Environment="DINGTALK_CLIENT_ID=xxx"
Environment="DINGTALK_CLIENT_SECRET=xxx"
Environment="BUS_URL=nats://localhost:4222"
Environment="AGENT_ID=dingtalk-proxy"
ExecStart=/usr/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
