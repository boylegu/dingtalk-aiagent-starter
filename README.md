# DingTalk AI Agent Starter
> 🚀 超轻量钉钉AI助理代理服务，专注于消息转发，无冗余功能

## ✨ 核心特性
- 🎯 **极简设计**：仅做消息接收、转发、流量控制、日志记录、兜底响应，**无RAG、无记忆、无复杂会话逻辑**
- 🚀 **开箱即用**：支持命令行参数、环境变量、配置文件多种配置方式
- 🛡️ **高可用**：内置并发控制、超时机制、异常兜底，服务永不崩溃
- 🧪 **测试模式**：无需后端AI服务即可调试钉钉集成
- 📦 **部署友好**：支持Docker、K8s、裸机多种部署方式
- 📝 **完整日志**：全链路traceId追踪，便于问题排查

## 🚀 快速开始

### 1. 安装依赖
```bash
pip install -r requirements.txt
```

### 2. 配置钉钉应用
> 先在 [钉钉开放平台](https://open-dev.dingtalk.com/) 创建AI助理应用，获取 `client_id` (app_key) 和 `client_secret` (app_secret)

### 3. 运行服务
#### 方式一：命令行参数
```bash
python main.py \
  --client_id <your_dingtalk_app_key> \
  --client_secret <your_dingtalk_app_secret> \
  --aiagent_url http://your-ai-service:port/chat
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

## 🔌 AI Agent 接口协议
本服务作为代理，会将钉钉消息标准化后转发到你的AI服务，你只需要实现一个简单的HTTP接口即可。

### 请求格式
服务会向你配置的 `aiagent_url` 发送 `POST` 请求，`Content-Type: application/json`，请求体如下：
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
| `channel` | string | 消息来源渠道，固定为 `dingtalk`，便于多渠道接入时区分来源 |
| `tenant_id` | string | 钉钉企业ID（corpId），唯一标识消息来自哪个企业组织 |
| `user_id` | string | 钉钉用户ID（sender），唯一标识发送消息的具体用户 |
| `thread_id` | string | 会话线程ID，同一个对话上下文的消息会携带相同的thread_id，可用于实现多轮会话 |
| `conversation_token` | string | 钉钉会话凭证，如果需要调用钉钉开放平台的高级接口（如发送富文本卡片、@指定用户、上传附件等），需要使用此token |
| `session_hint` | string | 会话唯一标识，格式为 `{tenant_id}:{user_id}:{thread_id}`（单聊场景）或 `{tenant_id}:{user_id}`（群聊场景），可直接作为会话缓存的key |
| `text` | string | 用户发送的原始消息文本内容 |
| `message_type` | string | 消息类型，默认是 `text`，后续可扩展支持图片、语音等其他类型消息 |
| `trace_id` | string | 请求链路ID，全链路日志追踪使用，排查问题时可通过此ID关联所有相关日志 |
| `metadata` | object | 扩展元数据，包含钉钉原始消息的完整属性信息，可用于获取消息的额外字段（如群聊ID、@列表等） |

### 响应格式
你的AI服务需要返回JSON响应，**至少包含以下任意一个字段**即可：
```json
{
  "text": "<response_content>",
  "answer": "<response_content>"
}
```
> 如果返回多个字段，优先使用 `text`，其次是 `answer`，内容会原样返回给钉钉用户

## ⚙️ 配置选项
所有配置都支持命令行参数和环境变量两种方式：

| 参数 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--client_id` | `DINGTALK_CLIENT_ID` | 必填 | 钉钉应用app_key |
| `--client_secret` | `DINGTALK_CLIENT_SECRET` | 必填 | 钉钉应用app_secret |
| `--aiagent_url` | `AIAGENT_URL` | `http://127.0.0.1:8000/chat` | 后端AI服务地址 |
| `--max_concurrency` | `MAX_CONCURRENCY` | `100` | 最大并发请求数，超过会排队 |
| `--connect_timeout` | `CONNECT_TIMEOUT` | `1.0` | HTTP连接超时时间（秒） |
| `--total_timeout` | `TOTAL_TIMEOUT` | `8.0` | 总请求超时时间（秒） |
| `--fallback_text` | `FALLBACK_TEXT` | `当前请求较多或服务暂时不可用，请稍后再试。` | 服务异常时的兜底回复 |
| `--log_level` | `LOG_LEVEL` | `INFO` | 日志级别，可选：DEBUG/INFO/WARNING/ERROR/CRITICAL |
| `--test_mode` | `TEST_MODE` | `false` | 测试模式开关，开启后无需后端AI服务 |

## 📦 部署建议
### Docker 部署
```dockerfile
FROM python:3.9-slim
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
Environment="AIAGENT_URL=http://your-ai-service/chat"
ExecStart=/usr/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```
