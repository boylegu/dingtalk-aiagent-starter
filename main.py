# !/usr/bin/env python

import json
import argparse
import logging
import asyncio
import time
import os
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv
from dingtalk_stream import AckMessage
import dingtalk_stream

from agentflowbus import Bus, NATSDriver, Envelope, MessageReceived

# Load environment variables
load_dotenv()

# Default configurations
DEFAULT_AGENT_ID = "dingtalk-proxy"
DEFAULT_BUS_URL = "nats://localhost:4222"
DEFAULT_SUBJECT_PREFIX = "acp.v1"
DEFAULT_TENANT = ""
DEFAULT_MAX_CONCURRENCY = 100
DEFAULT_TOTAL_TIMEOUT = 8.0
DEFAULT_FALLBACK_TEXT = "当前请求较多或服务暂时不可用，请稍后再试。"
DEFAULT_LOG_LEVEL = "INFO"

THREAD_ID_KEYS = (
    "threadId",
    "thread_id",
    "conversationThreadId",
    "conversationId",
    "openConversationId",
    "chatId",
)


def setup_logger(log_level: str = DEFAULT_LOG_LEVEL):
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            '%(asctime)s %(name)-8s %(levelname)-8s %(message)s [%(filename)s:%(lineno)d]'
        )
    )
    logger.addHandler(handler)
    logger.setLevel(log_level.upper())
    return logger


def define_options():
    parser = argparse.ArgumentParser(description="DingTalk AI Agent Proxy Service")
    parser.add_argument(
        '--client_id', dest='client_id',
        default=os.getenv('DINGTALK_CLIENT_ID'),
        help='DingTalk app_key or suite_key (can also set via DINGTALK_CLIENT_ID environment variable)'
    )
    parser.add_argument(
        '--client_secret', dest='client_secret',
        default=os.getenv('DINGTALK_CLIENT_SECRET'),
        help='DingTalk app_secret or suite_secret (can also set via DINGTALK_CLIENT_SECRET environment variable)'
    )
    parser.add_argument(
        '--agent_id', dest='agent_id',
        default=os.getenv('AGENT_ID', DEFAULT_AGENT_ID),
        help=f'Agent ID for this proxy service, default: {DEFAULT_AGENT_ID} (can also set via AGENT_ID environment variable)'
    )
    parser.add_argument(
        '--bus_url', dest='bus_url',
        default=os.getenv('BUS_URL', DEFAULT_BUS_URL),
        help=f'Message bus URL, default: {DEFAULT_BUS_URL} (can also set via BUS_URL environment variable)'
    )
    parser.add_argument(
        '--subject_prefix', dest='subject_prefix',
        default=os.getenv('SUBJECT_PREFIX', DEFAULT_SUBJECT_PREFIX),
        help=f'Subject prefix for agentflowbus, default: {DEFAULT_SUBJECT_PREFIX} (can also set via SUBJECT_PREFIX environment variable)'
    )
    parser.add_argument(
        '--tenant', dest='tenant',
        default=os.getenv('TENANT', DEFAULT_TENANT),
        help='Tenant identifier for agentflowbus (can also set via TENANT environment variable)'
    )
    parser.add_argument(
        '--max_concurrency', dest='max_concurrency', type=int,
        default=int(os.getenv('MAX_CONCURRENCY', DEFAULT_MAX_CONCURRENCY)),
        help=f'Max concurrent publish requests, default: {DEFAULT_MAX_CONCURRENCY} (can also set via MAX_CONCURRENCY environment variable)'
    )
    parser.add_argument(
        '--total_timeout', dest='total_timeout', type=float,
        default=float(os.getenv('TOTAL_TIMEOUT', DEFAULT_TOTAL_TIMEOUT)),
        help=f'Total request timeout in seconds, default: {DEFAULT_TOTAL_TIMEOUT} (can also set via TOTAL_TIMEOUT environment variable)'
    )
    parser.add_argument(
        '--fallback_text', dest='fallback_text',
        default=os.getenv('FALLBACK_TEXT', DEFAULT_FALLBACK_TEXT),
        help='Fallback response text when service is unavailable (can also set via FALLBACK_TEXT environment variable)'
    )
    parser.add_argument(
        '--log_level', dest='log_level',
        default=os.getenv('LOG_LEVEL', DEFAULT_LOG_LEVEL),
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help=f'Log level, default: {DEFAULT_LOG_LEVEL} (can also set via LOG_LEVEL environment variable)'
    )
    parser.add_argument(
        '--test_mode', dest='test_mode',
        action='store_true',
        default=os.getenv('TEST_MODE', 'false').lower() in ('true', '1', 'yes'),
        help='Enable test mode: no backend AI service required, directly return test responses (can also set via TEST_MODE environment variable)'
    )

    args = parser.parse_args()

    # Validate required parameters
    if not args.client_id or not args.client_secret:
        parser.error("client_id and client_secret are required. Please provide them via command line arguments or environment variables.")

    if args.max_concurrency <= 0:
        parser.error("max_concurrency must be a positive integer.")

    if args.total_timeout <= 0:
        parser.error("timeout values must be positive numbers.")

    return args


def first_present_string(*values):
    for value in values:
        if isinstance(value, str) and value:
            return value
    return ""


def find_nested_string(value, keys):
    if isinstance(value, dict):
        for key in keys:
            item = value.get(key)
            if isinstance(item, str) and item:
                return item
        for item in value.values():
            found = find_nested_string(item, keys)
            if found:
                return found
    elif isinstance(value, list):
        for item in value:
            found = find_nested_string(item, keys)
            if found:
                return found
    return ""


class DispatchHandler(dingtalk_stream.GraphHandler):
    def __init__(
        self,
        bus: Bus,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        total_timeout: float = DEFAULT_TOTAL_TIMEOUT,
        fallback_text: str = DEFAULT_FALLBACK_TEXT,
        test_mode: bool = False,
        logger=None
    ):
        super().__init__()
        self.logger = logger or logging.getLogger(__name__)
        self.bus = bus
        self.max_concurrency = max_concurrency
        self.total_timeout = total_timeout
        self.fallback_text = fallback_text
        self.test_mode = test_mode

        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._bus_lock = asyncio.Lock()
        self._bus_ready = False

    async def _ensure_bus(self):
        if self._bus_ready:
            return
        async with self._bus_lock:
            if self._bus_ready:
                return
            await self.bus.connect()
            self._bus_ready = True
            self.logger.info("agentflowbus connected to %s", self.bus.transport._url)

    async def close(self):
        await self.bus.close()

    async def process(self, callback: dingtalk_stream.CallbackMessage):
        start_ts = time.time()

        try:
            request = dingtalk_stream.GraphRequest.from_dict(callback.data)

            self.logger.info(
                "incoming request, method=%s, uri=%s, body=%s",
                request.request_line.method,
                request.request_line.uri,
                request.body
            )

            parsed_uri = urlparse(request.request_line.uri or "")
            query_params = parse_qs(parsed_uri.query)

            body = {}
            if request.body:
                try:
                    body = json.loads(request.body)
                except Exception:
                    self.logger.exception("failed to parse request body")

            raw_attr = body.get("attribute", "")
            attr_obj = {}
            if raw_attr:
                try:
                    attr_obj = json.loads(raw_attr)
                except Exception:
                    self.logger.warning("attribute is not valid json: %s", raw_attr)

            trace_id = callback.data.get("headers", {}).get("traceId", "")
            sender = body.get("sender", "")
            corp_id = body.get("corpId", "")
            input_text = body.get("input", "")
            thread_id = first_present_string(
                body.get("threadId"),
                body.get("thread_id"),
                query_params.get("threadId", [""])[0],
                find_nested_string(attr_obj, THREAD_ID_KEYS),
                find_nested_string(callback.data, THREAD_ID_KEYS),
            )
            conversation_token = first_present_string(
                query_params.get("conversationToken", [""])[0],
                body.get("conversationToken"),
                body.get("conversation_token"),
                find_nested_string(callback.data, ("conversationToken", "conversation_token")),
            )

            session_key = f"{corp_id}:{sender}:{thread_id}" if thread_id else f"{corp_id}:{sender}"

            self.logger.info(
                "resolved fields traceId=%s sender=%s corpId=%s threadId=%s conversationToken=%s sessionKey=%s",
                trace_id, sender, corp_id, thread_id, conversation_token, session_key
            )

            # Test mode: return test response directly
            if self.test_mode:
                diagnostics = {
                    "requestUri": request.request_line.uri,
                    "query": {key: values[0] if len(values) == 1 else values for key, values in query_params.items()},
                    "bodyKeys": sorted(body.keys()),
                    "attributeKeys": sorted(attr_obj.keys()) if isinstance(attr_obj, dict) else [],
                }
                text = (
                    f"🤖 测试模式已启用\n\n"
                    f"收到你的消息：{input_text}\n\n"
                    f"发送者ID：{sender}\n"
                    f"企业ID：{corp_id}\n"
                    f"会话ID：{thread_id or '未下发'}\n"
                    f"conversationToken：{conversation_token or '未下发'}"
                )
                response = dingtalk_stream.GraphResponse()
                response.status_line.code = 200
                response.status_line.reason_phrase = "OK"
                response.headers["Content-Type"] = "application/json"
                response.body = json.dumps({
                    "text": text,
                    "input": input_text,
                    "sender": sender,
                    "corpId": corp_id,
                    "threadId": thread_id,
                    "conversationToken": conversation_token,
                    "diagnostics": diagnostics,
                }, ensure_ascii=False)
                return AckMessage.STATUS_OK, response.to_dict()

            # Ensure bus is connected
            await self._ensure_bus()

            # Build and publish message
            payload = {
                "channel": "dingtalk",
                "tenant_id": corp_id,
                "user_id": sender,
                "thread_id": thread_id,
                "conversation_token": conversation_token,
                "session_hint": session_key,
                "text": input_text,
                "message_type": attr_obj.get("msgType", "text"),
                "trace_id": trace_id,
                "metadata": {
                    "source": "dingtalk-stream",
                    "attr": attr_obj,
                }
            }

            env = Envelope.new(MessageReceived)
            env.trace_id = trace_id
            env.session_id = session_key
            env.conversation_id = conversation_token
            env.tenant_id = corp_id
            env.user_id = sender
            env.channel = "dingtalk"
            env.from_ = self.bus.agent_id
            env.payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")

            self.logger.info(
                "publishing message traceId=%s event_type=%s",
                trace_id, env.event_type
            )

            async with self.semaphore:
                try:
                    await self.bus.publish(env)
                    text = "已收到，正在处理中..."
                except Exception:
                    self.logger.exception("failed to publish message traceId=%s", trace_id)
                    text = self.fallback_text

            cost_ms = int((time.time() - start_ts) * 1000)
            self.logger.info(
                "request accepted traceId=%s sender=%s cost_ms=%s",
                trace_id, sender, cost_ms
            )

            response = dingtalk_stream.GraphResponse()
            response.status_line.code = 200
            response.status_line.reason_phrase = "OK"
            response.headers["Content-Type"] = "application/json"
            response.body = json.dumps({
                "text": text,
                "input": input_text,
                "sender": sender,
                "corpId": corp_id,
                "threadId": thread_id,
                "conversationToken": conversation_token,
            }, ensure_ascii=False)

            return AckMessage.STATUS_OK, response.to_dict()

        except Exception:
            self.logger.exception("unexpected error in dispatch handler")

            response = dingtalk_stream.GraphResponse()
            response.status_line.code = 200
            response.status_line.reason_phrase = "OK"
            response.headers["Content-Type"] = "application/json"
            response.body = json.dumps({
                "text": self.fallback_text
            }, ensure_ascii=False)

            return AckMessage.STATUS_OK, response.to_dict()


async def run_services(options, logger):
    credential = dingtalk_stream.Credential(options.client_id, options.client_secret)
    client = dingtalk_stream.DingTalkStreamClient(credential, logger=logger)

    transport = NATSDriver(url=options.bus_url, name=options.agent_id)
    bus = Bus(
        agent_id=options.agent_id,
        transport=transport,
        tenant=options.tenant,
        subject_prefix=options.subject_prefix,
        default_timeout=options.total_timeout,
        logger=logger,
    )

    handler = DispatchHandler(
        bus=bus,
        max_concurrency=options.max_concurrency,
        total_timeout=options.total_timeout,
        fallback_text=options.fallback_text,
        test_mode=options.test_mode,
        logger=logger
    )

    if options.test_mode:
        logger.info("✅ Test mode enabled: No backend AI service required")

    client.register_callback_handler(
        dingtalk_stream.GraphMessage.TOPIC,
        handler
    )

    try:
        await client.start()
    finally:
        await handler.close()
        logger.info("Shutdown complete")


def main():
    options = define_options()
    logger = setup_logger(options.log_level)

    try:
        asyncio.run(run_services(options, logger))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == '__main__':
    print(main())
