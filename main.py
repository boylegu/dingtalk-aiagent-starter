# !/usr/bin/env python

import json
import argparse
import logging
import asyncio
import time
import os
from urllib.parse import urlparse, parse_qs
from typing import Optional

import aiohttp
from dotenv import load_dotenv
from dingtalk_stream import AckMessage
import dingtalk_stream


# Load environment variables
load_dotenv()

# Default configurations
DEFAULT_AIAGENT_URL = "http://127.0.0.1:8000/chat"
DEFAULT_MAX_CONCURRENCY = 100
DEFAULT_CONNECT_TIMEOUT = 1.0
DEFAULT_TOTAL_TIMEOUT = 8.0
DEFAULT_FALLBACK_TEXT = "当前请求较多或服务暂时不可用，请稍后再试。"
DEFAULT_LOG_LEVEL = "INFO"


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
        '--aiagent_url', dest='aiagent_url',
        default=os.getenv('AIAGENT_URL', DEFAULT_AIAGENT_URL),
        help=f'AI Agent service URL, default: {DEFAULT_AIAGENT_URL} (can also set via AIAGENT_URL environment variable)'
    )
    parser.add_argument(
        '--max_concurrency', dest='max_concurrency', type=int,
        default=int(os.getenv('MAX_CONCURRENCY', DEFAULT_MAX_CONCURRENCY)),
        help=f'Max concurrent requests, default: {DEFAULT_MAX_CONCURRENCY} (can also set via MAX_CONCURRENCY environment variable)'
    )
    parser.add_argument(
        '--connect_timeout', dest='connect_timeout', type=float,
        default=float(os.getenv('CONNECT_TIMEOUT', DEFAULT_CONNECT_TIMEOUT)),
        help=f'HTTP connection timeout in seconds, default: {DEFAULT_CONNECT_TIMEOUT} (can also set via CONNECT_TIMEOUT environment variable)'
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

    if args.connect_timeout <= 0 or args.total_timeout <= 0:
        parser.error("timeout values must be positive numbers.")

    return args


class DispatchHandler(dingtalk_stream.GraphHandler):
    def __init__(
        self,
        aiagent_url: str,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        total_timeout: float = DEFAULT_TOTAL_TIMEOUT,
        fallback_text: str = DEFAULT_FALLBACK_TEXT,
        test_mode: bool = False,
        logger=None
    ):
        super().__init__()
        self.logger = logger or logging.getLogger(__name__)
        self.aiagent_url = aiagent_url
        self.max_concurrency = max_concurrency
        self.connect_timeout = connect_timeout
        self.total_timeout = total_timeout
        self.fallback_text = fallback_text
        self.test_mode = test_mode

        self.semaphore = asyncio.Semaphore(max_concurrency)
        self._http_session = None
        self._timeout = aiohttp.ClientTimeout(
            total=total_timeout,
            connect=connect_timeout,
        )

    @property
    def http_session(self):
        """Lazy create http session in async context to avoid event loop error"""
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession(timeout=self._timeout)
        return self._http_session

    async def close(self):
        if self._http_session is not None:
            await self._http_session.close()

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
            thread_id = body.get("threadId") or body.get("thread_id") or ""
            conversation_token = query_params.get("conversationToken", [""])[0]

            session_key = f"{corp_id}:{sender}:{thread_id}" if thread_id else f"{corp_id}:{sender}"

            self.logger.info(
                "resolved fields traceId=%s sender=%s corpId=%s threadId=%s conversationToken=%s sessionKey=%s",
                trace_id, sender, corp_id, thread_id, conversation_token, session_key
            )

            # 并发保护：入口层不要无限制打下游
            async with self.semaphore:
                result = await self.forward_to_aiagent(
                    trace_id=trace_id,
                    sender=sender,
                    corp_id=corp_id,
                    input_text=input_text,
                    thread_id=thread_id,
                    conversation_token=conversation_token,
                    session_key=session_key,
                    attr_obj=attr_obj,
                )

            cost_ms = int((time.time() - start_ts) * 1000)
            self.logger.info(
                "request finished traceId=%s sender=%s cost_ms=%s",
                trace_id, sender, cost_ms
            )

            text = result.get("text") or result.get("answer") or self.fallback_text

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

    async def forward_to_aiagent(
        self,
        trace_id: str,
        sender: str,
        corp_id: str,
        input_text: str,
        thread_id: str,
        conversation_token: str,
        session_key: str,
        attr_obj: dict,
    ) -> dict:
        # Test mode: return test response directly without calling backend
        if self.test_mode:
            self.logger.info("test mode enabled, returning test response traceId=%s", trace_id)
            return {
                "text": f"🤖 测试模式已启用\n\n收到你的消息：{input_text}\n\n发送者ID：{sender}\n企业ID：{corp_id}\n会话ID：{thread_id}"
            }

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

        self.logger.info(
            "forwarding to aiagent traceId=%s url=%s payload=%s",
            trace_id, self.aiagent_url, json.dumps(payload, ensure_ascii=False)
        )

        try:
            async with self.http_session.post(self.aiagent_url, json=payload) as resp:
                resp_text = await resp.text()

                self.logger.info(
                    "aiagent response traceId=%s status=%s body=%s",
                    trace_id, resp.status, resp_text
                )

                if resp.status != 200:
                    return {
                        "text": self.fallback_text
                    }

                try:
                    return json.loads(resp_text)
                except Exception:
                    self.logger.warning("aiagent response is not valid json traceId=%s", trace_id)
                    return {
                        "text": resp_text or self.fallback_text
                    }

        except asyncio.TimeoutError:
            self.logger.warning("aiagent timeout traceId=%s", trace_id)
            return {
                "text": self.fallback_text
            }
        except Exception:
            self.logger.exception("failed to call aiagent traceId=%s", trace_id)
            return {
                "text": self.fallback_text
            }


def main():
    options = define_options()
    logger = setup_logger(options.log_level)

    credential = dingtalk_stream.Credential(options.client_id, options.client_secret)
    client = dingtalk_stream.DingTalkStreamClient(credential)

    handler = DispatchHandler(
        aiagent_url=options.aiagent_url,
        max_concurrency=options.max_concurrency,
        connect_timeout=options.connect_timeout,
        total_timeout=options.total_timeout,
        fallback_text=options.fallback_text,
        test_mode=options.test_mode,
        logger=logger
    )

    if options.test_mode:
        logger.info("✅ Test mode enabled: No backend AI service required, will return test responses directly")

    client.register_callback_handler(
        dingtalk_stream.GraphMessage.TOPIC,
        handler
    )

    try:
        client.start_forever()
    finally:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(handler.close())
            else:
                loop.run_until_complete(handler.close())
        except Exception:
            logger.exception("failed to close http session")


if __name__ == '__main__':
    print(main())