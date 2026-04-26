#!/usr/bin/env python3
"""
无代理直接测试API
"""

import requests
import os

# 禁用代理
os.environ.pop('HTTP_PROXY', None)
os.environ.pop('http_proxy', None)
os.environ.pop('HTTPS_PROXY', None)
os.environ.pop('https_proxy', None)

# 创建不使用代理的session
session = requests.Session()
session.trust_env = False

BASE_URL = "http://127.0.0.1:8000"

try:
    print("测试 /health 端点...")
    resp = session.get(f"{BASE_URL}/health", timeout=5)
    print(f"状态码: {resp.status_code}")
    print(f"响应: {resp.text[:500]}")
except Exception as e:
    print(f"错误: {e}")

try:
    print("\n测试 /api/v1/reports/list 端点...")
    resp = session.get(f"{BASE_URL}/api/v1/reports/list?limit=1", timeout=5)
    print(f"状态码: {resp.status_code}")
    print(f"响应: {resp.text[:500]}")
except Exception as e:
    print(f"错误: {e}")
