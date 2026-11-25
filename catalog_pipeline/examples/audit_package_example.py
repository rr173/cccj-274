#!/usr/bin/env python3
"""审计历史来源包复现示例入口。

用法::

    python3 -I examples/audit_package_example.py --workdir ./audit-demo

脚本会构造带“明确缺席”成员和平价选择的可复现世界，导出成功批次的 JSONL
证据来源包，演示事实锁定、源版本缺失时严格失败、半成品不可读，以及对现有
cross_demo 采购库的兼容。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.audit_demo import main


if __name__ == "__main__":
    raise SystemExit(main())
