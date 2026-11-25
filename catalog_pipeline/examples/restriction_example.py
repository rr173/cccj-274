#!/usr/bin/env python3
"""按版本发布的商品限制清单复现示例入口。

用法::

    python3 -I examples/restriction_example.py --workdir ./restriction-demo

脚本会生成两个供应商样例库、两份限制清单版本（含一份修订）与多份批次 JSON，
跑完启用限制、核算时点切换、清单修订、无效清单严格失败等批次，并为成功批次
导出可离线复核的审计来源包。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.restriction_demo import main


if __name__ == "__main__":
    raise SystemExit(main())
