#!/usr/bin/env python3
"""跨供应商目录复现示例入口。

用法::

    python3 -I examples/cross_catalog_example.py --workdir ./cross-demo

脚本会生成三个供应商的已发布 SQLite 目录、四份批次 JSON（含一份失败批次），
应用三个成功批次，并重开跨供应商库验证变更持久化。
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.cross_demo import main


if __name__ == "__main__":
    raise SystemExit(main())
