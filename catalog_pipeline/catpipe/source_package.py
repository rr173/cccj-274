"""描述性别名：历史来源包能力的规范实现位于 :mod:`catpipe.audit`。"""

from __future__ import annotations

import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.audit import *  # noqa: F401,F403
    from catpipe.audit import main  # noqa: F401
else:
    from .audit import *  # noqa: F401,F403
    from .audit import main  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
