"""兼容命名：跨供应商目录能力的规范实现位于 :mod:`catpipe.procurement`。"""

from __future__ import annotations

import os
import sys

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.procurement import *  # noqa: F401,F403
    from catpipe.procurement import (  # noqa: F401
        CrossCatalog,
        CrossCatalogError,
        InvalidBatch,
        ProductRef,
        SourceVersionNotFound,
        apply_batch,
        main,
    )
else:
    from .procurement import *  # noqa: F401,F403
    from .procurement import (  # noqa: F401
        CrossCatalog,
        CrossCatalogError,
        InvalidBatch,
        ProductRef,
        SourceVersionNotFound,
        apply_batch,
        main,
    )

if __name__ == "__main__":
    raise SystemExit(main())
