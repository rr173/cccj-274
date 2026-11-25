"""命名别名，规范实现见 :mod:`catpipe.procurement`。"""

from .procurement import *  # noqa: F401,F403
from .procurement import CrossCatalog, main  # noqa: F401

if __name__ == "__main__":
    raise SystemExit(main())
