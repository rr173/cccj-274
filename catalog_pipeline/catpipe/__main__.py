"""支持 ``python -m catpipe`` 调用现有 CLI。"""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
