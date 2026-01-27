"""DataGen: 极简高性能流式数据加工库"""
from . import core
from . import impl
from . import llm
from . import operators
from . import connectors
from . import prompts
from . import util

__version__ = "0.1.0"

__all__ = [
    "core",
    "impl",
    "llm",
    "operators",
    "connectors",
    "prompts",
    "util",
]
