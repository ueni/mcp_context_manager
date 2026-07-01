"""Focused MCP context manager for coding agents."""

from .config import ContextConfig
from .context import ContextService
from .manager import ProjectContextService
from .projects import ProjectRegistry

__all__ = ["ContextConfig", "ContextService", "ProjectContextService", "ProjectRegistry"]
