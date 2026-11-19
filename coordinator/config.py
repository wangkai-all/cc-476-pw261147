"""加载装载配置（任务规格 + 资源预算）。"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .model import TaskSpec


def load_config(path: str | Path) -> Tuple[List[TaskSpec], int, int]:
    with open(path, "r", encoding="utf-8") as f:
        data: Dict[str, Any] = json.load(f)
    resources = data.get("resources", {})
    total_slots = int(resources.get("total_slots", 4))
    total_mem = int(resources.get("total_mem", 8))
    specs = [TaskSpec.from_dict(t) for t in data["tasks"]]
    return specs, total_slots, total_mem
