"""贴源层管理业务规则：支持从入库批次创建表结构，存储模拟表数据，并可被下游DWD消费。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "ods_layer"
LIST_FIELDS = ["表编号", "来源系统", "表名称", "分区字段", "存储格式", "保留天数", "压缩算法", "创建批次"]
REQUIRED_FIELDS = ["表编号", "来源系统", "表名称"]
STATUS_ORDER = ["草稿", "待校验", "已上线", "已下线"]
ACTION_RULES = {"提交校验": "待校验", "确认上线": "已上线", "标记下线": "已下线"}
NEGATIVE_ACTIONS = []


class OdsLayerService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("表编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def get_by_table_name(self, table_name: str) -> dict[str, Any] | None:
        return store.find_by_field(MODULE, "表名称", table_name)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        if values.get("字段列表"):
            entry["字段列表"] = values["字段列表"]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"贴源表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于贴源层管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        store.save_version(MODULE, entry_id, entry)
        return entry, f"贴源表已{action}"

    def get_table_data(self, table_name: str) -> list[dict[str, Any]]:
        return store.table_rows(table_name)

    def get_batches_for_table(self, table_name: str) -> list[dict[str, Any]]:
        """跨模块读取入库批次清单：按该贴源表的「创建批次」回查原始入库批次。

        同一批次号即使存在重复登记，也只返回一份批次记录，保证详情页不重复显示。
        """
        ods = self.get_by_table_name(table_name)
        if not ods:
            return []
        batch_no = ods.get("创建批次")
        if not batch_no:
            return []
        from app.services.raw_ingest import RawIngestService
        ri = RawIngestService()
        # 优先走入库批次清单（已按批次号去重），保证与原始入库列表页口径一致
        manifest = {item.get("批次编号"): item for item in ri.get_ingest_manifest()}
        item = manifest.get(batch_no)
        if item is not None:
            batch = ri.get_by_batch_no(batch_no)
            return [batch] if batch else []
        # 清单里没有（如未确认入库的历史数据）时再兜底直查，并自行去重
        seen: set[str] = set()
        batches: list[dict[str, Any]] = []
        for row in store.rows("raw_ingest"):
            if row.get("批次编号") == batch_no and batch_no not in seen:
                seen.add(batch_no)
                batches.append(row)
        return batches
