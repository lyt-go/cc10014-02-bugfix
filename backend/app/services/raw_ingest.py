"""原始入库 时间线服务：支持入库提交后触发字段映射规则提取，并关联创建ODS表结构。

确认入库的关键约束：
- 幂等：同一入库原始文件（批次编号）无论确认多少次，只生成一份字段映射、
  一份结构定义和一张贴源表，入库批次清单里只保留唯一批次与唯一ODS表引用。
- 原子：批次状态推进与映射/贴源表/结构定义创建在同一个 store.transaction()
  落库边界内完成，任一步失败整体回滚，不会留下状态与结构不一致的半成品。
- 串行：同一批次号的确认动作通过条目锁串行化，首次确认与重复确认并发时，
  先拿到锁的请求决定清单范围，后到的请求要么复用其结果，要么收到明确冲突提示。
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from app.store import store

MODULE = "raw_ingest"
REQUIRED_FIELDS = ["批次编号", "来源系统", "文件格式"]
STATUS_ORDER = ["草稿", "校验中", "已入库", "已归档"]
ACTION_RULES = {"发起校验": "校验中", "确认入库": "已入库", "归档批次": "已归档"}
NEGATIVE_ACTIONS = []


class RawIngestService:
    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("批次编号", "")),
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[i % len(STATUS_ORDER)])),
                "logs": f"[{start}] 作业启动\n[{start}] 正在处理...\n[{end}] 任务{'完成' if row.get('status') == STATUS_ORDER[-1] else '进行中'}",
            })
        return items

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def get_by_batch_no(self, batch_no: str) -> dict[str, Any] | None:
        return store.find_by_field(MODULE, "批次编号", batch_no)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        batch_no = str(values.get("批次编号", "")).strip()
        with store.lock:
            # 同一入库原始文件只允许登记一个批次，杜绝源头重复批次
            if store.find_by_field(MODULE, "批次编号", batch_no) is not None:
                return None, [f"批次编号 {batch_no} 已存在，请勿重复登记"]
            rows = store.rows(MODULE)
            entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
            entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            # 补全其他字段
            for k in ["原始行数", "入库时间", "字符编码", "校验摘要", "落地路径"]:
                if k not in entry or not entry.get(k):
                    entry[k] = values.get(k, "")
            if not entry.get("入库时间"):
                entry["入库时间"] = date.today().isoformat()
            rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"入库批次 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于原始入库可执行范围"
        target = ACTION_RULES[action]

        # 确认入库走独立的幂等 + 条目锁 + 事务通道
        if action == "确认入库":
            return self._confirm_ingest(entry)

        with store.lock:
            # 状态机校验：只能按顺序推进
            current_idx = STATUS_ORDER.index(entry["status"])
            target_idx = STATUS_ORDER.index(target)
            if target_idx < current_idx:
                return None, f"状态不能回退：当前{entry['status']} → 目标{target}"
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            # 保存版本
            store.save_version(MODULE, entry_id, entry)
        return entry, f"入库批次已{action}"

    # ------------------------------------------------------------------
    # 确认入库：幂等、并发安全、事务原子
    # ------------------------------------------------------------------
    def _confirm_ingest(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        batch_no = str(entry.get("批次编号", ""))
        entry_id = int(entry.get("id", 0))
        entry_lock = store.lock_entry(f"{MODULE}:{batch_no or entry_id}")

        # 非阻塞抢锁：首次确认与重复确认并发到达同一批次号时，
        # 先到的请求决定入库批次清单范围，后到的请求拿到明确的冲突原因。
        if not entry_lock.acquire(blocking=False):
            return None, (
                f"批次 {batch_no or entry_id} 正在执行确认入库，请稍后重试；"
                "请勿重复点击确认"
            )
        try:
            # 抢到锁后重新读取，避免等待期间状态已被前一个请求推进
            latest = store.find(MODULE, entry_id)
            if latest is None:
                return None, f"入库批次 {entry_id} 不存在或已归档"
            # 幂等：已经入库的批次直接返回现有结果，不再重复创建任何产物
            if latest.get("status") == "已入库":
                ods_table = self._ods_table_for_batch(batch_no)
                return latest, (
                    f"批次 {batch_no} 已入库，重复确认已忽略；"
                    f"入库批次清单保持唯一批次与唯一ODS表引用"
                    + (f"（{ods_table}）" if ods_table else "（ODS表尚未创建）")
                )

            current_idx = STATUS_ORDER.index(latest["status"])
            target_idx = STATUS_ORDER.index("已入库")
            if target_idx < current_idx:
                return None, f"状态不能回退：当前{latest['status']} → 目标已入库"

            # 同一落库边界：状态推进、映射、贴源表、结构定义要么全部成功，
            # 要么整体回滚，避免留下一半导致批次状态与结构不一致。
            # 事务期间持有仓库锁，杜绝并发写入与快照回滚互相干扰。
            try:
                with store.lock, store.transaction():
                    latest["status"] = "已入库"
                    latest["pending"] = False
                    latest["abnormal"] = False
                    store.save_version(MODULE, entry_id, latest)
                    self._on_ingest_confirmed(latest)
            except Exception as exc:  # 事务已在 store 层回滚
                return None, f"确认入库失败，已整体回滚未保留半成品：{exc}"
            return latest, "入库批次已确认入库，字段映射、贴源表与结构定义已在同一落库边界创建"
        finally:
            entry_lock.release()

    def _ods_table_for_batch(self, batch_no: str) -> str:
        """返回批次清单中该批次唯一引用的ODS表名（没有则空串）。"""
        for row in store.rows("ods_layer"):
            if row.get("创建批次") == batch_no:
                return str(row.get("表名称", ""))
        return ""

    def get_ingest_manifest(self) -> list[dict[str, Any]]:
        """入库批次清单：每个入库原始文件只保留一条批次记录，
        并附带唯一的贴源表（ODS）引用；重复批次号在展示层去重。"""
        manifest: list[dict[str, Any]] = []
        seen_batches: set[str] = set()
        for row in store.rows(MODULE):
            batch_no = str(row.get("批次编号", ""))
            if not batch_no or batch_no in seen_batches:
                continue
            seen_batches.add(batch_no)
            # 每个批次只解析一次贴源表引用，天然保证批次内引用唯一
            table_name = self._ods_table_for_batch(batch_no)
            manifest.append({
                "id": int(row.get("id", 0)),
                "批次编号": batch_no,
                "来源系统": row.get("来源系统", ""),
                "文件格式": row.get("文件格式", ""),
                "原始行数": row.get("原始行数", ""),
                "入库时间": row.get("入库时间", ""),
                "status": row.get("status", ""),
                "贴源表名称": table_name,
                "映射规则编号": self._mapping_rule_for_batch(batch_no),
                "结构定义编号": self._schema_id_for_batch(batch_no),
            })
        return manifest

    def _mapping_rule_for_batch(self, batch_no: str) -> str:
        for row in store.rows("column_mapping"):
            if row.get("来源批次") == batch_no:
                return str(row.get("规则编号", ""))
        return ""

    def _schema_id_for_batch(self, batch_no: str) -> str:
        for row in store.rows("schema_registry"):
            if row.get("生效批次") == batch_no:
                return str(row.get("定义编号", ""))
        return ""

    def _on_ingest_confirmed(self, entry: dict[str, Any]) -> None:
        batch_no = entry.get("批次编号", "")
        source = entry.get("来源系统", "")
        fmt = entry.get("文件格式", "")
        # 自动提取字段映射规则（模拟从文件头解析）
        from app.services.column_mapping import ColumnMappingService
        cms = ColumnMappingService()
        # 模拟解析源字段（按来源系统决定）
        if "订单" in source or "ord" in source.lower():
            src_fields = "order_id,amount,status"
            tgt_fields = "order_id,order_amount,order_status"
            dtypes = "STRING,DECIMAL,STRING"
            tbl_name = "ods_orders"
        elif "支付" in source or "pay" in source.lower():
            src_fields = "pay_id,amount,pay_time"
            tgt_fields = "pay_id,pay_amount,pay_time"
            dtypes = "STRING,DECIMAL,TIMESTAMP"
            tbl_name = "ods_payments"
        elif "用户" in source or "user" in source.lower():
            src_fields = "user_id,register_time,level"
            tgt_fields = "user_id,register_time,user_level"
            dtypes = "STRING,TIMESTAMP,STRING"
            tbl_name = "ods_users"
        else:
            src_fields = "col1,col2,col3"
            tgt_fields = "field1,field2,field3"
            dtypes = "STRING,STRING,STRING"
            tbl_name = f"ods_{source}"
        rule_no = f"MAP-{batch_no.replace('RAW-', '')}"
        # 幂等：同一批次的映射规则只保留一份（按规则编号优先，回退按来源批次）
        existing_mapping = store.find_by_field("column_mapping", "规则编号", rule_no)
        if existing_mapping is None:
            existing_mapping = store.find_by_field("column_mapping", "来源批次", batch_no)
        if existing_mapping is None:
            cms.create_entry({
                "规则编号": rule_no,
                "源字段名": src_fields,
                "目标字段名": tgt_fields,
                "数据类型": dtypes,
                "默认值": ",,",
                "转换函数": "trim,trim,trim",
                "规则版本": "v1",
                "生效日期": date.today().isoformat(),
                "来源批次": batch_no,
            })
        # 自动创建/更新ODS表结构
        from app.services.ods_layer import OdsLayerService
        ods = OdsLayerService()
        existing = store.find_by_field("ods_layer", "表名称", tbl_name)
        if existing is None:
            ods.create_entry({
                "表编号": f"ODS_{batch_no.replace('RAW-', '').replace('-', '_')}",
                "来源系统": source,
                "表名称": tbl_name,
                "分区字段": "dt",
                "存储格式": fmt if fmt else "PARQUET",
                "保留天数": "90",
                "压缩算法": "SNAPPY",
                "创建批次": batch_no,
                "字段列表": tgt_fields,
            })
        elif existing.get("创建批次") != batch_no:
            # 表已存在且属于别的批次时才更新版本；同一批次重复确认不产生重复版本
            existing["创建批次"] = batch_no
            existing["字段列表"] = tgt_fields
            store.save_version("ods_layer", existing["id"], existing)
        # 自动注册schema：按定义编号去重，重复确认只保留一份结构定义
        from app.services.schema_registry import SchemaRegistryService
        sch = SchemaRegistryService()
        schema_no = f"SCH-{batch_no.replace('RAW-', '')}"
        existing_schema = store.find_by_field("schema_registry", "定义编号", schema_no)
        if existing_schema is None:
            existing_schema = store.find_by_field("schema_registry", "生效批次", batch_no)
        if existing_schema is None:
            sch.create_entry({
                "定义编号": schema_no,
                "表名称": tbl_name,
                "字段名称": tgt_fields,
                "字段类型": dtypes,
                "可空标识": "N,N,N",
                "默认值": ",,",
                "定义版本": "v1",
                "生效批次": batch_no,
            })
        # 写入模拟表数据（按表名整体覆盖，重复确认不会追加重复行）
        row_count = int(entry.get("原始行数", "0") or "0")
        if row_count > 0:
            sample_rows = []
            for i in range(min(row_count, 10)):  # 最多模拟10条
                sample_rows.append({"_id": i + 1, "batch": batch_no, "source": source})
            store.set_table_rows(tbl_name, sample_rows)
