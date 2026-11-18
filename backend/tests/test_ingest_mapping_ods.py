"""测试1：入库→映射→ODS表链路。

验证：
- 入库批次确认后自动触发字段映射规则创建
- 字段映射规则与入库批次关联
- ODS表被自动创建/更新
- schema_registry同步注册
- 重复确认幂等：只保留一份批次记录、一份映射、一张贴源表、一份结构定义
- 确认入库与贴源表创建在同一落库边界，失败整体回滚
- 入库批次清单按批次/贴源表去重，贴源层跨模块读取口径一致
- 同批次并发确认时先到者决定清单范围
"""
from __future__ import annotations

import threading

import pytest
from app.store import store
from app.services.raw_ingest import RawIngestService
from app.services.column_mapping import ColumnMappingService
from app.services.ods_layer import OdsLayerService
from app.services.schema_registry import SchemaRegistryService


def reset_store():
    """重置内存仓库到初始seed状态。"""
    from app.seed import SEED_ROWS
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    store._table_data.clear()
    store._versions.clear()
    store._lineage_edges.clear()
    store._quality_results.clear()
    store._snapshot_data.clear()
    store._archive_records.clear()
    store._backfill_partitions.clear()


class TestIngestMappingOds:
    def setup_method(self):
        reset_store()

    def test_ingest_confirm_triggers_mapping_and_ods(self):
        """确认入库后应自动创建字段映射和ODS表。"""
        ri = RawIngestService()
        entry, missing = ri.create_entry({
            "批次编号": "RAW-20261115",
            "来源系统": "订单系统",
            "文件格式": "CSV",
            "原始行数": "50000",
            "入库时间": "2026-11-15T02:00:00",
            "字符编码": "UTF-8",
            "校验摘要": "abc123",
            "落地路径": "/data/raw/orders/20261115.csv",
        })
        assert entry is not None
        assert entry["status"] == "草稿"

        # 执行确认入库动作
        updated, msg = ri.run_action(entry["id"], "确认入库")
        assert updated is not None
        assert updated["status"] == "已入库"

        # 验证字段映射已创建
        cms = ColumnMappingService()
        mappings = cms.get_mappings_by_batch("RAW-20261115")
        assert len(mappings) >= 1
        mapping = mappings[0]
        assert "order_id" in mapping.get("源字段名", "")
        assert "RAW-20261115" == mapping.get("来源批次", "")

        # 验证ODS表已创建
        ods = OdsLayerService()
        ods_entry = ods.get_by_table_name("ods_orders")
        assert ods_entry is not None
        assert ods_entry.get("来源系统") == "订单系统"
        assert "RAW-20261115" == ods_entry.get("创建批次", "")

        # 验证Schema已注册
        sch = SchemaRegistryService()
        schemas = sch.get_schema_for_table("ods_orders")
        assert len(schemas) >= 1
        latest = sch.get_latest_schema("ods_orders")
        assert latest is not None
        assert "order_id" in latest.get("字段名称", "")

        # 验证版本记录
        versions = store.get_versions("raw_ingest", entry["id"])
        assert len(versions) >= 1
        assert versions[-1]["data"]["status"] == "已入库"

    def test_ingest_batch_linkage(self):
        """验证入库批次与ODS表的双向关联。"""
        ods = OdsLayerService()
        batches = ods.get_batches_for_table("ods_orders")
        assert len(batches) >= 1
        assert batches[0].get("来源系统") == "订单系统"


def _create_pending_batch(ri: RawIngestService, batch_no: str = "RAW-20261116") -> dict:
    entry, missing = ri.create_entry({
        "批次编号": batch_no,
        "来源系统": "订单系统",
        "文件格式": "CSV",
        "原始行数": "120",
        "入库时间": "2026-11-16T02:00:00",
        "字符编码": "UTF-8",
        "校验摘要": "dup001",
        "落地路径": f"/data/raw/orders/{batch_no}.csv",
    })
    assert entry is not None, missing
    return entry


class TestIngestIdempotency:
    """重复确认同一入库原始文件：幂等与落库边界。"""

    def setup_method(self):
        reset_store()

    def test_repeated_confirm_creates_single_set_of_artifacts(self):
        """连续两次确认后只剩一份映射、一张贴源表、一份结构定义和一份批次记录。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri)

        first, msg1 = ri.run_action(entry["id"], "确认入库")
        assert first is not None and first["status"] == "已入库"
        # 重复确认：不报错，返回幂等提示
        second, msg2 = ri.run_action(entry["id"], "确认入库")
        assert second is not None
        assert second["status"] == "已入库"
        assert "重复确认已忽略" in msg2

        batch_no = entry["批次编号"]
        # 批次记录只有一份
        batch_rows = [r for r in store.rows("raw_ingest") if r.get("批次编号") == batch_no]
        assert len(batch_rows) == 1
        # 字段映射只有一份
        mappings = ColumnMappingService().get_mappings_by_batch(batch_no)
        assert len(mappings) == 1
        # 贴源表只有一张且引用该批次
        ods_rows = [r for r in store.rows("ods_layer") if r.get("创建批次") == batch_no]
        assert len(ods_rows) == 1
        assert ods_rows[0]["表名称"] == "ods_orders"
        # 结构定义只有一份
        schema_rows = [r for r in store.rows("schema_registry") if r.get("生效批次") == batch_no]
        assert len(schema_rows) == 1
        # 模拟表数据不被重复追加
        data = store.table_rows("ods_orders")
        assert len(data) == 10
        # 版本只记录一次入库推进（重复确认不产生新版本）
        versions = store.get_versions("raw_ingest", entry["id"])
        assert len(versions) == 1

    def test_manifest_keeps_unique_batch_and_ods_reference(self):
        """入库批次清单：同一批次与同一贴源表只出现一次。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri)
        ri.run_action(entry["id"], "确认入库")
        ri.run_action(entry["id"], "确认入库")

        manifest = ri.get_ingest_manifest()
        target = [m for m in manifest if m["批次编号"] == entry["批次编号"]]
        assert len(target) == 1
        item = target[0]
        assert item["贴源表名称"] == "ods_orders"
        assert item["映射规则编号"] == "MAP-20261116"
        assert item["结构定义编号"] == "SCH-20261116"
        # 全清单中贴源表引用不重复
        table_refs = [m["贴源表名称"] for m in manifest if m["贴源表名称"]]
        assert len(table_refs) == len(set(table_refs))

    def test_duplicate_batch_registration_rejected(self):
        """同一入库原始文件不允许重复登记批次。"""
        ri = RawIngestService()
        _create_pending_batch(ri, "RAW-DUP-1")
        again, missing = ri.create_entry({
            "批次编号": "RAW-DUP-1",
            "来源系统": "订单系统",
            "文件格式": "CSV",
        })
        assert again is None
        assert any("已存在" in m for m in missing)

    def test_ods_detail_reads_manifest_without_duplication(self):
        """贴源层管理详情页跨模块读取入库批次清单，重复数据不重复显示。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri)
        ri.run_action(entry["id"], "确认入库")
        ri.run_action(entry["id"], "确认入库")

        ods = OdsLayerService()
        batches = ods.get_batches_for_table("ods_orders")
        assert len(batches) == 1
        assert batches[0]["批次编号"] == entry["批次编号"]

    def test_confirm_failure_rolls_back_all_artifacts(self):
        """落库边界内任一步失败：批次状态与贴源结构整体回滚，不留半成品。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri, "RAW-ROLLBACK-1")
        batch_no = entry["批次编号"]

        def boom(self, values):  # 模拟结构注册在确认链路中途失败
            raise RuntimeError("schema registry unavailable")

        original = SchemaRegistryService.create_entry
        SchemaRegistryService.create_entry = boom
        try:
            result, message = ri.run_action(entry["id"], "确认入库")
        finally:
            SchemaRegistryService.create_entry = original

        assert result is None
        assert "整体回滚" in message
        # 批次状态回到草稿，仍可重新确认
        refreshed = store.find("raw_ingest", entry["id"])
        assert refreshed["status"] == "草稿"
        # 映射、贴源表、结构定义都没有残留
        assert not [r for r in store.rows("column_mapping") if r.get("来源批次") == batch_no]
        assert not [r for r in store.rows("ods_layer") if r.get("创建批次") == batch_no]
        assert not [r for r in store.rows("schema_registry") if r.get("生效批次") == batch_no]
        # 回滚后重新确认可以成功（Bugfix 中断后从完整版本恢复）
        ok, _ = ri.run_action(entry["id"], "确认入库")
        assert ok is not None and ok["status"] == "已入库"
        assert len(ColumnMappingService().get_mappings_by_batch(batch_no)) == 1

    def test_concurrent_confirm_only_one_wins(self):
        """首次确认与重复确认并发到达同一批次号：先到者决定清单范围。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri, "RAW-CONC-1")
        batch_no = entry["批次编号"]
        results: list[tuple] = []

        def worker() -> None:
            results.append(ri.run_action(entry["id"], "确认入库"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 恰好一个请求真正落库创建，其余请求拿到幂等结果或冲突提示，均不报错丢数据
        created = [r for r in results if r[0] is not None and "已在同一落库边界创建" in r[1]]
        idempotent = [r for r in results if r[0] is not None and "重复确认已忽略" in r[1]]
        conflicted = [r for r in results if r[0] is None and "正在执行确认入库" in r[1]]
        assert len(created) == 1
        assert len(idempotent) + len(conflicted) == 7
        assert all(r[0]["status"] == "已入库" for r in idempotent)
        # 最终产物仍然唯一
        assert len(ColumnMappingService().get_mappings_by_batch(batch_no)) == 1
        assert len([r for r in store.rows("ods_layer") if r.get("创建批次") == batch_no]) == 1
        manifest = ri.get_ingest_manifest()
        assert len([m for m in manifest if m["批次编号"] == batch_no]) == 1
        assert store.find("raw_ingest", entry["id"])["status"] == "已入库"

    def test_pending_batch_manifest_reports_missing_ods_reason(self):
        """待确认批次在清单中显示且明确ODS表尚未创建。"""
        ri = RawIngestService()
        entry = _create_pending_batch(ri, "RAW-PENDING-1")
        manifest = ri.get_ingest_manifest()
        item = next(m for m in manifest if m["批次编号"] == "RAW-PENDING-1")
        assert item["status"] == "草稿"
        assert item["贴源表名称"] == ""
        assert item["映射规则编号"] == ""
        # 确认后再看清单即出现唯一ODS引用
        ri.run_action(entry["id"], "确认入库")
        item = next(m for m in ri.get_ingest_manifest() if m["批次编号"] == "RAW-PENDING-1")
        assert item["贴源表名称"] == "ods_orders"
