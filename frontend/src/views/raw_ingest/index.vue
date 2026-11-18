<template>
  <section class="page" data-module="raw_ingest">
    <header class="page-head">
      <div><h2>原始入库</h2><p class="page-desc">维护原始入库批次的输入、执行约束与结果记录，为原始数据入库提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记入库批次</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <p v-if="errorMessage" class="error-message" data-testid="error-message">{{ errorMessage }}</p>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
        <div class="timeline-actions">
          <button
            v-for="action in actions"
            :key="action"
            class="link"
            type="button"
            :data-testid="`action-${item.id}-${action}`"
            :disabled="busyId === item.id"
            @click="runAction(action, item)"
          >
            {{ action }}
          </button>
        </div>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <div class="manifest-section" data-testid="ingest-manifest">
      <h3>入库批次清单</h3>
      <p class="page-desc">每个入库原始文件仅保留一条批次记录与唯一贴源表引用，重复确认不会重复显示。</p>
      <table class="data-table">
        <thead>
          <tr>
            <th v-for="column in manifestColumns" :key="column">{{ column }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in manifestItems" :key="String(row.id)" :data-testid="`manifest-row-${row.id}`" :data-status="String(row.status)">
            <td v-for="column in manifestColumns" :key="column">{{ row[column] || '—' }}</td>
          </tr>
          <tr v-if="!manifestItems.length">
            <td :colspan="manifestColumns.length" class="empty-state">暂无入库批次清单</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记入库批次</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create" :disabled="creating">{{ creating ? '提交中…' : '提交' }}</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/raw_ingest'
const columns = ["批次编号", "来源系统", "文件格式", "原始行数", "入库时间", "字符编码", "校验摘要", "落地路径"]
const manifestColumns = ["批次编号", "来源系统", "文件格式", "status", "贴源表名称", "映射规则编号", "结构定义编号"]
const actions = ["发起校验", "确认入库", "归档批次"]
const timelineItems = ref<any[]>([])
const manifestItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
// 连点防护：同一批次的动作请求未返回前，禁用该行所有动作按钮
const busyId = ref<number | null>(null)
const creating = ref(false)
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  errorMessage.value = ''
  try {
    const [timelineRes, manifestRes] = await Promise.all([
      request(ENDPOINT),
      request(`${ENDPOINT}/manifest`),
    ])
    if (!timelineRes.ok || !manifestRes.ok) throw new Error('加载失败')
    const data = await timelineRes.json()
    allItems.value = data.items || []
    applyFilter()
    const manifest = await manifestRes.json()
    manifestItems.value = manifest.items || []
  } catch(e: any) {
    errorMessage.value = e.message || '加载失败'
  }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; errorMessage.value = ''; showCreate.value = true }
async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) })
    const payload = await res.json()
    if(!res.ok || !payload.ok) throw new Error(payload.message || '创建失败')
    showCreate.value = false
    await reload()
  } catch(e: any) {
    errorMessage.value = e.message
  } finally {
    creating.value = false
  }
}
async function runAction(action: string, item: any) {
  // 前端连点防护：同一批次确认期间拒绝重复提交，避免重复请求打到后端
  if (busyId.value !== null) return
  busyId.value = item.id
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${item.id}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || '动作未生效')
    await reload()
  } catch(e: any) {
    errorMessage.value = e.message
  } finally {
    busyId.value = null
  }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
