<template>
  <section class="group-panel" data-testid="snapshot-group-panel">
    <header class="group-panel-head">
      <div>
        <h3>成组快照</h3>
        <p class="page-desc">
          多选数据集一次提交，全组共用同一业务截止时间；登记结果回写血缘清单，
          补做始终按原截止时间取数，联合回查只接受同组齐全成员。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-group-create" type="button" @click="openRegister">
          成组登记快照
        </button>
        <button class="btn" data-testid="refresh-groups" type="button" @click="loadGroups">
          刷新组清单
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="group-error">{{ errorMessage }}</p>

    <article v-for="group in groups" :key="group.group_id" class="group-card"
             :data-testid="`group-card-${group.group_id}`"
             :data-status="group.status">
      <div class="group-card-head">
        <div>
          <strong>快照组 G{{ group.group_id }}</strong>
          <span class="group-badge" :data-testid="`group-status-${group.group_id}`">{{ group.status }}</span>
          <span class="group-cutoff" :data-testid="`group-cutoff-${group.group_id}`">
            业务截止时间：{{ group.cutoff_time }}
          </span>
        </div>
        <div class="page-actions">
          <button class="btn" :data-testid="`resubmit-${group.group_id}`" type="button"
                  @click="resubmit(group.group_id)">
            重提原组
          </button>
          <button class="btn primary" :data-testid="`joint-trace-${group.group_id}`" type="button"
                  @click="traceGroup(group.group_id)">
            联合回查血缘
          </button>
        </div>
      </div>

      <table class="data-table inner-table">
        <thead>
          <tr>
            <th>成员数据集</th>
            <th>快照编号</th>
            <th>血缘编号</th>
            <th>来源表</th>
            <th>来源版本</th>
            <th>成员状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="member in group.members" :key="member.dataset"
              :data-testid="`group-${group.group_id}-member-${member.dataset}`"
              :data-member-status="member.status">
            <td>{{ member.dataset }}</td>
            <td>{{ member.snapshot_id }}</td>
            <td>{{ member.lineage_id }}</td>
            <td>{{ member.source_table }}</td>
            <td>{{ member.source_version || '—' }}</td>
            <td>{{ member.status }}</td>
            <td class="row-actions">
              <button v-if="member.status !== '已完成'" class="link" type="button"
                      :data-testid="`complete-${group.group_id}-${member.dataset}`"
                      @click="completeMember(group.group_id, member.dataset)">
                补齐快照
              </button>
              <button class="link" type="button"
                      :data-testid="`trace-${member.snapshot_id}`"
                      @click="traceOne(member.snapshot_id)">
                单份回查
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </article>

    <p v-if="!groups.length" class="empty-state" data-testid="group-empty">
      暂无成组快照，点击「成组登记快照」选择多个数据集并指定统一截止时间。
    </p>

    <div v-if="traceResult" class="trace-box" :class="{ ok: traceResult.traceable, fail: !traceResult.traceable }"
         data-testid="trace-result">
      <header>
        <strong>{{ traceResult.traceable ? '血缘可回查' : '血缘不可回查' }}</strong>
        <button class="link" type="button" data-testid="clear-trace" @click="traceResult = null">关闭</button>
      </header>
      <p>{{ traceResult.message }}</p>
      <p v-if="traceResult.basis" class="trace-basis" data-testid="trace-basis">
        判定依据：{{ traceResult.basis }}
      </p>
      <p v-if="traceResult.missing_members?.length" class="trace-basis">
        缺失成员：{{ traceResult.missing_members.join('、') }}
      </p>
      <table v-if="traceResult.records?.length" class="data-table inner-table">
        <thead>
          <tr>
            <th>成员数据集</th>
            <th>快照编号</th>
            <th>血缘编号</th>
            <th>来源表</th>
            <th>来源版本</th>
            <th>截止时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="record in traceResult.records" :key="record.snapshot_id"
              :data-testid="`trace-record-${record.snapshot_id}`">
            <td>{{ record.dataset }}</td>
            <td>{{ record.snapshot_id }}</td>
            <td>{{ record.lineage_id }}</td>
            <td>{{ record.source_table }}</td>
            <td>{{ record.source_version }}</td>
            <td>{{ record.cutoff_time }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <details class="ingest-box" data-testid="ingest-box">
      <summary>模拟业务数据入表（验证补做仍按原截止时间取数）</summary>
      <form class="ingest-form" @submit.prevent="submitIngest">
        <label class="create-field">
          <span>数据集</span>
          <select v-model="ingestDataset" data-testid="ingest-dataset">
            <option v-for="option in DATASET_OPTIONS" :key="option.value" :value="option.value">
              {{ option.label }}
            </option>
          </select>
        </label>
        <label class="create-field">
          <span>生效时间（ISO）</span>
          <input v-model="ingestTime" data-testid="ingest-time" placeholder="2026-11-22T10:00:00Z" />
        </label>
        <button class="btn" data-testid="ingest-submit" type="submit">登记入表新版本</button>
      </form>
      <p v-if="ingestResult" class="trace-basis" data-testid="ingest-result">{{ ingestResult.message }}</p>
    </details>

    <div v-if="showRegister" class="modal-backdrop" data-testid="group-create-modal">
      <form class="modal" @submit.prevent="submitRegister">
        <h3>成组登记快照</h3>
        <label class="create-field">
          <span>业务截止时间（全组共用）</span>
          <input v-model="cutoffTime" data-testid="group-cutoff-input" required
                 placeholder="2026-11-21T00:00:00Z" />
        </label>
        <label class="create-field">
          <span>快照类型</span>
          <select v-model="snapshotType" data-testid="group-snapshot-type">
            <option value="full">full（全量）</option>
            <option value="incremental">incremental（增量）</option>
            <option value="daily_sealed">daily_sealed（日封版）</option>
          </select>
        </label>
        <fieldset class="member-picker">
          <legend>选择成员数据集（可多选）</legend>
          <label v-for="option in DATASET_OPTIONS" :key="option.value" class="member-option">
            <input type="checkbox" :value="option.value" v-model="selectedDatasets"
                   :data-testid="`member-pick-${option.value}`" />
            <span>{{ option.label }}（{{ option.value }}）</span>
          </label>
        </fieldset>
        <div class="page-actions">
          <button class="btn" type="button" @click="showRegister = false">取消</button>
          <button class="btn primary" data-testid="submit-group-create" type="submit"
                  :disabled="submitting">
            {{ submitting ? '提交中…' : '确认成组登记' }}
          </button>
        </div>
      </form>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  completeSnapshotGroupMember,
  ingestSourceVersion,
  listSnapshotGroups,
  registerSnapshotGroup,
  resubmitSnapshotGroup,
  traceSnapshotLineage,
} from '@/api/backend-service'
import type { SnapshotGroup, SnapshotLineageTrace, SourceIngestResult } from '@/data/types'

const emit = defineEmits<{ (event: 'changed'): void }>()

const DATASET_OPTIONS = [
  { value: 'ods_order_detail', label: '订单明细' },
  { value: 'ods_refund_detail', label: '退款明细' },
  { value: 'dwd_order_detail', label: 'DWD订单明细' },
  { value: 'dwd_refund_detail', label: 'DWD退款明细' },
  { value: 'ads_order_detail', label: 'ADS订单明细' },
  { value: 'ads_refund_detail', label: 'ADS退款明细' },
]

const groups = ref<SnapshotGroup[]>([])
const errorMessage = ref('')
const traceResult = ref<SnapshotLineageTrace | null>(null)
const ingestResult = ref<SourceIngestResult | null>(null)

const showRegister = ref(false)
const submitting = ref(false)
const cutoffTime = ref('2026-11-21T00:00:00Z')
const snapshotType = ref('full')
const selectedDatasets = ref<string[]>(['ods_order_detail', 'ods_refund_detail'])

const ingestDataset = ref('ods_refund_detail')
const ingestTime = ref('2026-11-22T10:00:00Z')

async function loadGroups() {
  errorMessage.value = ''
  try {
    const payload = await listSnapshotGroups()
    groups.value = payload.groups
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '成组快照清单读取失败'
  }
}

function openRegister() {
  errorMessage.value = ''
  traceResult.value = null
  showRegister.value = true
}

async function submitRegister() {
  if (!selectedDatasets.value.length) {
    errorMessage.value = '至少选择一个成员数据集'
    return
  }
  submitting.value = true
  errorMessage.value = ''
  try {
    await registerSnapshotGroup(
      cutoffTime.value.trim(),
      selectedDatasets.value.map((dataset) => ({ dataset, snapshot_type: snapshotType.value })),
    )
    showRegister.value = false
    await loadGroups()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '成组登记失败'
  } finally {
    submitting.value = false
  }
}

async function completeMember(groupId: number, dataset: string) {
  errorMessage.value = ''
  try {
    await completeSnapshotGroupMember(groupId, dataset)
    await loadGroups()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '成员快照补齐失败'
  }
}

async function resubmit(groupId: number) {
  errorMessage.value = ''
  try {
    await resubmitSnapshotGroup(groupId)
    await loadGroups()
    emit('changed')
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '重提原组失败'
  }
}

async function traceOne(snapshotId: number) {
  errorMessage.value = ''
  try {
    traceResult.value = await traceSnapshotLineage({ snapshot_id: snapshotId })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '单份回查失败'
  }
}

async function traceGroup(groupId: number) {
  errorMessage.value = ''
  try {
    traceResult.value = await traceSnapshotLineage({ group_id: groupId })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '联合回查失败'
  }
}

async function submitIngest() {
  errorMessage.value = ''
  try {
    ingestResult.value = await ingestSourceVersion({
      dataset: ingestDataset.value,
      effective_time: ingestTime.value.trim() || undefined,
      remark: '值班员模拟登记后业务数据入表',
    })
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '来源入表登记失败'
  }
}

onMounted(loadGroups)
</script>

<style scoped>
.group-panel {
  margin: 16px 0 24px;
  padding: 16px;
  border: 1px solid var(--border-color, #d9e0ea);
  border-radius: 10px;
  background: #f8fafc;
}

.group-panel-head,
.group-card-head {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
}

.group-card {
  margin-top: 14px;
  padding: 14px;
  border: 1px solid var(--border-color, #d9e0ea);
  border-radius: 8px;
  background: #ffffff;
}

.group-badge {
  margin-left: 8px;
  padding: 2px 10px;
  border-radius: 999px;
  background: #e0edff;
  color: #1d4ed8;
  font-size: 12px;
}

.group-cutoff {
  margin-left: 12px;
  color: #475569;
  font-size: 13px;
}

.inner-table {
  margin-top: 10px;
}

.trace-box {
  margin-top: 16px;
  padding: 14px;
  border-radius: 8px;
  border: 1px solid;
}

.trace-box.ok {
  background: #ecfdf5;
  border-color: #10b981;
}

.trace-box.fail {
  background: #fff7ed;
  border-color: #f97316;
}

.trace-basis {
  color: #334155;
  font-size: 13px;
}

.ingest-box {
  margin-top: 16px;
  padding: 10px 14px;
  border-radius: 8px;
  background: #ffffff;
  border: 1px dashed var(--border-color, #d9e0ea);
}

.ingest-form {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
  align-items: flex-end;
  margin-top: 10px;
}

.member-picker {
  border: 1px solid var(--border-color, #d9e0ea);
  border-radius: 8px;
  padding: 10px 14px;
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px;
}

.member-option {
  display: flex;
  gap: 6px;
  align-items: center;
  font-weight: normal;
}
</style>
