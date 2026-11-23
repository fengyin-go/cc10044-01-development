import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  OverviewResult,
  PageResult,
  SnapshotGroup,
  SnapshotLineageTrace,
  SourceIngestResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------------------------------------------------------------------------
// 成组快照：登记/重提、补齐、入表模拟、血缘回查
// ---------------------------------------------------------------------------

export type SnapshotGroupMemberInput = {
  dataset: string
  snapshot_type: string
}

export async function registerSnapshotGroup(
  cutoffTime: string,
  members: SnapshotGroupMemberInput[],
  groupId?: number,
): Promise<SnapshotGroup> {
  const response = await request('/api/snapshot_creator/groups', {
    method: 'POST',
    body: JSON.stringify({
      cutoff_time: cutoffTime,
      members,
      group_id: groupId ?? null,
    }),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '成组快照登记失败')
  }
  return await response.json()
}

export async function listSnapshotGroups(): Promise<{ total: number; groups: SnapshotGroup[] }> {
  const response = await request('/api/snapshot_creator/groups')
  if (!response.ok) throw new Error('成组快照清单读取失败')
  return await response.json()
}

export async function completeSnapshotGroupMember(
  groupId: number,
  dataset: string,
): Promise<Record<string, unknown>> {
  const response = await request('/api/snapshot_creator/groups/complete-member', {
    method: 'POST',
    body: JSON.stringify({ group_id: groupId, dataset }),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '成员快照补齐失败')
  }
  return await response.json()
}

export async function resubmitSnapshotGroup(
  groupId: number,
): Promise<Record<string, unknown>> {
  const response = await request(`/api/snapshot_creator/groups/${groupId}/resubmit`, {
    method: 'POST',
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '重提原组失败')
  }
  return await response.json()
}

export async function traceSnapshotLineage(
  selector: { snapshot_id: number } | { group_id: number },
): Promise<SnapshotLineageTrace> {
  const response = await request('/api/snapshot_creator/lineage/trace', {
    method: 'POST',
    body: JSON.stringify(selector),
  })
  // 业务拒绝（如成员不齐全）也返回 200，由 traceable 表达；网络错误才抛异常。
  if (!response.ok) throw new Error('血缘回查请求失败')
  return await response.json()
}

export async function ingestSourceVersion(payload: {
  dataset?: string
  source_table?: string
  remark?: string
  effective_time?: string
}): Promise<SourceIngestResult> {
  const response = await request('/api/snapshot_creator/source/ingest', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ?? '来源入表登记失败')
  }
  return await response.json()
}
