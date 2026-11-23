/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 成组快照成员：登记后编号即固定，补齐只更新状态和来源版本。 */
export type SnapshotGroupMember = {
  dataset: string
  snapshot_type: string
  snapshot_id: number
  lineage_id: number
  source_table: string
  source_version: string
  status: string
}

export type SnapshotGroup = {
  ok?: boolean
  group_id: number
  status: string
  cutoff_time: string
  registered_at?: string
  member_count: number
  completed_count: number
  members: SnapshotGroupMember[]
}

/** 血缘回查结果：traceable 表示是否取得可信来源版本。 */
export type SnapshotLineageTrace = {
  ok: boolean
  traceable?: boolean
  snapshot_id?: number
  lineage_id?: number
  group_id?: number
  dataset?: string
  target_table?: string
  source_table?: string
  source_version?: string
  cutoff_time?: string
  missing_members?: string[]
  member_count?: number
  records?: SnapshotLineageRecord[]
  message: string
  basis?: string
}

export type SnapshotLineageRecord = {
  dataset: string
  snapshot_id: number
  lineage_id: number
  target_table: string
  source_table: string
  source_version: string
  cutoff_time: string
  group_id: number
}

export type SourceIngestResult = {
  ok: boolean
  source_table: string
  version: string
  effective_time: string
  message: string
}
