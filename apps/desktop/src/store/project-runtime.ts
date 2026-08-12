import { atom } from 'nanostores'

import type {
  ProjectActiveRun,
  ProjectApproval,
  ProjectArtifact,
  ProjectArtifactOpenTarget,
  ProjectArtifactPresentation,
  ProjectDeliveryStatus,
  ProjectQueueItem,
  ProjectRuntimeBlock,
  ProjectRuntimeEvent,
  ProjectRuntimeJson,
  ProjectRuntimeSnapshot,
  SessionMessage
} from '@/types/hermes'

import { isCredentialFreeHttpUrl } from './project-artifact-validation'

export type { ProjectRuntimeEvent, ProjectRuntimeSnapshot } from '@/types/hermes'

/** The runtime store consumes untrusted wire values and validates them itself.
 * Keeping this boundary non-generic lets ordinary request functions and test
 * doubles return their actual response union without pretending to satisfy all
 * possible caller-selected result types. */
export type ProjectRuntimeRequester = (method: string, params?: Record<string, unknown>) => Promise<unknown>

export interface ProjectRuntimeState {
  events: readonly ProjectRuntimeEvent[]
  snapshot: ProjectRuntimeSnapshot
}

export interface ProjectRuntimeAuthority {
  readonly requesterGeneration: number
  readonly scope: null | string
}

export const $projectRuntimes = atom<Record<string, ProjectRuntimeState>>({})

const REPLAY_PAGE_SIZE = 100
const MAX_REPLAY_PAGES = 100
const SAFE_PUBLIC_CODE = /^[a-z][a-z0-9_]{0,63}$/
let requester: ProjectRuntimeRequester | undefined
let requesterGeneration = 0
let requesterScope: string | undefined
const syncing = new Map<string, { minimumSequence: number; task: Promise<void> }>()

interface RequestContext {
  generation: number
  requester: ProjectRuntimeRequester
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isText = (value: unknown): value is string => typeof value === 'string' && value.length > 0

const isSafePublicCode = (value: unknown): value is string => isText(value) && SAFE_PUBLIC_CODE.test(value)

const isNonNegativeInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0

const isPositiveInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value > 0

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value)

  return actual.length === keys.length && actual.every(key => keys.includes(key))
}

function isJson(value: unknown, depth = 0, budget = { nodes: 0 }): value is ProjectRuntimeJson {
  budget.nodes += 1

  if (depth > 64 || budget.nodes > 10_000) {
    return false
  }

  if (value === null || typeof value === 'boolean' || typeof value === 'string') {
    return true
  }

  if (typeof value === 'number') {
    return Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value))
  }

  if (Array.isArray(value)) {
    return value.every(item => isJson(item, depth + 1, budget))
  }

  if (!isRecord(value)) {
    return false
  }

  return Object.values(value).every(item => isJson(item, depth + 1, budget))
}

function isQueueItem(value: unknown): value is ProjectQueueItem {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['turn_id', 'sequence', 'status']) &&
    isText(value.turn_id) &&
    isPositiveInteger(value.sequence) &&
    isText(value.status)
  )
}

function isApproval(value: unknown): value is ProjectApproval {
  return (
    isRecord(value) && hasExactKeys(value, ['approval_id', 'kind']) && isText(value.approval_id) && isText(value.kind)
  )
}

function isActiveRun(value: unknown): value is ProjectActiveRun {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['turn_id', 'control_state', 'control_version']) &&
    isText(value.turn_id) &&
    ['running', 'awaiting_approval', 'stop_requested', 'stopped', 'resume_requested'].includes(
      value.control_state as string
    ) &&
    isNonNegativeInteger(value.control_version)
  )
}

function isDeliveryStatus(value: unknown): value is ProjectDeliveryStatus {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['state', 'error_code']) &&
    ['not_configured', 'caught_up', 'pending', 'in_flight', 'blocked'].includes(value.state as string) &&
    (value.error_code === null || isSafePublicCode(value.error_code)) &&
    (!['not_configured', 'caught_up', 'in_flight'].includes(value.state as string) || value.error_code === null)
  )
}

function isBlock(value: unknown): value is ProjectRuntimeBlock {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['kind', 'code']) &&
    ['runtime', 'operation', 'delivery'].includes(value.kind as string) &&
    isSafePublicCode(value.code)
  )
}

function isOpenTarget(value: unknown): value is ProjectArtifactOpenTarget {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ['kind', 'href']) ||
    value.kind !== 'external_url' ||
    !isText(value.href)
  ) {
    return false
  }

  return isCredentialFreeHttpUrl(value.href)
}

function isArtifactPresentation(value: unknown): value is ProjectArtifactPresentation {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['kind', 'label', 'created_at', 'size_bytes', 'sha256', 'open_target']) &&
    ['file', 'image', 'link'].includes(value.kind as string) &&
    isText(value.label) &&
    !/[\\/]/.test(value.label) &&
    value.label !== '.' &&
    value.label !== '..' &&
    isNonNegativeInteger(value.created_at) &&
    (value.size_bytes === null || isNonNegativeInteger(value.size_bytes)) &&
    (value.sha256 === null || (isText(value.sha256) && /^[a-f0-9]{64}$/.test(value.sha256))) &&
    (value.open_target === null || (value.kind === 'link' && isOpenTarget(value.open_target)))
  )
}

function isArtifact(value: unknown): value is ProjectArtifact {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['artifact_id', 'presentation']) &&
    isText(value.artifact_id) &&
    isArtifactPresentation(value.presentation)
  )
}

function isTranscriptMessage(value: unknown): value is SessionMessage {
  if (!isRecord(value) || !Object.hasOwn(value, 'role') || !Object.hasOwn(value, 'content')) {
    return false
  }

  if (!['assistant', 'system', 'tool', 'user'].includes(value.role as string) || !isJson(value.content)) {
    return false
  }

  const allowed = new Set([
    'codex_reasoning_items',
    'content',
    'context',
    'display_kind',
    'display_metadata',
    'name',
    'reasoning',
    'reasoning_content',
    'reasoning_details',
    'role',
    'text',
    'timestamp',
    'tool_call_id',
    'tool_calls',
    'tool_name'
  ])

  if (Object.keys(value).some(key => !allowed.has(key))) {
    return false
  }

  if (value.timestamp !== undefined && !isNonNegativeInteger(value.timestamp)) {
    return false
  }

  if (value.name !== undefined && typeof value.name !== 'string') {
    return false
  }

  if (value.display_kind !== undefined && typeof value.display_kind !== 'string') {
    return false
  }

  if (
    value.display_metadata !== undefined &&
    typeof value.display_metadata !== 'string' &&
    !isJson(value.display_metadata)
  ) {
    return false
  }

  if (value.reasoning !== undefined && value.reasoning !== null && typeof value.reasoning !== 'string') {
    return false
  }

  if (
    value.reasoning_content !== undefined &&
    value.reasoning_content !== null &&
    typeof value.reasoning_content !== 'string'
  ) {
    return false
  }

  if (value.tool_call_id !== undefined && value.tool_call_id !== null && typeof value.tool_call_id !== 'string') {
    return false
  }

  if (value.tool_name !== undefined && typeof value.tool_name !== 'string') {
    return false
  }

  for (const key of ['codex_reasoning_items', 'context', 'reasoning_details', 'text', 'tool_calls']) {
    if (value[key] !== undefined && !isJson(value[key])) {
      return false
    }
  }

  return true
}

function hasCoherentLifecycle(value: Record<string, unknown>): boolean {
  if (value.lifecycle !== 'active') {
    return value.active_run === null && value.pending_approval === null
  }

  const awaitingApproval = isRecord(value.active_run) && value.active_run.control_state === 'awaiting_approval'

  return (value.pending_approval !== null) === awaitingApproval
}

export function isProjectRuntimeSnapshot(value: unknown): value is ProjectRuntimeSnapshot {
  if (!isRecord(value)) {
    return false
  }

  if (
    !hasExactKeys(value, [
      'project_id',
      'binding_id',
      'canonical_session_id',
      'lifecycle',
      'last_sequence',
      'version',
      'transcript_revision',
      'current_phase',
      'active_run',
      'delivery_status',
      'block',
      'queue',
      'pending_approval',
      'transcript',
      'artifacts'
    ])
  ) {
    return false
  }

  return (
    isText(value.project_id) &&
    isText(value.binding_id) &&
    isText(value.canonical_session_id) &&
    (value.lifecycle === 'active' || value.lifecycle === 'awaiting_acceptance' || value.lifecycle === 'completed') &&
    isNonNegativeInteger(value.last_sequence) &&
    isNonNegativeInteger(value.version) &&
    isNonNegativeInteger(value.transcript_revision) &&
    isText(value.current_phase) &&
    (value.active_run === null || isActiveRun(value.active_run)) &&
    isDeliveryStatus(value.delivery_status) &&
    (value.block === null || isBlock(value.block)) &&
    Array.isArray(value.queue) &&
    value.queue.every(isQueueItem) &&
    (value.pending_approval === null || isApproval(value.pending_approval)) &&
    hasCoherentLifecycle(value) &&
    Array.isArray(value.transcript) &&
    value.transcript.every(isTranscriptMessage) &&
    Array.isArray(value.artifacts) &&
    value.artifacts.every(isArtifact)
  )
}

export function isProjectRuntimeEvent(value: unknown): value is ProjectRuntimeEvent {
  if (!isRecord(value)) {
    return false
  }

  const required = ['event_id', 'project_id', 'sequence', 'kind', 'turn_id', 'payload', 'created_at']

  if (!hasExactKeys(value, required)) {
    return false
  }

  return (
    isText(value.event_id) &&
    isText(value.project_id) &&
    isPositiveInteger(value.sequence) &&
    isText(value.kind) &&
    isJson(value.payload) &&
    isText(value.created_at) &&
    (value.turn_id === null || isText(value.turn_id))
  )
}

function immutable<T>(value: T): T {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) {
    return value
  }

  for (const child of Object.values(value as Record<string, unknown>)) {
    immutable(child)
  }

  return Object.freeze(value)
}

function copyImmutable<T>(value: T): T {
  return immutable(structuredClone(value))
}

function replaceSnapshot(snapshot: ProjectRuntimeSnapshot): void {
  const immutableSnapshot = copyImmutable(snapshot)
  $projectRuntimes.set({
    ...$projectRuntimes.get(),
    [snapshot.project_id]: { events: [], snapshot: immutableSnapshot }
  })
}

export function configureProjectRuntimeRequester(next: ProjectRuntimeRequester | undefined, scope?: string): void {
  const normalizedScope = next ? scope?.trim() || 'default' : undefined

  if (requester === next && requesterScope === normalizedScope) {
    return
  }

  requester = next
  requesterScope = normalizedScope
  requesterGeneration += 1
  syncing.clear()
  $projectRuntimes.set({})
}

export function projectRuntimeScope(): null | string {
  return requester ? (requesterScope ?? 'default') : null
}

export function projectRuntimeAuthority(): ProjectRuntimeAuthority {
  return Object.freeze({
    requesterGeneration,
    scope: projectRuntimeScope()
  })
}

export function resetProjectRuntimeStore(): void {
  requesterGeneration += 1
  syncing.clear()
  $projectRuntimes.set({})
}

export function managedProjectRuntimeIds(): string[] {
  return Object.keys($projectRuntimes.get())
}

export function applyProjectEvent(event: ProjectRuntimeEvent): 'applied' | 'gap' | 'stale' {
  if (!isProjectRuntimeEvent(event)) {
    return 'stale'
  }

  const current = $projectRuntimes.get()[event.project_id]

  if (!current || event.sequence <= current.snapshot.last_sequence) {
    return 'stale'
  }

  if (event.sequence !== current.snapshot.last_sequence + 1) {
    return 'gap'
  }

  const nextEvent = copyImmutable(event)
  const nextSnapshot = copyImmutable({ ...current.snapshot, last_sequence: event.sequence })
  $projectRuntimes.set({
    ...$projectRuntimes.get(),
    [event.project_id]: { events: [...current.events, nextEvent], snapshot: nextSnapshot }
  })

  return 'applied'
}

function parseEventPage(
  value: unknown,
  projectId: string,
  cursor: number
): { events: ProjectRuntimeEvent[]; lastSequence: number } {
  if (!isRecord(value) || !hasExactKeys(value, ['project_id', 'after_sequence', 'last_sequence', 'events'])) {
    throw new Error('invalid project runtime event page')
  }

  if (
    value.project_id !== projectId ||
    value.after_sequence !== cursor ||
    !isNonNegativeInteger(value.last_sequence) ||
    value.last_sequence < cursor ||
    !Array.isArray(value.events) ||
    value.events.length > REPLAY_PAGE_SIZE ||
    !value.events.every(isProjectRuntimeEvent)
  ) {
    throw new Error('invalid project runtime event page')
  }

  const events = value.events as ProjectRuntimeEvent[]
  let expected = cursor + 1

  for (const event of events) {
    if (event.project_id !== projectId || event.sequence !== expected || event.sequence > value.last_sequence) {
      throw new Error('project runtime replay gap')
    }

    expected += 1
  }

  return { events, lastSequence: value.last_sequence }
}

function currentRequestContext(): RequestContext {
  if (!requester) {
    throw new Error('project runtime requester is unavailable')
  }

  return { generation: requesterGeneration, requester }
}

function assertCurrent(context: RequestContext): void {
  if (context.generation !== requesterGeneration || context.requester !== requester) {
    throw new Error('project runtime requester changed')
  }
}

async function loadSnapshot(
  projectId: string,
  context: RequestContext,
  minimumSequence = 0
): Promise<ProjectRuntimeSnapshot> {
  const value = await context.requester('project.runtime.snapshot', { project_id: projectId })
  assertCurrent(context)

  if (!isProjectRuntimeSnapshot(value) || value.project_id !== projectId) {
    throw new Error('invalid project runtime snapshot')
  }

  if (value.last_sequence < minimumSequence) {
    throw new Error('project runtime snapshot moved backwards')
  }

  replaceSnapshot(value)

  return value
}

async function acknowledge(snapshot: ProjectRuntimeSnapshot, context: RequestContext): Promise<void> {
  assertCurrent(context)

  const receipt = await context.requester('project.runtime.ack', {
    binding_id: snapshot.binding_id,
    cursor: snapshot.last_sequence,
    project_id: snapshot.project_id
  })

  assertCurrent(context)

  if (
    !isRecord(receipt) ||
    !hasExactKeys(receipt, ['project_id', 'binding_id', 'cursor']) ||
    receipt.project_id !== snapshot.project_id ||
    receipt.binding_id !== snapshot.binding_id ||
    receipt.cursor !== snapshot.last_sequence
  ) {
    throw new Error('invalid project runtime acknowledgement')
  }
}

async function sync(projectId: string, context: RequestContext, minimumSequence = 0): Promise<void> {
  if (!isText(projectId)) {
    throw new Error('invalid project runtime project id')
  }

  assertCurrent(context)
  let current = $projectRuntimes.get()[projectId]

  if (!current) {
    await loadSnapshot(projectId, context, minimumSequence)
    assertCurrent(context)
    current = $projectRuntimes.get()[projectId]

    if (!current) {
      throw new Error('project runtime snapshot was not stored')
    }

    await acknowledge(current.snapshot, context)

    return
  }

  let pages = 0
  let replayCursor = current.snapshot.last_sequence
  let observedHighWater = Math.max(replayCursor, minimumSequence)

  while (pages < MAX_REPLAY_PAGES) {
    assertCurrent(context)
    let page: { events: ProjectRuntimeEvent[]; lastSequence: number }

    try {
      const response = await context.requester('project.runtime.events', {
        after_sequence: replayCursor,
        limit: REPLAY_PAGE_SIZE,
        project_id: projectId
      })

      assertCurrent(context)
      page = parseEventPage(response, projectId, replayCursor)
    } catch (error) {
      assertCurrent(context)
      const recoveredSnapshot = await loadSnapshot(projectId, context, observedHighWater)
      await acknowledge(recoveredSnapshot, context)

      return
    }

    observedHighWater = Math.max(observedHighWater, page.lastSequence)

    if (!page.events.length) {
      if (page.lastSequence !== replayCursor) {
        const recoveredSnapshot = await loadSnapshot(projectId, context, observedHighWater)
        await acknowledge(recoveredSnapshot, context)

        return
      }

      const authoritativeSnapshot = await loadSnapshot(projectId, context, observedHighWater)
      await acknowledge(authoritativeSnapshot, context)

      return
    }

    replayCursor = page.events.at(-1)!.sequence
    pages += 1
  }

  await loadSnapshot(projectId, context, observedHighWater)
  const recoveredSnapshot = $projectRuntimes.get()[projectId]?.snapshot

  if (!recoveredSnapshot) {
    throw new Error('project runtime snapshot was not stored')
  }

  await acknowledge(recoveredSnapshot, context)
}

export function syncProjectRuntime(projectId: string, minimumSequence = 0): Promise<void> {
  const active = syncing.get(projectId)

  if (active) {
    if (active.minimumSequence >= minimumSequence) {
      return active.task
    }

    return active.task.then(() => syncProjectRuntime(projectId, minimumSequence))
  }

  let context: RequestContext

  try {
    context = currentRequestContext()
  } catch (error) {
    return Promise.reject(error)
  }

  const task = sync(projectId, context, minimumSequence).finally(() => {
    if (syncing.get(projectId)?.task === task) {
      syncing.delete(projectId)
    }
  })

  syncing.set(projectId, { minimumSequence, task })

  return task
}
