import { atom } from 'nanostores'

import { translateNow } from '@/i18n'
import { type ChatMessage, textPart, toChatMessages } from '@/lib/chat-messages'
import type { ProjectRuntimeSnapshot } from '@/types/hermes'

import {
  executeProjectMutation,
  isProjectMutationRetryAvailable,
  type ProjectMutationOutcome,
  retryProjectMutation
} from './project-command-runtime'
import {
  $projectRuntimes,
  projectRuntimeAuthority,
  type ProjectRuntimeAuthority,
  type ProjectRuntimeState
} from './project-runtime'

export interface OptimisticProjectPrompt {
  accepted_turn_id: null | string
  binding_id: string
  local_id: string
  project_id: string
  requester_generation: number
  requester_scope: null | string
  session_id: string
  submitted_transcript_revision: number
  text: string
}

type OptimisticProjectPromptState = Record<string, OptimisticProjectPrompt[]>

interface ManagedComposerActionBase {
  binding_id: string
  local_id: null | string
  message: string
  project_id: string
  requester_generation?: number
  requester_scope?: null | string
  session_id: string
  text: string
}

export type ManagedComposerAction =
  | (ManagedComposerActionBase & { intent_id: string; status: 'retry_required' })
  | (ManagedComposerActionBase & { intent_id: string; status: 'retrying' })
  | (ManagedComposerActionBase & { status: 'blocked' | 'conflict' | 'failed' })

export const $optimisticProjectPrompts = atom<OptimisticProjectPromptState>({})
export const $managedComposerActionsBySession = atom<Record<string, ManagedComposerAction>>({})
export const $managedComposerAmbiguitiesBySession = atom<Record<string, true>>({})

export interface ManagedVoiceRecoveryEntry {
  attachments: []
  captured: ProjectSubmitAuthorityCapture
  id: string
  queuedAt: number
  text: string
}

export const $managedVoiceRecoveries = atom<Record<string, ManagedVoiceRecoveryEntry[]>>({})

const nextLocalId = () => `project-optimistic-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

interface ManagedComposerSubmitLease {
  binding_id: string
  project_id: string
  requester_generation: number
  requester_scope: null | string
  session_id: string
}

const managedComposerSubmitLeases = new Map<string, ManagedComposerSubmitLease>()

export type ManagedProjectSessionResolution =
  | { status: 'ambiguous' }
  | { snapshot: ProjectRuntimeSnapshot; status: 'managed' }
  | { status: 'legacy' }

interface ProjectSubmitAuthorityBase {
  requesterGeneration: number
  requesterScope: null | string
  sessionId: null | string
}

export type ProjectSubmitAuthorityCapture =
  | (ProjectSubmitAuthorityBase & { status: 'ambiguous' | 'legacy' })
  | (ProjectSubmitAuthorityBase & {
      bindingId: string
      projectId: string
      sessionId: string
      status: 'managed'
    })

export type CapturedProjectSubmitResolution =
  | { snapshot: ProjectRuntimeSnapshot; status: 'managed' }
  | { status: 'legacy' }
  | { status: 'stale' }

/** Resolve one canonical owner without ever selecting arbitrarily. Duplicate
 * claims stay on the managed side of the boundary and must fail closed. */
export function resolveManagedProjectSession(
  runtimes: Record<string, ProjectRuntimeState>,
  sessionId: null | string | undefined
): ManagedProjectSessionResolution {
  if (!sessionId) {
    return { status: 'legacy' }
  }

  const matches = Object.values(runtimes).filter(runtime => runtime.snapshot.canonical_session_id === sessionId)

  if (matches.length === 0) {
    return { status: 'legacy' }
  }

  return matches.length === 1 ? { snapshot: matches[0].snapshot, status: 'managed' } : { status: 'ambiguous' }
}

export function captureProjectSubmitAuthority(
  sessionId: null | string | undefined,
  runtimes: Record<string, ProjectRuntimeState> = $projectRuntimes.get(),
  authority: ProjectRuntimeAuthority = projectRuntimeAuthority()
): ProjectSubmitAuthorityCapture {
  const opaqueSessionId = sessionId ?? null
  const resolution = resolveManagedProjectSession(runtimes, opaqueSessionId)

  const base = {
    requesterGeneration: authority.requesterGeneration,
    requesterScope: authority.scope,
    sessionId: opaqueSessionId
  }

  return resolution.status === 'managed'
    ? {
        ...base,
        bindingId: resolution.snapshot.binding_id,
        projectId: resolution.snapshot.project_id,
        sessionId: resolution.snapshot.canonical_session_id,
        status: 'managed'
      }
    : { ...base, status: resolution.status }
}

export function resolveCapturedProjectSubmitAuthority(
  captured: ProjectSubmitAuthorityCapture,
  runtimes: Record<string, ProjectRuntimeState> = $projectRuntimes.get(),
  authority: ProjectRuntimeAuthority = projectRuntimeAuthority()
): CapturedProjectSubmitResolution {
  if (
    captured.requesterGeneration !== authority.requesterGeneration ||
    captured.requesterScope !== authority.scope ||
    captured.status === 'ambiguous'
  ) {
    return { status: 'stale' }
  }

  const resolution = resolveManagedProjectSession(runtimes, captured.sessionId)

  if (captured.status !== 'managed') {
    return captured.status === 'legacy' && resolution.status === 'legacy' ? { status: 'legacy' } : { status: 'stale' }
  }

  return resolution.status === 'managed' &&
    resolution.snapshot.binding_id === captured.bindingId &&
    resolution.snapshot.project_id === captured.projectId &&
    resolution.snapshot.canonical_session_id === captured.sessionId
    ? resolution
    : { status: 'stale' }
}

const recoveryKeyPart = (value: null | string) => encodeURIComponent(value ?? 'none')

export function managedVoiceRecoveryKey(captured: ProjectSubmitAuthorityCapture): string {
  const scope = recoveryKeyPart(captured.requesterScope)
  const sessionId = recoveryKeyPart(captured.sessionId)
  const generation = captured.requesterGeneration

  return captured.status === 'managed'
    ? `managed-voice:${scope}:${generation}:${recoveryKeyPart(captured.bindingId)}:${recoveryKeyPart(captured.projectId)}:${sessionId}`
    : `managed-voice:${scope}:${generation}:${captured.status}:${captured.status}:${sessionId}`
}

export function quarantineProjectVoicePrompt(captured: ProjectSubmitAuthorityCapture, text: string): boolean {
  const normalizedText = text.trim()

  if (!normalizedText) {
    return false
  }

  const key = managedVoiceRecoveryKey(captured)
  const current = $managedVoiceRecoveries.get()

  const entry: ManagedVoiceRecoveryEntry = {
    attachments: [],
    captured,
    id: `managed-voice-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    queuedAt: Date.now(),
    text: normalizedText
  }

  $managedVoiceRecoveries.set({ ...current, [key]: [...(current[key] ?? []), entry] })

  return true
}

export function restoreManagedVoiceRecovery(
  currentAuthority: ProjectSubmitAuthorityCapture,
  entryId: string
): ManagedVoiceRecoveryEntry | null {
  if (resolveCapturedProjectSubmitAuthority(currentAuthority).status !== 'managed') {
    return null
  }

  const key = managedVoiceRecoveryKey(currentAuthority)
  const current = $managedVoiceRecoveries.get()
  const entries = current[key]
  const entry = entries?.find(candidate => candidate.id === entryId)

  if (!entry) {
    return null
  }

  if (JSON.stringify(entry.captured) !== JSON.stringify(currentAuthority)) {
    return null
  }

  const nextEntries = entries.filter(candidate => candidate.id !== entryId)
  const next = { ...current }

  if (nextEntries.length) {
    next[key] = nextEntries
  } else {
    delete next[key]
  }

  $managedVoiceRecoveries.set(next)

  return entry
}

const isTurnLive = (snapshot: ProjectRuntimeSnapshot, turnId: string): boolean =>
  snapshot.active_run?.turn_id === turnId || snapshot.queue.some(item => item.turn_id === turnId)

const matchesSnapshotIdentity = (
  value: Pick<ManagedComposerActionBase | OptimisticProjectPrompt, 'binding_id' | 'project_id' | 'session_id'>,
  snapshot: ProjectRuntimeSnapshot
): boolean =>
  value.binding_id === snapshot.binding_id &&
  value.project_id === snapshot.project_id &&
  value.session_id === snapshot.canonical_session_id

const matchesCurrentAuthority = (value: {
  requester_generation?: number
  requester_scope?: null | string
}): boolean => {
  const authority = projectRuntimeAuthority()

  return value.requester_generation === authority.requesterGeneration && value.requester_scope === authority.scope
}

function acquireManagedComposerSubmit(snapshot: ProjectRuntimeSnapshot): ManagedComposerSubmitLease | null {
  const authority = projectRuntimeAuthority()
  const existing = managedComposerSubmitLeases.get(snapshot.canonical_session_id)

  if (
    existing &&
    existing.requester_generation === authority.requesterGeneration &&
    existing.requester_scope === authority.scope
  ) {
    return null
  }

  const lease = {
    binding_id: snapshot.binding_id,
    project_id: snapshot.project_id,
    requester_generation: authority.requesterGeneration,
    requester_scope: authority.scope,
    session_id: snapshot.canonical_session_id
  }

  managedComposerSubmitLeases.set(lease.session_id, lease)

  return lease
}

function releaseManagedComposerSubmit(lease: ManagedComposerSubmitLease): void {
  if (managedComposerSubmitLeases.get(lease.session_id) === lease) {
    managedComposerSubmitLeases.delete(lease.session_id)
  }
}

interface ManagedProjectSubmitCopy {
  attachmentsUnsupported: string
  conflict: string
  messageFailed: string
  missingAcceptedTurn: string
}

export async function submitManagedProjectPrompt(options: {
  attachmentsPresent: boolean
  copy: ManagedProjectSubmitCopy
  fromQueue: boolean
  onOptimistic: (row: OptimisticProjectPrompt) => void
  snapshot: ProjectRuntimeSnapshot
  text: string
}): Promise<boolean> {
  const { copy, snapshot, text } = options
  reconcileManagedComposerState($projectRuntimes.get())
  const pendingAction = $managedComposerActionsBySession.get()[snapshot.canonical_session_id]

  if (pendingAction?.status === 'retry_required' || pendingAction?.status === 'retrying') {
    return false
  }

  clearManagedComposerAction(snapshot.canonical_session_id)

  if (options.attachmentsPresent) {
    markManagedComposerNotice(snapshot, {
      message: copy.attachmentsUnsupported,
      status: 'blocked',
      text
    })

    return false
  }

  if (!text || options.fromQueue) {
    return false
  }

  const submitLease = acquireManagedComposerSubmit(snapshot)

  if (!submitLease) {
    return false
  }

  const scopeStillCurrent = () => {
    const current = resolveManagedProjectSession($projectRuntimes.get(), snapshot.canonical_session_id)
    const authority = projectRuntimeAuthority()

    return (
      managedComposerSubmitLeases.get(submitLease.session_id) === submitLease &&
      submitLease.requester_generation === authority.requesterGeneration &&
      submitLease.requester_scope === authority.scope &&
      current.status === 'managed' &&
      matchesSnapshotIdentity(submitLease, current.snapshot)
    )
  }

  try {
    const optimistic = addOptimisticProjectPrompt(snapshot, text)
    options.onOptimistic(optimistic)

    try {
      const outcome = await executeProjectMutation({
        expected_version: snapshot.version,
        name: 'turn.enqueue',
        payload: { message: text },
        project_id: snapshot.project_id
      })

      if (!scopeStillCurrent()) {
        discardOptimisticProjectPrompt(snapshot.project_id, optimistic.local_id)

        return false
      }

      if (outcome.status === 'retry_required') {
        markManagedComposerRetry(snapshot, optimistic.local_id, outcome.intent_id)

        return true
      }

      if (outcome.status === 'conflict') {
        discardOptimisticProjectPrompt(snapshot.project_id, optimistic.local_id)
        markManagedComposerNotice(snapshot, {
          localId: optimistic.local_id,
          message: copy.conflict,
          status: 'conflict',
          text
        })

        return false
      }

      if (!outcome.result.accepted_turn_id) {
        discardOptimisticProjectPrompt(snapshot.project_id, optimistic.local_id)
        markManagedComposerNotice(snapshot, {
          localId: optimistic.local_id,
          message: copy.missingAcceptedTurn,
          status: 'failed',
          text
        })

        return false
      }

      bindOptimisticProjectPrompt(snapshot.project_id, optimistic.local_id, outcome.result.accepted_turn_id)
      clearManagedComposerAction(snapshot.canonical_session_id)

      return true
    } catch (error) {
      discardOptimisticProjectPrompt(snapshot.project_id, optimistic.local_id)

      if (!scopeStillCurrent()) {
        return false
      }

      markManagedComposerNotice(snapshot, {
        localId: optimistic.local_id,
        message: copy.messageFailed,
        status: 'failed',
        text
      })

      return false
    }
  } finally {
    releaseManagedComposerSubmit(submitLease)
  }
}

function setManagedComposerAction(action: ManagedComposerAction): void {
  $managedComposerActionsBySession.set({
    ...$managedComposerActionsBySession.get(),
    [action.session_id]: action
  })
}

export function clearManagedComposerAction(sessionId: string): void {
  const current = $managedComposerActionsBySession.get()

  if (!current[sessionId]) {
    return
  }

  const next = { ...current }
  delete next[sessionId]
  $managedComposerActionsBySession.set(next)
}

export function markManagedComposerRetry(snapshot: ProjectRuntimeSnapshot, localId: string, intentId: string): void {
  const authority = projectRuntimeAuthority()

  setManagedComposerAction({
    binding_id: snapshot.binding_id,
    intent_id: intentId,
    local_id: localId,
    message: translateNow('statusStack.managedProject.retryRequired'),
    project_id: snapshot.project_id,
    requester_generation: authority.requesterGeneration,
    requester_scope: authority.scope,
    session_id: snapshot.canonical_session_id,
    status: 'retry_required',
    text: $optimisticProjectPrompts.get()[snapshot.project_id]?.find(row => row.local_id === localId)?.text ?? ''
  })
}

export function markManagedComposerNotice(
  snapshot: ProjectRuntimeSnapshot,
  notice: { localId?: string; message: string; status: 'blocked' | 'conflict' | 'failed'; text: string }
): void {
  const authority = projectRuntimeAuthority()

  setManagedComposerAction({
    binding_id: snapshot.binding_id,
    local_id: notice.localId ?? null,
    message: notice.message,
    project_id: snapshot.project_id,
    requester_generation: authority.requesterGeneration,
    requester_scope: authority.scope,
    session_id: snapshot.canonical_session_id,
    status: notice.status,
    text: notice.text
  })
}

export function addOptimisticProjectPrompt(snapshot: ProjectRuntimeSnapshot, text: string): OptimisticProjectPrompt {
  const authority = projectRuntimeAuthority()

  const row: OptimisticProjectPrompt = {
    accepted_turn_id: null,
    binding_id: snapshot.binding_id,
    local_id: nextLocalId(),
    project_id: snapshot.project_id,
    requester_generation: authority.requesterGeneration,
    requester_scope: authority.scope,
    session_id: snapshot.canonical_session_id,
    submitted_transcript_revision: snapshot.transcript_revision,
    text
  }

  const current = $optimisticProjectPrompts.get()
  $optimisticProjectPrompts.set({ ...current, [row.project_id]: [...(current[row.project_id] ?? []), row] })

  return row
}

export function bindOptimisticProjectPrompt(
  projectId: string,
  localId: string,
  acceptedTurnId: string
): OptimisticProjectPrompt | undefined {
  const current = $optimisticProjectPrompts.get()
  const rows = current[projectId]

  if (!rows) {
    return undefined
  }

  const existing = rows.find(row => row.accepted_turn_id === acceptedTurnId)

  if (existing) {
    if (existing.local_id !== localId) {
      const next = rows.filter(row => row.local_id !== localId)
      $optimisticProjectPrompts.set({ ...current, ...(next.length ? { [projectId]: next } : {}) })
    }

    return existing
  }

  let bound: OptimisticProjectPrompt | undefined

  const next = rows.map(row => {
    if (row.local_id !== localId) {
      return row
    }

    bound = { ...row, accepted_turn_id: acceptedTurnId }

    return bound
  })

  if (!bound) {
    return undefined
  }

  $optimisticProjectPrompts.set({ ...current, [projectId]: next })

  return bound
}

export function discardOptimisticProjectPrompt(projectId: string, localId: string): void {
  const current = $optimisticProjectPrompts.get()
  const rows = current[projectId]

  if (!rows) {
    return
  }

  const next = rows.filter(row => row.local_id !== localId)

  if (next.length === rows.length) {
    return
  }

  const state = { ...current }

  if (next.length) {
    state[projectId] = next
  } else {
    delete state[projectId]
  }

  $optimisticProjectPrompts.set(state)
}

/** Reconciliation never creates runtime state: canonical queue/active turns only
 * retain the correlated local echo, and a later stable transcript replaces it. */
export function reconcileOptimisticProjectPrompts(snapshot: ProjectRuntimeSnapshot): void {
  const current = $optimisticProjectPrompts.get()
  const rows = current[snapshot.project_id]

  if (!rows) {
    return
  }

  const next = rows.filter(
    row =>
      matchesCurrentAuthority(row) &&
      (!matchesSnapshotIdentity(row, snapshot) ||
        row.accepted_turn_id === null ||
        row.submitted_transcript_revision >= snapshot.transcript_revision ||
        isTurnLive(snapshot, row.accepted_turn_id))
  )

  if (next.length === rows.length) {
    return
  }

  const state = { ...current }

  if (next.length) {
    state[snapshot.project_id] = next
  } else {
    delete state[snapshot.project_id]
  }

  $optimisticProjectPrompts.set(state)
}

export function projectComposerMessages(snapshot: ProjectRuntimeSnapshot): ChatMessage[] {
  const canonical = toChatMessages(snapshot.transcript)

  const optimistic = ($optimisticProjectPrompts.get()[snapshot.project_id] ?? []).filter(
    row => matchesCurrentAuthority(row) && matchesSnapshotIdentity(row, snapshot)
  )

  return [
    ...canonical,
    ...optimistic.map(row => ({
      id: row.local_id,
      parts: [textPart(row.text)],
      pending: true,
      role: 'user' as const
    }))
  ]
}

function exactRuntimeForAction(
  action: ManagedComposerAction,
  runtimes: Record<string, ProjectRuntimeState>
): ProjectRuntimeSnapshot | null {
  const resolution = resolveManagedProjectSession(runtimes, action.session_id)

  return resolution.status === 'managed' &&
    matchesCurrentAuthority(action) &&
    matchesSnapshotIdentity(action, resolution.snapshot)
    ? resolution.snapshot
    : null
}

function clearActionAndOptimistic(action: ManagedComposerAction): void {
  if ($managedComposerActionsBySession.get()[action.session_id] === action) {
    clearManagedComposerAction(action.session_id)
  }

  if (action.local_id) {
    discardOptimisticProjectPrompt(action.project_id, action.local_id)
  }
}

export function reconcileManagedComposerState(runtimes: Record<string, ProjectRuntimeState>): void {
  const canonicalSessionCounts = Object.values(runtimes).reduce<Record<string, number>>((counts, runtime) => {
    const sessionId = runtime.snapshot.canonical_session_id
    counts[sessionId] = (counts[sessionId] ?? 0) + 1

    return counts
  }, {})

  const ambiguities = Object.fromEntries(
    Object.entries(canonicalSessionCounts)
      .filter(([, count]) => count > 1)
      .map(([sessionId]) => [sessionId, true as const])
  )

  $managedComposerAmbiguitiesBySession.set(ambiguities)

  for (const action of Object.values($managedComposerActionsBySession.get())) {
    const runtime = exactRuntimeForAction(action, runtimes)
    const retryUnavailable = action.status === 'retry_required' && !isProjectMutationRetryAvailable(action.intent_id)

    if (!runtime || retryUnavailable) {
      clearActionAndOptimistic(action)
    }
  }

  const current = $optimisticProjectPrompts.get()
  const next: OptimisticProjectPromptState = {}
  let optimisticChanged = false

  for (const [projectId, rows] of Object.entries(current)) {
    const liveRows = rows.filter(row => {
      const resolution = resolveManagedProjectSession(runtimes, row.session_id)

      return (
        resolution.status === 'managed' &&
        matchesCurrentAuthority(row) &&
        matchesSnapshotIdentity(row, resolution.snapshot)
      )
    })

    if (liveRows.length > 0) {
      next[projectId] = liveRows
    }

    optimisticChanged ||= liveRows.length !== rows.length
  }

  if (optimisticChanged) {
    $optimisticProjectPrompts.set(next)
  }

  for (const [sessionId, lease] of managedComposerSubmitLeases) {
    const resolution = resolveManagedProjectSession(runtimes, sessionId)

    if (
      resolution.status !== 'managed' ||
      !matchesCurrentAuthority(lease) ||
      !matchesSnapshotIdentity(lease, resolution.snapshot)
    ) {
      managedComposerSubmitLeases.delete(sessionId)
    }
  }
}

function applyRetryOutcome(
  action: Extract<ManagedComposerAction, { status: 'retrying' }>,
  outcome: ProjectMutationOutcome
) {
  if (outcome.status === 'succeeded') {
    const acceptedTurnId = outcome.result.accepted_turn_id

    if (!acceptedTurnId || !action.local_id) {
      throw new Error(translateNow('statusStack.managedProject.invalidRetryReceipt'))
    }

    bindOptimisticProjectPrompt(action.project_id, action.local_id, acceptedTurnId)
    clearManagedComposerAction(action.session_id)
  } else if (outcome.status === 'conflict') {
    if (action.local_id) {
      discardOptimisticProjectPrompt(action.project_id, action.local_id)
    }

    setManagedComposerAction({
      ...action,
      message: translateNow('statusStack.managedProject.conflict'),
      status: 'conflict'
    })
  } else {
    setManagedComposerAction({ ...action, intent_id: outcome.intent_id, status: 'retry_required' })
  }
}

export async function retryManagedComposerPrompt(sessionId: string): Promise<ProjectMutationOutcome> {
  const current = $managedComposerActionsBySession.get()[sessionId]

  if (!current || current.status !== 'retry_required' || !isProjectMutationRetryAvailable(current.intent_id)) {
    if (current) {
      clearActionAndOptimistic(current)
    }

    throw new Error(translateNow('statusStack.managedProject.retryUnavailable'))
  }

  if (!exactRuntimeForAction(current, $projectRuntimes.get())) {
    clearActionAndOptimistic(current)
    throw new Error(translateNow('statusStack.managedProject.retryScopeChanged'))
  }

  const retrying: Extract<ManagedComposerAction, { status: 'retrying' }> = { ...current, status: 'retrying' }
  setManagedComposerAction(retrying)

  try {
    const outcome = await retryProjectMutation(current.intent_id)
    const active = $managedComposerActionsBySession.get()[sessionId]
    const runtimes = $projectRuntimes.get()

    if (active !== retrying || !exactRuntimeForAction(retrying, runtimes)) {
      clearActionAndOptimistic(retrying)
      throw new Error(translateNow('statusStack.managedProject.retryScopeChanged'))
    }

    applyRetryOutcome(retrying, outcome)

    return outcome
  } catch (error) {
    const active = $managedComposerActionsBySession.get()[sessionId]

    if (active === retrying) {
      if (exactRuntimeForAction(retrying, $projectRuntimes.get())) {
        if (retrying.local_id) {
          discardOptimisticProjectPrompt(retrying.project_id, retrying.local_id)
        }

        setManagedComposerAction({
          binding_id: retrying.binding_id,
          local_id: null,
          message: translateNow('statusStack.managedProject.retryFailed'),
          project_id: retrying.project_id,
          requester_generation: retrying.requester_generation,
          requester_scope: retrying.requester_scope,
          session_id: retrying.session_id,
          status: 'failed',
          text: retrying.text
        })
      } else {
        clearActionAndOptimistic(retrying)
      }
    }

    throw error
  }
}

export function resetOptimisticProjectPrompts(): void {
  $optimisticProjectPrompts.set({})
  $managedComposerActionsBySession.set({})
  $managedComposerAmbiguitiesBySession.set({})
  $managedVoiceRecoveries.set({})
  managedComposerSubmitLeases.clear()
}
