import { atom, computed, type ReadableAtom } from 'nanostores'

import { $activeGatewayProfile } from './profile'
import { executeProjectMutation, type ProjectMutationOutcome, retryProjectMutation } from './project-command-runtime'
import { $projectRuntimes, projectRuntimeAuthority } from './project-runtime'
import { resolveCurrentManagedProjectSurface } from './project-surface-authority-store'
import { $activeProjectId, $projectCatalogAuthority, $projects } from './projects'
import { $sessions } from './session'

export interface ManagedProjectApproval {
  approvalId: string
  approvalKind: string
  bindingId: string
  projectId: string
  requesterGeneration: number
  requesterScope: null | string
  runtimeSessionId: null | string
  sessionId: string
  storedSessionId: null | string
  version: number
}

export interface ManagedProjectApprovalSelection {
  approval: ManagedProjectApproval | null
  managed: boolean
}

export type ManagedProjectApprovalOutcome = 'approved' | 'denied'

export type ManagedProjectApprovalActionState =
  | { status: 'idle' }
  | { outcome: ManagedProjectApprovalOutcome; status: 'submitting' }
  | { status: 'conflict' }
  | { intentId: string; outcome: ManagedProjectApprovalOutcome; status: 'retry_required' }
  | { status: 'failed' }

interface StoredApprovalAction {
  fingerprint: string
  state: ManagedProjectApprovalActionState
}

const IDLE_ACTION = Object.freeze({ status: 'idle' } as const)
const UNMANAGED_SELECTION = Object.freeze({ approval: null, managed: false } as const)
const MANAGED_WITHOUT_APPROVAL = Object.freeze({ approval: null, managed: true } as const)
const $approvalActions = atom<Record<string, StoredApprovalAction>>({})

function approvalFingerprint(approval: ManagedProjectApproval): string {
  return JSON.stringify([
    approval.requesterGeneration,
    approval.requesterScope,
    approval.projectId,
    approval.sessionId,
    approval.bindingId,
    approval.approvalId,
    approval.version
  ])
}

function selectManagedProjectApproval(
  runtimeSessionId: null | string,
  storedSessionId: null | string
): ManagedProjectApprovalSelection {
  if (!storedSessionId) {
    return MANAGED_WITHOUT_APPROVAL
  }

  const resolution = resolveCurrentManagedProjectSurface(runtimeSessionId, storedSessionId)

  if (resolution.status === 'conclusively-legacy') {
    return UNMANAGED_SELECTION
  }

  if (resolution.status !== 'managed') {
    return MANAGED_WITHOUT_APPROVAL
  }

  const snapshot = resolution.snapshot
  const authority = projectRuntimeAuthority()

  if (!snapshot.pending_approval) {
    return MANAGED_WITHOUT_APPROVAL
  }

  return {
    approval: {
      approvalId: snapshot.pending_approval.approval_id,
      approvalKind: snapshot.pending_approval.kind,
      bindingId: snapshot.binding_id,
      projectId: snapshot.project_id,
      requesterGeneration: authority.requesterGeneration,
      requesterScope: authority.scope,
      runtimeSessionId: runtimeSessionId ?? null,
      sessionId: snapshot.canonical_session_id,
      storedSessionId: storedSessionId ?? null,
      version: snapshot.version
    },
    managed: true
  }
}

function currentApproval(approval: ManagedProjectApproval): ManagedProjectApproval | null {
  return selectManagedProjectApproval(approval.runtimeSessionId, approval.storedSessionId).approval
}

function isSameApproval(left: ManagedProjectApproval | null, right: ManagedProjectApproval): boolean {
  return (
    left !== null &&
    left.approvalId === right.approvalId &&
    left.bindingId === right.bindingId &&
    left.projectId === right.projectId &&
    left.requesterGeneration === right.requesterGeneration &&
    left.requesterScope === right.requesterScope &&
    left.sessionId === right.sessionId &&
    left.version === right.version
  )
}

function assertCurrent(approval: ManagedProjectApproval): void {
  if (!isSameApproval(currentApproval(approval), approval)) {
    throw new Error('managed project approval changed')
  }
}

function actionState(approval: ManagedProjectApproval): ManagedProjectApprovalActionState {
  const stored = $approvalActions.get()[approval.projectId]

  return stored?.fingerprint === approvalFingerprint(approval) ? stored.state : IDLE_ACTION
}

function setAction(approval: ManagedProjectApproval, state: ManagedProjectApprovalActionState): void {
  if (!isSameApproval(currentApproval(approval), approval)) {
    clearAction(approval)

    return
  }

  $approvalActions.set({
    ...$approvalActions.get(),
    [approval.projectId]: { fingerprint: approvalFingerprint(approval), state }
  })
}

function clearAction(approval: ManagedProjectApproval): void {
  const actions = $approvalActions.get()

  if (actions[approval.projectId]?.fingerprint !== approvalFingerprint(approval)) {
    return
  }

  const next = { ...actions }
  delete next[approval.projectId]
  $approvalActions.set(next)
}

$projectRuntimes.subscribe(runtimes => {
  const current = $approvalActions.get()

  if (!Object.keys(current).length) {
    return
  }

  const sessionCounts = new Map<string, number>()

  for (const runtime of Object.values(runtimes)) {
    const sessionId = runtime.snapshot.canonical_session_id
    sessionCounts.set(sessionId, (sessionCounts.get(sessionId) ?? 0) + 1)
  }

  const valid = new Map<string, string>()
  const authority = projectRuntimeAuthority()

  for (const runtime of Object.values(runtimes)) {
    const snapshot = runtime.snapshot

    if (!snapshot.pending_approval || sessionCounts.get(snapshot.canonical_session_id) !== 1) {
      continue
    }

    valid.set(
      snapshot.project_id,
      approvalFingerprint({
        approvalId: snapshot.pending_approval.approval_id,
        approvalKind: snapshot.pending_approval.kind,
        bindingId: snapshot.binding_id,
        projectId: snapshot.project_id,
        requesterGeneration: authority.requesterGeneration,
        requesterScope: authority.scope,
        runtimeSessionId: snapshot.canonical_session_id,
        sessionId: snapshot.canonical_session_id,
        storedSessionId: snapshot.canonical_session_id,
        version: snapshot.version
      })
    )
  }

  const next = Object.fromEntries(
    Object.entries(current).filter(([projectId, stored]) => valid.get(projectId) === stored.fingerprint)
  )

  if (Object.keys(next).length !== Object.keys(current).length) {
    $approvalActions.set(next)
  }
})

function applyOutcome(
  approval: ManagedProjectApproval,
  requestedOutcome: ManagedProjectApprovalOutcome,
  mutationOutcome: ProjectMutationOutcome
): void {
  if (mutationOutcome.status === 'succeeded') {
    clearAction(approval)
  } else if (mutationOutcome.status === 'conflict') {
    setAction(approval, { status: 'conflict' })
  } else {
    setAction(approval, {
      intentId: mutationOutcome.intent_id,
      outcome: requestedOutcome,
      status: 'retry_required'
    })
  }
}

export function managedProjectApprovalForSession(
  sessionId: null | string
): ReadableAtom<ManagedProjectApprovalSelection> {
  return managedProjectApprovalForSurface(sessionId, sessionId)
}

export function managedProjectApprovalForSurface(
  runtimeSessionId: null | string,
  storedSessionId: null | string
): ReadableAtom<ManagedProjectApprovalSelection> {
  return computed(
    [$projectRuntimes, $activeGatewayProfile, $activeProjectId, $projectCatalogAuthority, $projects, $sessions],
    () => selectManagedProjectApproval(runtimeSessionId, storedSessionId)
  )
}

export function managedProjectApprovalAction(
  approval: ManagedProjectApproval
): ReadableAtom<ManagedProjectApprovalActionState> {
  return computed($approvalActions, () => actionState(approval))
}

export async function resolveManagedProjectApproval(
  approval: ManagedProjectApproval,
  outcome: ManagedProjectApprovalOutcome
): Promise<ProjectMutationOutcome> {
  assertCurrent(approval)

  if (!['idle', 'failed'].includes(actionState(approval).status)) {
    throw new Error('managed project approval action is unavailable')
  }

  setAction(approval, { outcome, status: 'submitting' })

  try {
    const result = await executeProjectMutation({
      expected_version: approval.version,
      name: 'approval.resolve',
      payload: { approval_id: approval.approvalId, outcome },
      project_id: approval.projectId
    })

    applyOutcome(approval, outcome, result)

    return result
  } catch (error) {
    setAction(approval, { status: 'failed' })
    throw error
  }
}

export async function retryManagedProjectApproval(approval: ManagedProjectApproval): Promise<ProjectMutationOutcome> {
  assertCurrent(approval)
  const state = actionState(approval)

  if (state.status !== 'retry_required') {
    throw new Error('managed project approval retry is unavailable')
  }

  setAction(approval, { outcome: state.outcome, status: 'submitting' })

  try {
    const result = await retryProjectMutation(state.intentId)

    applyOutcome(approval, state.outcome, result)

    return result
  } catch (error) {
    setAction(approval, { status: 'failed' })
    throw error
  }
}
