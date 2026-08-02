import { JsonRpcGatewayError } from '@hermes/shared'

import type { ProjectArtifactPresentation, ProjectRunControlState } from '@/types/hermes'

import { isCredentialFreeHttpUrl } from './project-artifact-validation'

export type ProjectCommandRequester = (method: string, params: Record<string, unknown>) => Promise<unknown>
export type ProjectRuntimeSynchronizer = (projectId: string, minimumSequence: number) => Promise<void>

export interface ProjectCommandResult {
  accepted_turn_id: null | string
  active_control_version: null | number
  active_run_control: null | ProjectRunControlState
  active_turn_id: null | string
  artifact: null | { artifact_id: string; presentation: ProjectArtifactPresentation }
  canonical_session_id: null | string
  current_phase: null | string
  last_event_sequence: number
  lifecycle: 'active' | 'awaiting_acceptance' | 'completed'
  pending_approval_id: null | string
  project_id: string
  queue_depth: number
  version: number
}

export type ProjectMutationName =
  | 'project.create'
  | 'project.rename'
  | 'turn.enqueue'
  | 'run.stop'
  | 'run.resume'
  | 'approval.resolve'
  | 'project.mark_technically_complete'
  | 'project.accept_completion'
  | 'project.reopen'

export interface ProjectMutationIntent {
  expected_version: number
  name: ProjectMutationName
  payload: Record<string, unknown>
  project_id: null | string
}

export type ProjectMutationOutcome =
  | { result: ProjectCommandResult; status: 'succeeded' }
  | { status: 'conflict' }
  | { intent_id: string; status: 'retry_required' }

export interface ProjectMutationExecutor {
  configure(request: ProjectCommandRequester, scope?: string): void
  execute(intent: ProjectMutationIntent): Promise<ProjectMutationOutcome>
  executeProjectMutation(intent: ProjectMutationIntent): Promise<ProjectMutationOutcome>
  hasPendingRetry(intentId: string): boolean
  pendingRetryCount(): number
  retry(intentId: string): Promise<ProjectMutationOutcome>
}

export interface ProjectMutationExecutorOptions {
  createIdempotencyKey: () => string
  request: ProjectCommandRequester
  scope?: string
  sync: ProjectRuntimeSynchronizer
}

interface ProjectCommandParams {
  [key: string]: unknown
  expected_version: number
  idempotency_key: string
  name: string
  payload: Record<string, unknown>
  project_id: null | string
}

interface RequestContext {
  generation: number
  request: ProjectCommandRequester
}

interface PendingRetry {
  context: RequestContext
  params: ProjectCommandParams
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === 'object' && !Array.isArray(value)

const isText = (value: unknown): value is string => typeof value === 'string' && value.length > 0

const isMutationName = (value: unknown): value is ProjectMutationName =>
  [
    'project.create',
    'project.rename',
    'turn.enqueue',
    'run.stop',
    'run.resume',
    'approval.resolve',
    'project.mark_technically_complete',
    'project.accept_completion',
    'project.reopen'
  ].includes(value as ProjectMutationName)

const isRunControlState = (value: unknown): value is ProjectRunControlState =>
  ['running', 'awaiting_approval', 'stop_requested', 'stopped', 'resume_requested'].includes(
    value as ProjectRunControlState
  )

const isNonNegativeInteger = (value: unknown): value is number =>
  typeof value === 'number' && Number.isSafeInteger(value) && value >= 0

function hasExactKeys(value: Record<string, unknown>, keys: readonly string[]): boolean {
  const actual = Object.keys(value)

  return actual.length === keys.length && actual.every(key => keys.includes(key))
}

function isPresentation(value: unknown): value is ProjectArtifactPresentation {
  if (
    !isRecord(value) ||
    !hasExactKeys(value, ['kind', 'label', 'created_at', 'size_bytes', 'sha256', 'open_target']) ||
    !['file', 'image', 'link'].includes(value.kind as string) ||
    !isText(value.label) ||
    /[\\/]/.test(value.label) ||
    value.label === '.' ||
    value.label === '..' ||
    !isNonNegativeInteger(value.created_at) ||
    (value.size_bytes !== null && !isNonNegativeInteger(value.size_bytes)) ||
    (value.sha256 !== null && (!isText(value.sha256) || !/^[a-f0-9]{64}$/.test(value.sha256)))
  ) {
    return false
  }

  if (value.open_target === null) {
    return true
  }

  if (value.kind !== 'link') {
    return false
  }

  if (
    !isRecord(value.open_target) ||
    !hasExactKeys(value.open_target, ['kind', 'href']) ||
    value.open_target.kind !== 'external_url' ||
    !isText(value.open_target.href)
  ) {
    return false
  }

  return isCredentialFreeHttpUrl(value.open_target.href)
}

function isArtifact(value: unknown): value is { artifact_id: string; presentation: ProjectArtifactPresentation } {
  return (
    isRecord(value) &&
    hasExactKeys(value, ['artifact_id', 'presentation']) &&
    isText(value.artifact_id) &&
    isPresentation(value.presentation)
  )
}

function hasCoherentActiveRun(value: Record<string, unknown>): boolean {
  const allNull =
    value.active_turn_id === null && value.active_run_control === null && value.active_control_version === null

  const allValid =
    isText(value.active_turn_id) &&
    isRunControlState(value.active_run_control) &&
    isNonNegativeInteger(value.active_control_version)

  return allNull || allValid
}

function hasCoherentLifecycle(value: Record<string, unknown>): boolean {
  if (value.lifecycle !== 'active') {
    return (
      value.active_turn_id === null &&
      value.active_run_control === null &&
      value.active_control_version === null &&
      value.pending_approval_id === null
    )
  }

  return (value.pending_approval_id !== null) === (value.active_run_control === 'awaiting_approval')
}

export function isProjectCommandResult(value: unknown): value is ProjectCommandResult {
  return (
    isRecord(value) &&
    hasExactKeys(value, [
      'project_id',
      'lifecycle',
      'version',
      'canonical_session_id',
      'queue_depth',
      'active_turn_id',
      'active_run_control',
      'pending_approval_id',
      'last_event_sequence',
      'current_phase',
      'artifact',
      'accepted_turn_id',
      'active_control_version'
    ]) &&
    isText(value.project_id) &&
    ['active', 'awaiting_acceptance', 'completed'].includes(value.lifecycle as string) &&
    isNonNegativeInteger(value.version) &&
    (value.canonical_session_id === null || isText(value.canonical_session_id)) &&
    isNonNegativeInteger(value.queue_depth) &&
    (value.active_turn_id === null || isText(value.active_turn_id)) &&
    (value.active_run_control === null || isRunControlState(value.active_run_control)) &&
    hasCoherentActiveRun(value) &&
    (value.pending_approval_id === null || isText(value.pending_approval_id)) &&
    hasCoherentLifecycle(value) &&
    isNonNegativeInteger(value.last_event_sequence) &&
    (value.current_phase === null || isText(value.current_phase)) &&
    (value.artifact === null || isArtifact(value.artifact)) &&
    (value.accepted_turn_id === null || isText(value.accepted_turn_id)) &&
    (value.active_control_version === null || isNonNegativeInteger(value.active_control_version))
  )
}

function freeze<T>(value: T): T {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) {
    return value
  }

  for (const item of Object.values(value as Record<string, unknown>)) {
    freeze(item)
  }

  return Object.freeze(value)
}

function commandParams(intent: ProjectMutationIntent, idempotencyKey: string): ProjectCommandParams {
  if (
    !isMutationName(intent.name) ||
    (intent.project_id !== null && !isText(intent.project_id)) ||
    !isRecord(intent.payload) ||
    !isNonNegativeInteger(intent.expected_version) ||
    !isText(idempotencyKey)
  ) {
    throw new Error('invalid project mutation intent')
  }

  return freeze({
    expected_version: intent.expected_version,
    idempotency_key: idempotencyKey,
    name: intent.name,
    payload: structuredClone(intent.payload),
    project_id: intent.project_id
  })
}

function isAmbiguousTransportError(error: unknown): boolean {
  return (
    error instanceof Error &&
    !(error instanceof JsonRpcGatewayError) &&
    /(?:request timed out|gateway connection closed|websocket closed|gateway not connected)/i.test(error.message)
  )
}

function isConflict(error: unknown, projectId: null | string): boolean {
  if (!(error instanceof JsonRpcGatewayError) || error.code !== 5065 || !isRecord(error.data)) {
    return false
  }

  const data = error.data

  if (!isText(data.code) || (data.project_id !== undefined && !isText(data.project_id))) {
    return false
  }

  if (projectId !== null && data.project_id !== projectId) {
    return false
  }

  if (data.code === 'PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT') {
    return (
      (projectId === null
        ? hasExactKeys(data, ['code', 'current_version']) ||
          hasExactKeys(data, ['code', 'project_id', 'current_version'])
        : hasExactKeys(data, ['code', 'project_id', 'current_version'])) && isNonNegativeInteger(data.current_version)
    )
  }

  if (data.code === 'PROJECT_RUNTIME_CONTROL_VERSION_CONFLICT') {
    return (
      (projectId === null
        ? hasExactKeys(data, ['code', 'current_control_version']) ||
          hasExactKeys(data, ['code', 'project_id', 'current_control_version'])
        : hasExactKeys(data, ['code', 'project_id', 'current_control_version'])) &&
      isNonNegativeInteger(data.current_control_version)
    )
  }

  return false
}

export function createProjectMutationExecutor(options: ProjectMutationExecutorOptions): ProjectMutationExecutor {
  let request = options.request
  let scope = options.scope
  let generation = 0
  const pendingRetries = new Map<string, PendingRetry>()

  const context = (): RequestContext => ({ generation, request })

  const assertCurrent = (active: RequestContext): void => {
    if (active.generation !== generation || active.request !== request) {
      throw new Error('project command requester changed')
    }
  }

  const sync = async (projectId: string, minimumSequence: number, active: RequestContext): Promise<void> => {
    assertCurrent(active)
    await options.sync(projectId, minimumSequence)
    assertCurrent(active)
  }

  const run = async (
    params: ProjectCommandParams,
    active: RequestContext,
    retryBudget: number
  ): Promise<ProjectMutationOutcome> => {
    assertCurrent(active)
    let value: unknown

    try {
      value = await active.request('project.command', params)
      assertCurrent(active)
    } catch (error) {
      assertCurrent(active)

      if (isConflict(error, params.project_id)) {
        if (params.project_id !== null) {
          await sync(params.project_id, 0, active)
        }

        return { status: 'conflict' }
      }

      if (!isAmbiguousTransportError(error)) {
        throw error
      }

      if (retryBudget > 0) {
        return run(params, active, retryBudget - 1)
      }

      pendingRetries.set(params.idempotency_key, { context: active, params })

      return { intent_id: params.idempotency_key, status: 'retry_required' }
    }

    if (
      !isProjectCommandResult(value) ||
      (params.name === 'turn.enqueue') !== (value.accepted_turn_id !== null) ||
      (params.project_id !== null && value.project_id !== params.project_id)
    ) {
      throw new Error('invalid project command result')
    }

    await sync(value.project_id, value.last_event_sequence, active)

    return { result: value, status: 'succeeded' }
  }

  const executeProjectMutation = (intent: ProjectMutationIntent): Promise<ProjectMutationOutcome> => {
    const params = commandParams(intent, options.createIdempotencyKey())

    return run(params, context(), 1)
  }

  return {
    configure(nextRequest, nextScope) {
      if (request === nextRequest && scope === nextScope) {
        return
      }

      request = nextRequest
      scope = nextScope
      generation += 1
      pendingRetries.clear()
    },
    execute: executeProjectMutation,
    executeProjectMutation,
    hasPendingRetry(intentId) {
      return pendingRetries.has(intentId)
    },
    pendingRetryCount() {
      return pendingRetries.size
    },
    retry(intentId) {
      const pending = pendingRetries.get(intentId)

      if (!pending) {
        return Promise.reject(new Error('project command retry intent is unavailable'))
      }

      assertCurrent(pending.context)
      pendingRetries.delete(intentId)

      return run(pending.params, pending.context, 0)
    }
  }
}
