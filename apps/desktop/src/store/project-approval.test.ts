import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ProjectRuntimeSnapshot } from '@/types/hermes'

import { $activeGatewayProfile } from './profile'
import type { ProjectCommandResult } from './project-command'
import type { ProjectMutationOutcome } from './project-command-runtime'
import {
  $projectRuntimes,
  configureProjectRuntimeRequester,
  projectRuntimeAuthority,
  resetProjectRuntimeStore
} from './project-runtime'

const commandRuntime = vi.hoisted(() => ({
  executeProjectMutation: vi.fn(),
  retryProjectMutation: vi.fn()
}))

vi.mock('./project-command-runtime', () => commandRuntime)

import {
  managedProjectApprovalAction,
  managedProjectApprovalForSession,
  resolveManagedProjectApproval,
  retryManagedProjectApproval
} from './project-approval'

const snapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: { control_state: 'awaiting_approval', control_version: 3, turn_id: 'turn-a' },
  artifacts: [],
  binding_id: 'binding-a',
  block: null,
  canonical_session_id: 'session-a',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 7,
  lifecycle: 'active',
  pending_approval: { approval_id: 'approval-a', kind: 'tool' },
  project_id: 'project-a',
  queue: [],
  transcript: [],
  transcript_revision: 2,
  version: 4,
  ...overrides
})

function install(...snapshots: ProjectRuntimeSnapshot[]): void {
  $projectRuntimes.set(Object.fromEntries(snapshots.map(value => [value.project_id, { events: [], snapshot: value }])))
}

function deferred<T>(): {
  promise: Promise<T>
  reject: (reason?: unknown) => void
  resolve: (value: T) => void
} {
  let reject!: (reason?: unknown) => void
  let resolve!: (value: T) => void

  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    reject = rejectPromise
    resolve = resolvePromise
  })

  return { promise, reject, resolve }
}

const receipt = (overrides: Partial<ProjectCommandResult> = {}): ProjectCommandResult => ({
  accepted_turn_id: null,
  active_control_version: null,
  active_run_control: null,
  active_turn_id: null,
  artifact: null,
  canonical_session_id: 'session-b',
  current_phase: 'implementation',
  last_event_sequence: 8,
  lifecycle: 'active',
  pending_approval_id: null,
  project_id: 'project-b',
  queue_depth: 0,
  version: 9,
  ...overrides
})

describe('managed project approval store', () => {
  beforeEach(() => {
    commandRuntime.executeProjectMutation.mockReset()
    commandRuntime.retryProjectMutation.mockReset()
    $activeGatewayProfile.set('default')
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
  })

  afterEach(() => {
    $projectRuntimes.set({})
    configureProjectRuntimeRequester(undefined)
  })

  it('selects one exact canonical session and fails closed on prefixes or duplicate matches', () => {
    const authority = projectRuntimeAuthority()
    install(snapshot(), snapshot({ canonical_session_id: 'session-b', project_id: 'project-b' }))

    expect(managedProjectApprovalForSession('session-a').get()).toEqual({
      approval: {
        approvalId: 'approval-a',
        approvalKind: 'tool',
        bindingId: 'binding-a',
        projectId: 'project-a',
        requesterGeneration: authority.requesterGeneration,
        requesterScope: authority.scope,
        runtimeSessionId: 'session-a',
        sessionId: 'session-a',
        storedSessionId: 'session-a',
        version: 4
      },
      managed: true
    })
    expect(managedProjectApprovalForSession('session').get()).toEqual({ approval: null, managed: true })

    install(snapshot(), snapshot({ binding_id: 'binding-b', project_id: 'project-b' }))

    expect(managedProjectApprovalForSession('session-a').get()).toEqual({ approval: null, managed: true })
  })

  it('treats a matching managed snapshot without a pending approval as canonical absence', () => {
    install(snapshot({ active_run: null, pending_approval: null }))

    expect(managedProjectApprovalForSession('session-a').get()).toEqual({ approval: null, managed: true })
  })

  it('sends the exact managed approval command and retains canonical state on conflict', async () => {
    install(snapshot())
    const canonicalBefore = $projectRuntimes.get()
    const selection = managedProjectApprovalForSession('session-a').get()

    expect(selection.approval).not.toBeNull()
    expect(selection.approval?.bindingId).toBe('binding-a')
    commandRuntime.executeProjectMutation.mockResolvedValue({ status: 'conflict' })

    const outcome = await resolveManagedProjectApproval(selection.approval!, 'approved')

    expect(outcome).toEqual({ status: 'conflict' })
    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 4,
      name: 'approval.resolve',
      payload: { approval_id: 'approval-a', outcome: 'approved' },
      project_id: 'project-a'
    })
    expect($projectRuntimes.get()).toBe(canonicalBefore)
    expect(managedProjectApprovalAction(selection.approval!).get()).toEqual({ status: 'conflict' })
  })

  it('keeps a retry-required command frozen behind the retry seam', async () => {
    install(
      snapshot({
        canonical_session_id: 'session-b',
        pending_approval: { approval_id: 'approval-b', kind: 'tool' },
        project_id: 'project-b',
        version: 8
      })
    )
    const approval = managedProjectApprovalForSession('session-b').get().approval!

    commandRuntime.executeProjectMutation.mockResolvedValue({ intent_id: 'intent-a', status: 'retry_required' })
    commandRuntime.retryProjectMutation.mockResolvedValue({
      result: { project_id: 'project-b' },
      status: 'succeeded'
    })

    await resolveManagedProjectApproval(approval, 'denied')

    expect(managedProjectApprovalAction(approval).get()).toEqual({
      intentId: 'intent-a',
      outcome: 'denied',
      status: 'retry_required'
    })

    let finishRetry: ((value: ProjectMutationOutcome) => void) | undefined
    commandRuntime.retryProjectMutation.mockImplementation(
      () =>
        new Promise(resolve => {
          finishRetry = resolve
        })
    )

    const retry = retryManagedProjectApproval(approval)

    expect(managedProjectApprovalAction(approval).get()).toEqual({
      outcome: 'denied',
      status: 'submitting'
    })

    finishRetry?.({ result: receipt(), status: 'succeeded' })
    await retry

    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledTimes(1)
    expect(commandRuntime.retryProjectMutation).toHaveBeenCalledWith('intent-a')
    expect(managedProjectApprovalAction(approval).get()).toEqual({ status: 'idle' })
  })

  it('does not expose an old profile action to a colliding approval with another binding', async () => {
    install(snapshot())
    const oldApproval = managedProjectApprovalForSession('session-a').get().approval!
    let finish: ((value: ProjectMutationOutcome) => void) | undefined

    commandRuntime.executeProjectMutation.mockImplementation(
      () =>
        new Promise(resolve => {
          finish = resolve
        })
    )

    const oldRequest = resolveManagedProjectApproval(oldApproval, 'approved')

    install(snapshot({ binding_id: 'binding-new-profile' }))
    const newApproval = managedProjectApprovalForSession('session-a').get().approval!

    expect(managedProjectApprovalAction(newApproval).get()).toEqual({ status: 'idle' })

    finish?.({ intent_id: 'intent-old-profile', status: 'retry_required' })
    await oldRequest

    expect(managedProjectApprovalAction(newApproval).get()).toEqual({ status: 'idle' })
  })

  it('keeps a new-profile submitting lock when an identical old approval resolves late', async () => {
    $activeGatewayProfile.set('profile-old')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-old'
    )
    install(snapshot())
    const oldApproval = managedProjectApprovalForSession('session-a').get().approval!
    const oldCommand = deferred<ProjectMutationOutcome>()
    const newCommand = deferred<ProjectMutationOutcome>()

    commandRuntime.executeProjectMutation
      .mockImplementationOnce(() => oldCommand.promise)
      .mockImplementationOnce(() => newCommand.promise)
      .mockResolvedValue({ status: 'conflict' })

    const oldRequest = resolveManagedProjectApproval(oldApproval, 'approved')

    $activeGatewayProfile.set('profile-new')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      ' profile-new '
    )
    install(snapshot())
    const newApproval = managedProjectApprovalForSession('session-a').get().approval!
    const newRequest = resolveManagedProjectApproval(newApproval, 'denied')

    expect(newApproval.requesterGeneration).toBeGreaterThan(oldApproval.requesterGeneration)
    expect(newApproval.requesterScope).toBe('profile-new')
    expect(managedProjectApprovalAction(newApproval).get()).toEqual({
      outcome: 'denied',
      status: 'submitting'
    })

    oldCommand.resolve({ result: receipt({ project_id: 'project-a' }), status: 'succeeded' })
    await oldRequest

    expect(managedProjectApprovalAction(newApproval).get()).toEqual({
      outcome: 'denied',
      status: 'submitting'
    })
    await expect(resolveManagedProjectApproval(newApproval, 'approved')).rejects.toThrow(
      'managed project approval action is unavailable'
    )
    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledTimes(2)

    newCommand.resolve({ status: 'conflict' })
    await newRequest
  })

  it('keeps a replacement-requester lock when an identical old approval rejects late in the same profile', async () => {
    $activeGatewayProfile.set('profile-a')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      ' profile-a '
    )
    install(snapshot())
    const oldApproval = managedProjectApprovalForSession('session-a').get().approval!
    const oldCommand = deferred<ProjectMutationOutcome>()
    const newCommand = deferred<ProjectMutationOutcome>()

    commandRuntime.executeProjectMutation
      .mockImplementationOnce(() => oldCommand.promise)
      .mockImplementationOnce(() => newCommand.promise)
      .mockResolvedValue({ status: 'conflict' })

    const oldRequest = resolveManagedProjectApproval(oldApproval, 'approved')

    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-a'
    )
    install(snapshot())
    const newApproval = managedProjectApprovalForSession('session-a').get().approval!
    const newRequest = resolveManagedProjectApproval(newApproval, 'denied')

    expect(newApproval.requesterGeneration).toBeGreaterThan(oldApproval.requesterGeneration)
    expect(newApproval.requesterScope).toBe(oldApproval.requesterScope)
    expect(managedProjectApprovalAction(newApproval).get()).toEqual({
      outcome: 'denied',
      status: 'submitting'
    })

    oldCommand.reject(new Error('old requester failed'))
    await expect(oldRequest).rejects.toThrow('old requester failed')

    expect(managedProjectApprovalAction(newApproval).get()).toEqual({
      outcome: 'denied',
      status: 'submitting'
    })
    await expect(resolveManagedProjectApproval(newApproval, 'approved')).rejects.toThrow(
      'managed project approval action is unavailable'
    )
    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledTimes(2)

    newCommand.resolve({ status: 'conflict' })
    await newRequest
  })

  it('rejects a stale approval context before either command seam is called', async () => {
    install(
      snapshot({
        canonical_session_id: 'session-c',
        pending_approval: { approval_id: 'approval-c', kind: 'tool' },
        project_id: 'project-c',
        version: 10
      })
    )
    const approval = managedProjectApprovalForSession('session-c').get().approval!

    install(
      snapshot({
        canonical_session_id: 'session-c',
        pending_approval: { approval_id: 'approval-d', kind: 'tool' },
        project_id: 'project-c',
        version: 11
      })
    )

    await expect(resolveManagedProjectApproval(approval, 'approved')).rejects.toThrow(
      'managed project approval changed'
    )
    await expect(retryManagedProjectApproval(approval)).rejects.toThrow('managed project approval changed')
    expect(commandRuntime.executeProjectMutation).not.toHaveBeenCalled()
    expect(commandRuntime.retryProjectMutation).not.toHaveBeenCalled()
  })
})
