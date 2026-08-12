import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ProjectCommandResult, ProjectMutationIntent } from './project-command'
import {
  configureProjectCommandRuntime,
  executeProjectMutation,
  isProjectMutationRetryAvailable,
  retryProjectMutation
} from './project-command-runtime'

const syncProjectRuntime = vi.hoisted(() => vi.fn(async () => undefined))

vi.mock('./project-runtime', () => ({ syncProjectRuntime }))

const receipt = (overrides: Partial<ProjectCommandResult> = {}): ProjectCommandResult => ({
  accepted_turn_id: 'turn-a',
  active_control_version: 3,
  active_run_control: 'running',
  active_turn_id: 'turn-active',
  artifact: null,
  canonical_session_id: 'session-a',
  current_phase: 'implementation',
  last_event_sequence: 9,
  lifecycle: 'active',
  pending_approval_id: null,
  project_id: 'project-a',
  queue_depth: 0,
  version: 4,
  ...overrides
})

const intent: ProjectMutationIntent = {
  expected_version: 4,
  name: 'turn.enqueue',
  payload: { content: 'Ship the UI seam' },
  project_id: 'project-a'
}

let disposeRuntime: (() => void) | undefined

describe('project command runtime', () => {
  beforeEach(() => {
    syncProjectRuntime.mockClear()
  })

  afterEach(() => {
    disposeRuntime?.()
    disposeRuntime = undefined
  })

  it('fails locally without issuing a request after its configured scope closes', async () => {
    const request = vi.fn(async () => receipt())
    const dispose = configureProjectCommandRuntime(request, 'profile-a')

    dispose()

    await expect(executeProjectMutation(intent)).rejects.toThrow('project command runtime is not configured')
    expect(request).not.toHaveBeenCalled()
  })

  it('mints a different idempotency key for each fresh user intent', async () => {
    const request = vi.fn(async (_method: string, _params: Record<string, unknown>) => receipt())
    disposeRuntime = configureProjectCommandRuntime(request, 'profile-a')

    await executeProjectMutation(intent)
    await executeProjectMutation({ ...intent, payload: { content: 'Ship another turn' } })

    const firstParams = request.mock.calls[0][1]
    const secondParams = request.mock.calls[1][1]

    expect(firstParams.idempotency_key).toEqual(expect.any(String))
    expect(secondParams.idempotency_key).toEqual(expect.any(String))
    expect(firstParams.idempotency_key).not.toBe(secondParams.idempotency_key)
  })

  it('reuses the exact frozen envelope for its one automatic retry', async () => {
    const request = vi
      .fn()
      .mockRejectedValueOnce(new Error('request timed out after 30s: project.command'))
      .mockResolvedValueOnce(receipt())

    disposeRuntime = configureProjectCommandRuntime(request, 'profile-a')

    await expect(executeProjectMutation(intent)).resolves.toMatchObject({ status: 'succeeded' })

    expect(request).toHaveBeenCalledTimes(2)
    expect(request.mock.calls[1][1]).toBe(request.mock.calls[0][1])
    expect(Object.isFrozen(request.mock.calls[0][1])).toBe(true)
  })

  it('keeps the frozen envelope available for one explicit retry', async () => {
    const request = vi
      .fn()
      .mockRejectedValueOnce(new Error('request timed out after 30s: project.command'))
      .mockRejectedValueOnce(new Error('Hermes gateway connection closed'))
      .mockResolvedValueOnce(receipt())

    disposeRuntime = configureProjectCommandRuntime(request, 'profile-a')

    const outcome = await executeProjectMutation(intent)

    expect(outcome.status).toBe('retry_required')

    if (outcome.status !== 'retry_required') {
      throw new Error('expected an explicit retry intent')
    }

    expect(isProjectMutationRetryAvailable(outcome.intent_id)).toBe(true)
    await expect(retryProjectMutation(outcome.intent_id)).resolves.toMatchObject({ status: 'succeeded' })
    expect(request.mock.calls[2][1]).toBe(request.mock.calls[0][1])
    expect(isProjectMutationRetryAvailable(outcome.intent_id)).toBe(false)
  })

  it('lets exact cleanup invalidate its scope without letting stale cleanup close a replacement', async () => {
    const oldRequest = vi.fn(async () => receipt())
    const newRequest = vi.fn(async () => receipt())
    const disposeOld = configureProjectCommandRuntime(oldRequest, 'profile-old')

    disposeRuntime = configureProjectCommandRuntime(newRequest, 'profile-new')
    disposeOld()

    await expect(executeProjectMutation(intent)).resolves.toMatchObject({ status: 'succeeded' })
    expect(oldRequest).not.toHaveBeenCalled()
    expect(newRequest).toHaveBeenCalledTimes(1)

    disposeRuntime()
    disposeRuntime = undefined

    await expect(executeProjectMutation(intent)).rejects.toThrow('project command runtime is not configured')
    expect(newRequest).toHaveBeenCalledTimes(1)
  })

  it('fences a delayed result when its requester and profile are replaced', async () => {
    let resolveOld: ((value: unknown) => void) | undefined

    const oldRequest = vi.fn(
      () =>
        new Promise<unknown>(resolve => {
          resolveOld = resolve
        })
    )

    const newRequest = vi.fn(async () => receipt())

    const disposeOld = configureProjectCommandRuntime(oldRequest, 'profile-old')
    const pending = executeProjectMutation(intent)

    await vi.waitFor(() => expect(oldRequest).toHaveBeenCalledTimes(1))
    disposeRuntime = configureProjectCommandRuntime(newRequest, 'profile-new')
    resolveOld?.(receipt())

    await expect(pending).rejects.toThrow('project command requester changed')
    expect(syncProjectRuntime).not.toHaveBeenCalled()
    disposeOld()
  })

  it('clears an explicit retry intent when its requester and profile are replaced', async () => {
    const oldRequest = vi
      .fn()
      .mockRejectedValueOnce(new Error('request timed out after 30s: project.command'))
      .mockRejectedValueOnce(new Error('Hermes gateway connection closed'))

    const disposeOld = configureProjectCommandRuntime(oldRequest, 'profile-old')
    const outcome = await executeProjectMutation(intent)

    expect(outcome.status).toBe('retry_required')

    if (outcome.status !== 'retry_required') {
      throw new Error('expected an explicit retry intent')
    }

    disposeRuntime = configureProjectCommandRuntime(async () => receipt(), 'profile-new')

    expect(isProjectMutationRetryAvailable(outcome.intent_id)).toBe(false)
    await expect(retryProjectMutation(outcome.intent_id)).rejects.toThrow('project command retry intent is unavailable')
    disposeOld()
  })
})
