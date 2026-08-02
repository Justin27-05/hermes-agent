import { JsonRpcGatewayError } from '@hermes/shared'
import { describe, expect, it, vi } from 'vitest'

import {
  createProjectMutationExecutor,
  isProjectCommandResult,
  type ProjectCommandResult,
  type ProjectMutationIntent
} from './project-command'

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
  payload: { content: 'Ship the preflight' },
  project_id: 'project-a'
}

describe('project command mutation executor', () => {
  it('accepts only an exact amended command receipt', () => {
    expect(isProjectCommandResult(receipt())).toBe(true)
    expect(isProjectCommandResult({ ...receipt(), accepted_turn_id: true })).toBe(false)
    expect(isProjectCommandResult({ ...receipt(), last_event_sequence: true })).toBe(false)
    expect(isProjectCommandResult({ ...receipt(), leaked: 'binding-id' })).toBe(false)
  })

  it('rejects an incoherent control receipt and unknown lifecycle', () => {
    expect(isProjectCommandResult({ ...receipt(), active_control_version: null })).toBe(false)
    expect(isProjectCommandResult({ ...receipt(), lifecycle: 'deleted' })).toBe(false)
    expect(isProjectCommandResult({ ...receipt(), lifecycle: 'completed' })).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        active_control_version: null,
        active_run_control: null,
        active_turn_id: null,
        pending_approval_id: 'approval-a'
      })
    ).toBe(false)
    expect(isProjectCommandResult({ ...receipt(), active_run_control: 'awaiting_approval' })).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        active_run_control: 'awaiting_approval',
        pending_approval_id: 'approval-a'
      })
    ).toBe(true)
  })

  it('accepts only safe artifact digests and credential-free external targets', () => {
    const artifact = {
      artifact_id: 'artifact-a',
      presentation: {
        created_at: 1,
        kind: 'link' as const,
        label: 'report.pdf',
        open_target: { href: 'https://example.test/report.pdf?download=1', kind: 'external_url' as const },
        sha256: 'a'.repeat(64),
        size_bytes: 10
      }
    }

    expect(isProjectCommandResult({ ...receipt(), artifact })).toBe(true)

    for (const href of [
      'https://192.0.0.9/report.pdf',
      'https://[2001:1::1]/report.pdf',
      'https://[3ffe::1]/report.pdf',
      'https://[3fff:1000::1]/report.pdf'
    ]) {
      expect(
        isProjectCommandResult({
          ...receipt(),
          artifact: {
            ...artifact,
            presentation: { ...artifact.presentation, open_target: { href, kind: 'external_url' } }
          }
        })
      ).toBe(true)
    }

    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: { ...artifact, presentation: { ...artifact.presentation, sha256: 'x' } }
      })
    ).toBe(false)

    for (const href of [
      'https://example.test/report.pdf?X-Amz-Credential=secret',
      'https://example.test/report.pdf?x-auth-token=secret',
      'https://example.test/report.pdf?a%21pi%21key=secret',
      'https://example.test/report.pdf?access_token%3Dsecret',
      'https://example.test/report.pdf?access_token%253Dsecret',
      'https://example.test/report.pdf?%61ccess_token=secret',
      'https://example.test/report.pdf?%2561ccess_token=secret',
      'https://example.test/report.pdf?note=%7F',
      'https://example.test/report.pdf?note=%C2%85',
      'https://example.test/report.pdf?no%E2%80%8Bte=1',
      'https://example.test/report.pdf#access_token=secret',
      'https://example.test/report.pdf#token=secret&view=full',
      'https://example.test/report.pdf#access_token%3Dsecret',
      'https://example.test/report.pdf#%61ccess_token%3Dsecret',
      'https://example.test/report.pdf#%61ccess_token=secret',
      'https://example.test/report.pdf#%2561ccess_token=secret',
      'https://example.test/report.pdf#access_token%253Dsecret',
      'https://example.test/report.pdf#access_token%25253Dsecret',
      'https://example.test/report.pdf#access_token%',
      'https://example.test/report.pdf#note%00value',
      'https://example.test/report.pdf#note%E2%80%83value',
      'https://example.test/report.pdf#note\u0085value',
      'https://example.test/report.pdf#note\u200Bvalue',
      `https://example.test/report.pdf#${'a'.repeat(4097)}`,
      ' https://example.test/report.pdf',
      'http:report.pdf',
      'http://localhost/report.pdf',
      'http://preview.localhost/report.pdf',
      'http://local/report.pdf',
      'http://printer.local/report.pdf',
      'http://home.arpa/report.pdf',
      'http://router.home.arpa/report.pdf',
      'http://127。0。0。1/report.pdf',
      'http://１２７．０．０．１/report.pdf',
      'http://ⓛⓞⓒⓐⓛⓗⓞⓢⓣ/report.pdf',
      'http://127.1/report.pdf',
      'http://2130706433/report.pdf',
      'http://0x7f000001/report.pdf',
      'http://0177.0.0.1/report.pdf',
      'http://127.0.0.1/report.pdf',
      'http://10.0.0.1/report.pdf',
      'http://172.16.0.1/report.pdf',
      'http://192.168.0.1/report.pdf',
      'http://169.254.1.1/report.pdf',
      'http://0.0.0.0/report.pdf',
      'http://192.0.0.1/report.pdf',
      'http://192.0.2.1/report.pdf',
      'http://198.18.0.1/report.pdf',
      'http://198.51.100.1/report.pdf',
      'http://203.0.113.1/report.pdf',
      'http://224.0.0.1/report.pdf',
      'http://240.0.0.1/report.pdf',
      'http://[::1]/report.pdf',
      'http://[::ffff:127.0.0.1]/report.pdf',
      'http://[::ffff:10.0.0.1]/report.pdf',
      'http://[::ffff:192.0.2.1]/report.pdf',
      'http://[::ffff:224.0.0.1]/report.pdf',
      'http://[64:ff9b:1::1]/report.pdf',
      'http://[100::1]/report.pdf',
      'http://[2001::1]/report.pdf',
      'http://[2001:db8::1]/report.pdf',
      'http://[2002::1]/report.pdf',
      'http://[3fff::1]/report.pdf',
      'http://[3fff:0fff::1]/report.pdf',
      'http://[fc00::1]/report.pdf',
      'http://[fe80::1]/report.pdf',
      'http://[fec0::1]/report.pdf',
      'http://[ff02::1]/report.pdf',
      'http://[::]/report.pdf'
    ]) {
      expect(
        isProjectCommandResult({
          ...receipt(),
          artifact: {
            ...artifact,
            presentation: { ...artifact.presentation, open_target: { href, kind: 'external_url' } }
          }
        })
      ).toBe(false)
    }

    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: {
            ...artifact.presentation,
            open_target: { href: 'https://example.test/report.pdf#authentication', kind: 'external_url' }
          }
        }
      })
    ).toBe(true)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: {
            ...artifact.presentation,
            open_target: { href: 'https://[2606:4700:4700::1111]/report.pdf', kind: 'external_url' }
          }
        }
      })
    ).toBe(true)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: {
            ...artifact.presentation,
            open_target: { href: 'https://8.8.8.8/report.pdf', kind: 'external_url' }
          }
        }
      })
    ).toBe(true)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: {
            ...artifact.presentation,
            open_target: { href: 'https://user:pass@example.test', kind: 'external_url' }
          }
        }
      })
    ).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: {
            ...artifact.presentation,
            open_target: { href: 'https://example.test/report.pdf?access_token=secret', kind: 'external_url' }
          }
        }
      })
    ).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: { ...artifact, presentation: { ...artifact.presentation, label: '.' } }
      })
    ).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: { ...artifact, presentation: { ...artifact.presentation, open_target: null } }
      })
    ).toBe(true)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: { ...artifact, presentation: { ...artifact.presentation, kind: 'file' } }
      })
    ).toBe(false)
    expect(
      isProjectCommandResult({
        ...receipt(),
        artifact: {
          ...artifact,
          presentation: { ...artifact.presentation, kind: 'link', open_target: null }
        }
      })
    ).toBe(true)
  })

  it('rejects an arbitrary command name before it reaches the requester', async () => {
    const request = vi.fn(async () => receipt())

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request,
      sync: async () => undefined
    })

    expect(() => executor.execute({ ...intent, name: 'project.status' } as unknown as typeof intent)).toThrow(
      'invalid project mutation intent'
    )

    expect(request).not.toHaveBeenCalled()
  })

  it('reuses the exact command envelope for one ambiguous timeout retry', async () => {
    const request = vi
      .fn()
      .mockRejectedValueOnce(new Error('request timed out after 30s: project.command'))
      .mockResolvedValueOnce(receipt())

    const sync = vi.fn(async () => undefined)
    const executor = createProjectMutationExecutor({ createIdempotencyKey: () => 'intent-a', request, sync })

    await expect(executor.executeProjectMutation(intent)).resolves.toMatchObject({ status: 'succeeded' })

    expect(request).toHaveBeenCalledTimes(2)
    expect(request.mock.calls[0][0]).toBe('project.command')
    expect(request.mock.calls[1][0]).toBe('project.command')
    expect(request.mock.calls[1][1]).toBe(request.mock.calls[0][1])
    expect(request.mock.calls[0][1]).toEqual({
      expected_version: 4,
      idempotency_key: 'intent-a',
      name: 'turn.enqueue',
      payload: { content: 'Ship the preflight' },
      project_id: 'project-a'
    })
    expect(sync).toHaveBeenCalledWith('project-a', 9)
  })

  it('keeps one explicit retry intent after a second ambiguous timeout', async () => {
    const request = vi
      .fn()
      .mockRejectedValueOnce(new Error('request timed out after 30s: project.command'))
      .mockRejectedValueOnce(new Error('Hermes gateway connection closed'))
      .mockResolvedValueOnce(receipt())

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request,
      sync: async () => undefined
    })

    const outcome = await executor.execute(intent)

    expect(outcome).toMatchObject({ intent_id: 'intent-a', status: 'retry_required' })
    expect(executor.pendingRetryCount()).toBe(1)
    expect(request).toHaveBeenCalledTimes(2)

    await expect(executor.retry('intent-a')).resolves.toMatchObject({ status: 'succeeded' })
    expect(executor.pendingRetryCount()).toBe(0)
    expect(request).toHaveBeenCalledTimes(3)
  })

  it('syncs a narrowed command conflict without resubmitting it', async () => {
    const request = vi.fn().mockRejectedValue(
      new JsonRpcGatewayError({
        code: 5065,
        data: { code: 'PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT', current_version: 5, project_id: 'project-a' }
      })
    )

    const sync = vi.fn(async () => undefined)
    const executor = createProjectMutationExecutor({ createIdempotencyKey: () => 'intent-a', request, sync })

    await expect(executor.execute(intent)).resolves.toMatchObject({ status: 'conflict' })

    expect(request).toHaveBeenCalledTimes(1)
    expect(sync).toHaveBeenCalledWith('project-a', 0)
  })

  it('rejects a non-create receipt whose project identity differs from its command', async () => {
    const request = vi.fn(async () => receipt({ project_id: 'project-b' }))

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request,
      sync: async () => undefined
    })

    await expect(executor.execute(intent)).rejects.toThrow('invalid project command result')
  })

  it('fails closed instead of syncing a conflict for another project', async () => {
    const error = new JsonRpcGatewayError({
      code: 5065,
      data: { code: 'PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT', current_version: 5, project_id: 'project-b' }
    })

    const request = vi.fn().mockRejectedValue(error)
    const sync = vi.fn(async () => undefined)
    const executor = createProjectMutationExecutor({ createIdempotencyKey: () => 'intent-a', request, sync })

    await expect(executor.execute(intent)).rejects.toBe(error)
    expect(sync).not.toHaveBeenCalled()
  })

  it('fails closed on a non-create conflict that omits its project identity', async () => {
    const error = new JsonRpcGatewayError({
      code: 5065,
      data: { code: 'PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT', current_version: 5 }
    })

    const sync = vi.fn(async () => undefined)

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request: vi.fn().mockRejectedValue(error),
      sync
    })

    await expect(executor.execute(intent)).rejects.toBe(error)
    expect(sync).not.toHaveBeenCalled()
  })

  it('fails closed on a conflict payload carried by the wrong outer JSON-RPC code', async () => {
    const error = new JsonRpcGatewayError({
      code: 4091,
      data: { code: 'PROJECT_RUNTIME_PROJECT_VERSION_CONFLICT', current_version: 5, project_id: 'project-a' }
    })

    const sync = vi.fn(async () => undefined)

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request: vi.fn().mockRejectedValue(error),
      sync
    })

    await expect(executor.execute(intent)).rejects.toBe(error)
    expect(sync).not.toHaveBeenCalled()
  })

  it('fails closed on a mixed project/control conflict payload', async () => {
    const error = new JsonRpcGatewayError({
      code: 5065,
      data: {
        code: 'PROJECT_RUNTIME_CONTROL_VERSION_CONFLICT',
        current_control_version: 7,
        current_version: 5,
        project_id: 'project-a'
      }
    })

    const sync = vi.fn(async () => undefined)

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request: vi.fn().mockRejectedValue(error),
      sync
    })

    await expect(executor.execute(intent)).rejects.toBe(error)
    expect(sync).not.toHaveBeenCalled()
  })

  it('syncs a successful receipt through its event high-water before reporting visibility', async () => {
    let releaseSync: (() => void) | undefined

    const sync = vi.fn(
      () =>
        new Promise<void>(resolve => {
          releaseSync = resolve
        })
    )

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request: async () => receipt({ last_event_sequence: 11 }),
      sync
    })

    let settled = false

    const mutation = executor.execute(intent).then(() => {
      settled = true
    })

    await vi.waitFor(() => expect(sync).toHaveBeenCalledWith('project-a', 11))

    expect(settled).toBe(false)
    releaseSync?.()
    await mutation
    expect(settled).toBe(true)
  })

  it('never resubmits a command after its receipt when the follow-up sync is ambiguous', async () => {
    const syncFailure = new Error('request timed out after 30s: project.runtime.snapshot')
    const request = vi.fn(async () => receipt())
    const sync = vi.fn().mockRejectedValueOnce(syncFailure).mockResolvedValue(undefined)
    const executor = createProjectMutationExecutor({ createIdempotencyKey: () => 'intent-a', request, sync })

    await expect(executor.execute(intent)).rejects.toBe(syncFailure)
    expect(request).toHaveBeenCalledTimes(1)
    expect(sync).toHaveBeenCalledTimes(1)
  })

  it('does not execute a stale automatic retry after its requester generation changes', async () => {
    let rejectFirst: ((error: Error) => void) | undefined

    const oldRequest = vi.fn(
      () =>
        new Promise<unknown>((_resolve, reject) => {
          rejectFirst = reject
        })
    )

    const newRequest = vi.fn(async () => receipt())

    const executor = createProjectMutationExecutor({
      createIdempotencyKey: () => 'intent-a',
      request: oldRequest,
      sync: async () => undefined
    })

    const pending = executor.execute(intent)

    await vi.waitFor(() => expect(oldRequest).toHaveBeenCalledTimes(1))
    executor.configure(newRequest, 'profile-new')
    rejectFirst?.(new Error('request timed out after 30s: project.command'))

    await expect(pending).rejects.toThrow('project command requester changed')
    expect(oldRequest).toHaveBeenCalledTimes(1)
    expect(newRequest).not.toHaveBeenCalled()
  })
})
