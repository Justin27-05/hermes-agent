import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'

import { handleProjectRuntimeGatewayEvent } from './gateway-event'

describe('project.event gateway hints', () => {
  beforeEach(() => {
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('uses a valid minimal hint only to wake authoritative snapshot sync', async () => {
    const request = vi.fn(async (method: string) => {
      if (method === 'project.runtime.snapshot') {
        return {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-a',
          block: null,
          canonical_session_id: 'session-a',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 0,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-a',
          queue: [],
          transcript: [],
          transcript_revision: 0,
          version: 1
        }
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 0, project_id: 'project-a' }
      }

      throw new Error(`unexpected ${method}`)
    })

    configureProjectRuntimeRequester(request)

    expect(
      handleProjectRuntimeGatewayEvent({
        payload: { highest_sequence: 99, project_id: 'project-a' },
        type: 'project.event'
      })
    ).toBe(true)

    await vi.waitFor(() =>
      expect(request).toHaveBeenCalledWith('project.runtime.snapshot', { project_id: 'project-a' })
    )
    expect($projectRuntimes.get()['project-a'].snapshot.last_sequence).toBe(0)
    expect(request).not.toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ events: expect.anything() }))
  })

  it('rejects a hint with a canonical body or malformed sequence without mutating runtime state', () => {
    const request = vi.fn()
    configureProjectRuntimeRequester(request)

    expect(
      handleProjectRuntimeGatewayEvent({
        payload: { event: { sequence: 1 }, highest_sequence: 1, project_id: 'project-a' },
        type: 'project.event'
      })
    ).toBe(false)
    expect(
      handleProjectRuntimeGatewayEvent({
        payload: { highest_sequence: -1, project_id: 'project-a' },
        type: 'project.event'
      })
    ).toBe(false)
    expect(request).not.toHaveBeenCalled()
    expect($projectRuntimes.get()).toEqual({})
  })

  it('does not throw when a valid hint arrives before requester wiring', () => {
    expect(() =>
      handleProjectRuntimeGatewayEvent({
        payload: { highest_sequence: 1, project_id: 'project-a' },
        type: 'project.event'
      })
    ).not.toThrow()
    expect($projectRuntimes.get()).toEqual({})
  })

  it('ignores a valid hint from an inactive gateway profile', () => {
    const request = vi.fn()
    configureProjectRuntimeRequester(request, 'active')

    expect(
      handleProjectRuntimeGatewayEvent(
        {
          payload: { highest_sequence: 1, project_id: 'project-a' },
          profile: 'inactive',
          type: 'project.event'
        },
        'active'
      )
    ).toBe(false)
    expect(request).not.toHaveBeenCalled()
  })
})
