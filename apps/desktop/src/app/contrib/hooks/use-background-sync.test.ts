import { renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  $projectRuntimes,
  configureProjectRuntimeRequester,
  type ProjectRuntimeRequester,
  type ProjectRuntimeSnapshot,
  resetProjectRuntimeStore
} from '@/store/project-runtime'
import { $projects } from '@/store/projects'
import {
  $attentionSessionIds,
  $stalledSessionIds,
  $workingSessionIds,
  clearAllSessionStates,
  SESSION_WATCHDOG_TIMEOUT_MS
} from '@/store/session-states'

import type { GatewayRequester } from '../types'

const configureProjectCommandRuntime = vi.hoisted(() =>
  vi.fn(
    (_request: unknown, _scope?: string): (() => void) =>
      () =>
        undefined
  )
)

vi.mock('@/store/project-command-runtime', () => ({ configureProjectCommandRuntime }))

import {
  rehydrateLiveSessionStatuses,
  syncManagedProjectRuntimeCatalog,
  syncManagedProjectRuntimes,
  useBackgroundSync
} from './use-background-sync'

function gatewayRequester(request: ProjectRuntimeRequester): GatewayRequester {
  return <T>(method: string, params?: Record<string, unknown>) => request(method, params) as Promise<T>
}

const runtimeSnapshot = (last_sequence: number): ProjectRuntimeSnapshot => ({
  active_run: null,
  artifacts: [],
  binding_id: 'binding-a',
  block: null,
  canonical_session_id: 'session-a',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence,
  lifecycle: 'active',
  pending_approval: null,
  project_id: 'project-a',
  queue: [],
  transcript: [],
  transcript_revision: 0,
  version: 1
})

describe('rehydrateLiveSessionStatuses', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
    clearAllSessionStates()
    resetProjectRuntimeStore()
  })

  it('restores running sessions after reconnect without opening them', () => {
    const now = 1_800_000_000_000

    rehydrateLiveSessionStatuses(
      {
        sessions: [
          {
            id: 'runtime-overnight',
            last_active: (now - SESSION_WATCHDOG_TIMEOUT_MS - 1_000) / 1000,
            session_key: 'overnight-exam-learning',
            status: 'working'
          },
          {
            id: 'runtime-cleanup',
            last_active: now / 1000,
            session_key: 'temporary-file-cleanup',
            status: 'working'
          }
        ]
      },
      now
    )

    expect($workingSessionIds.get()).toEqual(['overnight-exam-learning', 'temporary-file-cleanup'])
    expect($stalledSessionIds.get()).toEqual(['overnight-exam-learning'])
    expect($attentionSessionIds.get()).toEqual([])
  })

  it('restores a waiting turn as working and needing attention', () => {
    rehydrateLiveSessionStatuses({
      sessions: [{ id: 'runtime-needs-user', session_key: 'needs-user', status: 'waiting' }]
    })

    expect($workingSessionIds.get()).toEqual(['needs-user'])
    expect($attentionSessionIds.get()).toEqual(['needs-user'])
    expect($stalledSessionIds.get()).toEqual([])
  })

  it('ignores idle, starting, and malformed live-session rows', () => {
    rehydrateLiveSessionStatuses({
      sessions: [
        { id: 'runtime-idle', session_key: 'idle-session', status: 'idle' },
        { id: 'runtime-starting', session_key: 'starting-session', status: 'starting' },
        { id: 'runtime-malformed', status: 'working' }
      ]
    })

    expect($workingSessionIds.get()).toEqual([])
    expect($attentionSessionIds.get()).toEqual([])
    expect($stalledSessionIds.get()).toEqual([])
  })
})

describe('syncManagedProjectRuntimes', () => {
  afterEach(() => resetProjectRuntimeStore())

  it('recovers an event lost while disconnected through authoritative replay and snapshot', async () => {
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: runtimeSnapshot(1) } })

    const request = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.events') {
        if (params?.after_sequence === 1) {
          return {
            after_sequence: 1,
            events: [
              {
                created_at: '2026-07-30T10:00:00Z',
                event_id: 'event-2',
                kind: 'turn.queued',
                payload: {},
                project_id: 'project-a',
                sequence: 2,
                turn_id: null
              }
            ],
            last_sequence: 2,
            project_id: 'project-a'
          }
        }

        return { after_sequence: 2, events: [], last_sequence: 2, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return runtimeSnapshot(2)
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 2, project_id: 'project-a' }
      }

      throw new Error(`unexpected ${method}`)
    })

    await syncManagedProjectRuntimes(request)

    expect($projectRuntimes.get()['project-a']).toEqual({ events: [], snapshot: runtimeSnapshot(2) })
    expect(request).toHaveBeenCalledWith('project.runtime.ack', {
      binding_id: 'binding-a',
      cursor: 2,
      project_id: 'project-a'
    })
  })

  it('tries a bounded project-list candidate on reconnect without touching legacy state when the runtime RPC is absent', async () => {
    $projects.set([{ id: 'legacy-project' }] as never)

    const request = vi.fn(async () => {
      throw new Error('method not found')
    })

    await syncManagedProjectRuntimes(request, ['legacy-project'])

    expect(request).toHaveBeenCalledWith('project.runtime.snapshot', { project_id: 'legacy-project' })
    expect($projectRuntimes.get()).toEqual({})
    $projects.set([])
  })

  it('discovers a managed runtime from the canonical catalog when the sidebar cache is empty and its hint was lost', async () => {
    $projects.set([])

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return { active_id: null, projects: [{ id: 'project-a', managed: true }] }
      }

      if (method === 'project.runtime.snapshot') {
        return runtimeSnapshot(1)
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 1, project_id: 'project-a' }
      }

      throw new Error(`unexpected ${method}`)
    })

    await syncManagedProjectRuntimeCatalog(gatewayRequester(request), 'default')

    expect(request).toHaveBeenCalledWith('projects.list', {})
    expect(request).toHaveBeenCalledWith('project.runtime.snapshot', { project_id: 'project-a' })
    expect(request).toHaveBeenCalledWith('project.runtime.ack', {
      binding_id: 'binding-a',
      cursor: 1,
      project_id: 'project-a'
    })
  })

  it('never probes runtime RPC for a catalog entry without an exact managed marker', async () => {
    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return {
          active_id: null,
          projects: [
            { id: 'missing-marker' },
            { id: 'legacy-project', managed: false },
            { id: 'malformed-marker', managed: 'true' }
          ]
        }
      }

      throw new Error(`unexpected ${method}`)
    })

    await expect(syncManagedProjectRuntimeCatalog(gatewayRequester(request), 'default')).resolves.toBe(0)

    expect(request).toHaveBeenCalledTimes(1)
    expect(request).toHaveBeenCalledWith('projects.list', {})
    expect($projectRuntimes.get()).toEqual({})
  })

  it('does not apply a catalog response after its profile effect was cancelled', async () => {
    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return { active_id: null, projects: [{ id: 'project-a' }] }
      }

      throw new Error(`unexpected ${method}`)
    })

    await syncManagedProjectRuntimeCatalog(gatewayRequester(request), 'old-profile', 0, () => false)

    expect(request).toHaveBeenCalledWith('projects.list', {})
    expect(request).toHaveBeenCalledTimes(1)
    expect($projectRuntimes.get()).toEqual({})
  })
})

describe('useBackgroundSync managed command runtime wiring', () => {
  beforeEach(() => {
    configureProjectCommandRuntime.mockReset()
  })

  afterEach(() => {
    configureProjectRuntimeRequester(undefined)
    resetProjectRuntimeStore()
  })

  it('configures commands before managed runtime and catalog sync and disposes the exact effect', async () => {
    const order: string[] = []

    const disposeCommands = vi.fn(() => {
      order.push('dispose-commands')
    })

    configureProjectCommandRuntime.mockImplementation((_request, scope) => {
      order.push(`configure-commands:${scope}`)

      return disposeCommands
    })

    const request = vi.fn(async (method: string) => {
      if (method === 'session.active_list') {
        return { sessions: [] }
      }

      if (method === 'project.runtime.events') {
        order.push('sync-runtime')

        return { after_sequence: 1, events: [], last_sequence: 1, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return runtimeSnapshot(1)
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 1, project_id: 'project-a' }
      }

      if (method === 'projects.list') {
        order.push('sync-catalog')

        return { active_id: null, projects: [] }
      }

      throw new Error(`unexpected ${method}`)
    })

    const requestGateway = gatewayRequester(request)

    configureProjectRuntimeRequester(requestGateway, 'profile-a')
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: runtimeSnapshot(1) } })

    const { unmount } = renderHook(() =>
      useBackgroundSync({
        activeGatewayProfile: 'profile-a',
        activeIsMessaging: false,
        activeSessionId: null,
        freshDraftReady: false,
        gatewayState: 'open',
        refreshActiveMessagingTranscript: vi.fn(),
        refreshCronJobs: vi.fn(),
        refreshCurrentModel: vi.fn(),
        refreshHermesConfig: vi.fn(),
        refreshMessagingSessions: vi.fn(),
        refreshSessions: vi.fn(),
        requestGateway
      })
    )

    await vi.waitFor(() => expect(order).toContain('sync-catalog'))

    expect(configureProjectCommandRuntime).toHaveBeenCalledWith(requestGateway, 'profile-a')
    expect(order).toEqual(expect.arrayContaining(['configure-commands:profile-a', 'sync-runtime', 'sync-catalog']))
    expect(order.indexOf('configure-commands:profile-a')).toBeLessThan(order.indexOf('sync-runtime'))
    expect(order.indexOf('configure-commands:profile-a')).toBeLessThan(order.indexOf('sync-catalog'))

    unmount()

    expect(disposeCommands).toHaveBeenCalledTimes(1)
    expect(order.at(-1)).toBe('dispose-commands')
  })
})
