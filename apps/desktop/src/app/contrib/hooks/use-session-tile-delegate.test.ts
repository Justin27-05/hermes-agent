import { renderHook } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type * as HermesModule from '@/hermes'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import { setSessions } from '@/store/session'
import { sessionTileDelegate } from '@/store/session-states'
import type { SessionInfo } from '@/types/hermes'

import { useSessionTileDelegate } from './use-session-tile-delegate'

vi.mock('@/hermes', async importActual => ({
  ...(await importActual<typeof HermesModule>()),
  getSessionMessages: vi.fn(async () => ({ messages: [], session_id: '' }))
}))

const { getSessionMessages } = await import('@/hermes')

const row = (over: Partial<SessionInfo>): SessionInfo =>
  ({
    ended_at: null,
    id: 'live',
    input_tokens: 0,
    is_active: false,
    last_active: 0,
    message_count: 1,
    model: null,
    output_tokens: 0,
    preview: null,
    profile: 'default',
    source: null,
    started_at: 0,
    title: null,
    project_id: null,
    ...over
  }) as SessionInfo

function renderTile(requestGateway: ReturnType<typeof vi.fn>) {
  renderHook(() =>
    useSessionTileDelegate({
      archiveSession: vi.fn(async () => undefined),
      branchStoredSession: vi.fn(async () => undefined),
      executeSlashCommand: vi.fn(async () => undefined) as never,
      removeSession: vi.fn(async () => undefined),
      requestGateway: requestGateway as never,
      runtimeIdByStoredSessionIdRef: { current: new Map() },
      sessionStateByRuntimeIdRef: { current: new Map() },
      updateSessionState: vi.fn()
    })
  )
}

describe('useSessionTileDelegate resumeTile', () => {
  beforeEach(() => {
    setSessions([])
    $activeGatewayProfile.set('default')
    $projects.set([])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
    vi.mocked(getSessionMessages).mockClear()
  })

  afterEach(() => {
    setSessions([])
    $projects.set([])
    resetProjectRuntimeStore()
  })

  it('carries the owning profile into a cold tile resume so it cannot fork profiles', async () => {
    // A tile opens a session owned by another profile. Resuming without the
    // profile lets the gateway fall back to the launch-profile DB and clone the
    // conversation into the wrong profile (#67603). The owning profile must ride
    // both the transcript prefetch and the resume RPC.
    const stored = row({ id: 'stored-x', profile: 'ai-engineer' })
    setSessions([stored])
    $activeGatewayProfile.set('ai-engineer')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'ai-engineer'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'ai-engineer' })

    const requestGateway = vi.fn(async (method: string) =>
      method === 'session.resume' ? ({ session_id: 'runtime-1' } as never) : ({} as never)
    )

    renderTile(requestGateway)
    const runtimeId = await sessionTileDelegate()!.resumeTile(stored)

    expect(runtimeId).toBe('runtime-1')
    expect(getSessionMessages).toHaveBeenCalledWith('stored-x', 'ai-engineer')
    expect(requestGateway).toHaveBeenCalledWith('session.resume', {
      session_id: 'stored-x',
      cols: 96,
      profile: 'ai-engineer'
    })
  })

  it('resolves and carries a default-profile session explicitly', async () => {
    const stored = row({ id: 'stored-y', profile: 'default' })
    setSessions([stored])

    const requestGateway = vi.fn(async (method: string) =>
      method === 'session.resume' ? ({ session_id: 'runtime-2' } as never) : ({} as never)
    )

    renderTile(requestGateway)
    await sessionTileDelegate()!.resumeTile(stored)

    expect(requestGateway).toHaveBeenCalledWith('session.resume', {
      session_id: 'stored-y',
      cols: 96,
      profile: 'default'
    })
  })

  it('uses the exact row and profile when two profiles expose the same opaque stored id', async () => {
    const managedA = row({ id: 'same-C', profile: 'default', project_id: 'project-a' })
    const legacyB = row({ id: 'same-C', profile: 'work', project_id: null })
    const archiveSession = vi.fn(async () => undefined)
    const branchStoredSession = vi.fn(async () => undefined)
    const removeSession = vi.fn(async () => undefined)
    const requestGateway = vi.fn(async (method: string) =>
      method === 'session.resume' ? ({ session_id: 'runtime-b' } as never) : ({} as never)
    )

    setSessions([managedA, legacyB])
    $activeGatewayProfile.set('work')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'work'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'work' })

    renderHook(() =>
      useSessionTileDelegate({
        archiveSession,
        branchStoredSession,
        executeSlashCommand: vi.fn(async () => undefined) as never,
        removeSession,
        requestGateway: requestGateway as never,
        runtimeIdByStoredSessionIdRef: { current: new Map() },
        sessionStateByRuntimeIdRef: { current: new Map() },
        updateSessionState: vi.fn()
      })
    )

    const exactDelegate = sessionTileDelegate() as unknown as {
      archiveSession(session: SessionInfo): Promise<void>
      branchSession(session: SessionInfo): Promise<void>
      deleteSession(session: SessionInfo): Promise<void>
      resumeTile(session: SessionInfo): Promise<string>
    }

    await expect(exactDelegate.resumeTile(legacyB)).resolves.toBe('runtime-b')
    await exactDelegate.archiveSession(legacyB)
    await exactDelegate.branchSession(legacyB)
    await exactDelegate.deleteSession(legacyB)

    expect(requestGateway).toHaveBeenCalledWith('session.resume', {
      cols: 96,
      profile: 'work',
      session_id: 'same-C'
    })
    expect(getSessionMessages).toHaveBeenCalledWith('same-C', 'work')
    expect(archiveSession).toHaveBeenCalledWith('same-C')
    expect(branchStoredSession).toHaveBeenCalledWith('same-C', 'work')
    expect(removeSession).toHaveBeenCalledWith('same-C')
  })

  it('hydrates a managed tile from ProjectRuntime without calling session.resume', async () => {
    const stored = row({ id: 'stored-managed', profile: 'default', project_id: 'project-managed' })
    setSessions([stored])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: 'stored-managed',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [{ content: 'Hermes owns this transcript', role: 'assistant' }],
          transcript_revision: 1,
          version: 1
        }
      }
    })
    const requestGateway = vi.fn()
    const updateSessionState = vi.fn()

    renderHook(() =>
      useSessionTileDelegate({
        archiveSession: vi.fn(async () => undefined),
        branchStoredSession: vi.fn(async () => undefined),
        executeSlashCommand: vi.fn(async () => undefined) as never,
        removeSession: vi.fn(async () => undefined),
        requestGateway: requestGateway as never,
        runtimeIdByStoredSessionIdRef: { current: new Map([['stored-managed', 'runtime-managed']]) },
        sessionStateByRuntimeIdRef: {
          current: new Map([['runtime-managed', { storedSessionId: 'stored-managed' } as never]])
        },
        updateSessionState: updateSessionState as never
      })
    )

    await expect(sessionTileDelegate()!.resumeTile(stored)).resolves.toBe('stored-managed')

    expect(requestGateway).not.toHaveBeenCalled()
    expect(getSessionMessages).not.toHaveBeenCalled()
    expect(updateSessionState).toHaveBeenCalledWith('stored-managed', expect.any(Function), 'stored-managed')
  })

  it('fails a catalog-managed tile boot closed while ProjectRuntime is unavailable', async () => {
    const stored = row({ id: 'stored-managed', profile: 'default', project_id: 'project-managed' })
    setSessions([stored])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    const requestGateway = vi.fn()

    renderTile(requestGateway)

    await expect(sessionTileDelegate()!.resumeTile(stored)).rejects.toThrow(
      'The managed project runtime is not available yet.'
    )
    expect(requestGateway).not.toHaveBeenCalled()
    expect(getSessionMessages).not.toHaveBeenCalled()
  })

  it('blocks direct managed tile history mutations before delegating to legacy actions', async () => {
    const stored = row({ id: 'stored-managed', profile: 'default', project_id: 'project-managed' })
    setSessions([stored])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: 'stored-managed',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [],
          transcript_revision: 1,
          version: 1
        }
      }
    })
    const archiveSession = vi.fn(async () => undefined)
    const branchStoredSession = vi.fn(async () => undefined)
    const executeSlashCommand = vi.fn(async () => undefined)
    const removeSession = vi.fn(async () => undefined)
    const requestGateway = vi.fn()

    renderHook(() =>
      useSessionTileDelegate({
        archiveSession,
        branchStoredSession,
        executeSlashCommand: executeSlashCommand as never,
        removeSession,
        requestGateway: requestGateway as never,
        runtimeIdByStoredSessionIdRef: { current: new Map() },
        sessionStateByRuntimeIdRef: { current: new Map() },
        updateSessionState: vi.fn()
      })
    )

    await sessionTileDelegate()!.archiveSession(stored)
    await sessionTileDelegate()!.branchSession(stored)
    await sessionTileDelegate()!.deleteSession(stored)
    await sessionTileDelegate()!.executeSlash('/branch', 'runtime-managed', stored)
    await sessionTileDelegate()!.interruptSession('runtime-managed', stored)
    await sessionTileDelegate()!.submitToSession('runtime-managed', 'do work', stored)

    expect(archiveSession).not.toHaveBeenCalled()
    expect(branchStoredSession).not.toHaveBeenCalled()
    expect(executeSlashCommand).not.toHaveBeenCalled()
    expect(removeSession).not.toHaveBeenCalled()
    expect(requestGateway).not.toHaveBeenCalled()
  })
})
