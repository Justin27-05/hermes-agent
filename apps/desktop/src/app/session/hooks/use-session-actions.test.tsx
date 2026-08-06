import { act, cleanup, render, waitFor } from '@testing-library/react'
import type { MutableRefObject } from 'react'
import { useEffect } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { deleteSession, getSession, getSessionMessages, type SessionInfo, setSessionArchived } from '@/hermes'
import { createClientSessionState } from '@/lib/chat-runtime'
import { clearSessionDraft, stashSessionDraft, takeSessionDraft } from '@/store/composer'
import { $activeGatewayProfile, $newChatProfile, ensureGatewayProfile } from '@/store/profile'
import {
  $projectRuntimes,
  configureProjectRuntimeRequester,
  projectRuntimeAuthority,
  resetProjectRuntimeStore
} from '@/store/project-runtime'
import {
  $activeProjectId,
  $projectCatalogAuthority,
  $projects,
  $projectScope,
  $projectTree,
  ALL_PROJECTS
} from '@/store/projects'
import {
  $activeSessionId,
  $activeSessionStoredIdRotation,
  $currentCwd,
  $currentFastMode,
  $currentModel,
  $currentProvider,
  $currentReasoningEffort,
  $messages,
  $newChatWorkspaceTarget,
  $resumeFailedSessionId,
  $selectedStoredSessionId,
  $sessions,
  setActiveSessionId,
  setActiveSessionStoredIdRotation,
  setCurrentCwd,
  setCurrentFastMode,
  setCurrentModel,
  setCurrentProvider,
  setCurrentReasoningEffort,
  setMessages,
  setNewChatWorkspaceTarget,
  setResumeFailedSessionId,
  setSelectedStoredSessionId,
  setSessions
} from '@/store/session'
import { $sessionTiles } from '@/store/session-states'

import { sessionRoute } from '../../routes'
import type { ClientSessionState } from '../../types'

import { useSessionActions } from './use-session-actions'

vi.mock('@/hermes', async importOriginal => ({
  ...(await importOriginal<Record<string, unknown>>()),
  deleteSession: vi.fn(),
  getSession: vi.fn(),
  getSessionMessages: vi.fn(),
  listAllProfileSessions: vi.fn(),
  setApiRequestProfile: vi.fn(),
  setSessionArchived: vi.fn()
}))

vi.mock('@/store/profile', async importOriginal => ({
  ...(await importOriginal<Record<string, unknown>>()),
  ensureGatewayProfile: vi.fn().mockResolvedValue(undefined)
}))

const RUNTIME_SESSION_ID = 'rt-new-001'

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void

  const promise = new Promise<T>(done => {
    resolve = done
  })

  return { promise, resolve }
}

type HarnessHandle = Pick<
  ReturnType<typeof useSessionActions>,
  'createBackendSessionForSend' | 'startFreshSessionDraft'
>

function storedSession(overrides: Partial<SessionInfo> = {}): SessionInfo {
  return {
    ended_at: null,
    id: 'stored-1',
    input_tokens: 0,
    is_active: false,
    last_active: 1,
    message_count: 0,
    model: null,
    output_tokens: 0,
    preview: null,
    project_id: null,
    source: 'desktop',
    started_at: 1,
    title: 'stored',
    tool_call_count: 0,
    ...overrides
  }
}

function Harness({
  navigate = vi.fn(),
  onReady,
  requestGateway
}: {
  navigate?: ReturnType<typeof vi.fn>
  onReady: (handle: HarnessHandle) => void
  requestGateway: <T>(method: string, params?: Record<string, unknown>) => Promise<T>
}) {
  const ref = <T,>(value: T): MutableRefObject<T> => ({ current: value })

  const actions = useSessionActions({
    activeSessionId: null,
    activeSessionIdRef: ref<string | null>(null),
    busyRef: ref(false),
    creatingSessionRef: ref(false),
    ensureSessionState: () => ({}) as ClientSessionState,
    getRouteToken: () => 'token',
    getRoutedStoredSessionId: () => null,
    navigate: navigate as never,
    requestGateway,
    resetViewSync: vi.fn(),
    runtimeIdByStoredSessionIdRef: ref(new Map<string, string>()),
    selectedStoredSessionId: null,
    selectedStoredSessionIdRef: ref<string | null>(null),
    sessionStateByRuntimeIdRef: ref(new Map<string, ClientSessionState>()),
    syncSessionStateToView: vi.fn(),
    updateSessionState: () => ({}) as ClientSessionState
  })

  useEffect(() => {
    onReady(actions)
  }, [actions, onReady])

  return null
}

function StoredIdRotationHarness({
  activeSessionIdRef,
  getRoutedStoredSessionId,
  navigate,
  selectedStoredSessionIdRef
}: {
  activeSessionIdRef: MutableRefObject<string | null>
  getRoutedStoredSessionId: () => null | string
  navigate: (to: string, options?: { replace?: boolean }) => void
  selectedStoredSessionIdRef: MutableRefObject<string | null>
}) {
  const ref = <T,>(value: T): MutableRefObject<T> => ({ current: value })

  useSessionActions({
    activeSessionId: activeSessionIdRef.current,
    activeSessionIdRef,
    busyRef: ref(false),
    creatingSessionRef: ref(false),
    ensureSessionState: () => ({}) as ClientSessionState,
    getRouteToken: () => 'token',
    getRoutedStoredSessionId,
    navigate: navigate as never,
    requestGateway: async () => ({}) as never,
    resetViewSync: vi.fn(),
    runtimeIdByStoredSessionIdRef: ref(new Map<string, string>()),
    selectedStoredSessionId: selectedStoredSessionIdRef.current,
    selectedStoredSessionIdRef,
    sessionStateByRuntimeIdRef: ref(new Map<string, ClientSessionState>()),
    syncSessionStateToView: vi.fn(),
    updateSessionState: () => ({}) as ClientSessionState
  })

  return null
}

describe('active stored-session id rotation routing', () => {
  afterEach(() => {
    cleanup()
    setActiveSessionId(null)
    setActiveSessionStoredIdRotation(null)
    setSelectedStoredSessionId(null)
    vi.restoreAllMocks()
  })

  it('follows a rotation while the same conversation still owns the foreground route', async () => {
    const activeSessionIdRef: MutableRefObject<string | null> = { current: 'runtime-A' }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: 'stored-A' }
    const navigate = vi.fn()

    setSelectedStoredSessionId('stored-A')
    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => 'stored-A'}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: 'stored-A-next',
        previousStoredSessionId: 'stored-A',
        runtimeSessionId: 'runtime-A'
      })
    })

    await waitFor(() => expect(selectedStoredSessionIdRef.current).toBe('stored-A-next'))
    expect($selectedStoredSessionId.get()).toBe('stored-A-next')
    expect(navigate).toHaveBeenCalledWith(sessionRoute('stored-A-next'), { replace: true })
    expect($activeSessionStoredIdRotation.get()).toBeNull()
  })

  it('keeps draft on the previous tip when the new tip row is not loaded yet', async () => {
    const tipBefore = 'tip-root'
    const tipAfter = 'tip-new-unloaded'
    const runtimeSessionId = 'runtime-gap'
    const activeSessionIdRef: MutableRefObject<string | null> = { current: runtimeSessionId }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: tipBefore }
    const navigate = vi.fn()

    setSessions([])
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    stashSessionDraft(tipBefore, 'typed during gap', [])
    setSelectedStoredSessionId(tipBefore)
    setActiveSessionId(runtimeSessionId)

    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => tipBefore}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: tipAfter,
        previousStoredSessionId: tipBefore,
        runtimeSessionId
      })
    })

    await waitFor(() => expect($selectedStoredSessionId.get()).toBe(tipAfter))
    expect(takeSessionDraft(tipBefore).text).toBe('typed during gap')
    expect(takeSessionDraft(tipAfter).text).toBe('')

    clearSessionDraft(tipBefore)
    clearSessionDraft(tipAfter)
    setActiveSessionId(null)
  })

  it('parks an in-progress composer draft on the lineage root across tip rotation', async () => {
    // Desktop draft must stay on the durable composer key (lineage root), not
    // move onto the fresh tip — ChatBar scopes drafts via resolveComposerSessionKey.
    const tipBefore = '20260720_062637_ad96b3'
    const tipAfter = '20260720_071049_a28905'
    const runtimeSessionId = 'runtime-desktop-thinking'
    const activeSessionIdRef: MutableRefObject<string | null> = { current: runtimeSessionId }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: tipBefore }
    const navigate = vi.fn()
    const typedWhileThinking = 'follow up I am still typing during thinking'

    setSessions([storedSession({ id: tipAfter, message_count: 2, _lineage_root_id: tipBefore })])
    stashSessionDraft(tipBefore, typedWhileThinking, [])
    setSelectedStoredSessionId(tipBefore)
    setActiveSessionId(runtimeSessionId)

    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => tipBefore}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: tipAfter,
        previousStoredSessionId: tipBefore,
        runtimeSessionId
      })
    })

    await waitFor(() => expect($selectedStoredSessionId.get()).toBe(tipAfter))
    // Durable key remains the lineage root — same scope ChatBar will keep using.
    expect(takeSessionDraft(tipBefore).text).toBe(typedWhileThinking)
    expect(takeSessionDraft(tipAfter).text).toBe('')

    clearSessionDraft(tipBefore)
    clearSessionDraft(tipAfter)
    setActiveSessionId(null)
    setSessions([])
  })

  it('does not overwrite a newer route intent before its resume effect has synchronized selection', async () => {
    const activeSessionIdRef: MutableRefObject<string | null> = { current: 'runtime-A' }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: 'stored-A' }
    const navigate = vi.fn()

    setSelectedStoredSessionId('stored-A')
    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => 'stored-C'}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: 'stored-A-next',
        previousStoredSessionId: 'stored-A',
        runtimeSessionId: 'runtime-A'
      })
    })

    await waitFor(() => expect($activeSessionStoredIdRotation.get()).toBeNull())
    expect(selectedStoredSessionIdRef.current).toBe('stored-A')
    expect($selectedStoredSessionId.get()).toBe('stored-A')
    expect(navigate).not.toHaveBeenCalled()
  })

  it('does not let the previous runtime jump back after selection already moved', async () => {
    const activeSessionIdRef: MutableRefObject<string | null> = { current: 'runtime-A' }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: 'stored-C' }
    const navigate = vi.fn()

    setSelectedStoredSessionId('stored-C')
    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => 'stored-C'}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: 'stored-A-next',
        previousStoredSessionId: 'stored-A',
        runtimeSessionId: 'runtime-A'
      })
    })

    await waitFor(() => expect($activeSessionStoredIdRotation.get()).toBeNull())
    expect(selectedStoredSessionIdRef.current).toBe('stored-C')
    expect($selectedStoredSessionId.get()).toBe('stored-C')
    expect(navigate).not.toHaveBeenCalled()
  })

  it('updates the underlying selection without navigating out of an overlay or page', async () => {
    const activeSessionIdRef: MutableRefObject<string | null> = { current: 'runtime-A' }
    const selectedStoredSessionIdRef: MutableRefObject<string | null> = { current: 'stored-A' }
    const navigate = vi.fn()

    setSelectedStoredSessionId('stored-A')
    render(
      <StoredIdRotationHarness
        activeSessionIdRef={activeSessionIdRef}
        getRoutedStoredSessionId={() => null}
        navigate={navigate}
        selectedStoredSessionIdRef={selectedStoredSessionIdRef}
      />
    )

    act(() => {
      setActiveSessionStoredIdRotation({
        nextStoredSessionId: 'stored-A-next',
        previousStoredSessionId: 'stored-A',
        runtimeSessionId: 'runtime-A'
      })
    })

    await waitFor(() => expect(selectedStoredSessionIdRef.current).toBe('stored-A-next'))
    expect($selectedStoredSessionId.get()).toBe('stored-A-next')
    expect(navigate).not.toHaveBeenCalled()
  })
})

async function createWith(
  profileSetup: () => void,
  beforeCreate?: (handle: HarnessHandle) => Promise<void> | void
): Promise<Record<string, unknown> | undefined> {
  let createParams: Record<string, unknown> | undefined

  const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
    if (method === 'session.create') {
      createParams = params

      return { session_id: RUNTIME_SESSION_ID, stored_session_id: null } as never
    }

    return {} as never
  })

  setCurrentCwd('')
  setNewChatWorkspaceTarget(undefined)
  profileSetup()

  let handle: HarnessHandle | null = null
  render(<Harness onReady={h => (handle = h)} requestGateway={requestGateway} />)
  await waitFor(() => expect(handle).not.toBeNull())

  if (beforeCreate) {
    await act(async () => {
      await beforeCreate(handle!)
    })
  }

  await act(async () => {
    await handle!.createBackendSessionForSend()
  })

  return createParams
}

describe('startFreshSessionDraft', () => {
  afterEach(() => cleanup())

  it('can reset machine-bound session state without closing the current overlay route', async () => {
    const navigate = vi.fn()
    const requestGateway = vi.fn(async () => ({}) as never)
    let handle: HarnessHandle | null = null

    render(<Harness navigate={navigate} onReady={value => (handle = value)} requestGateway={requestGateway} />)
    await waitFor(() => expect(handle).not.toBeNull())

    act(() => handle!.startFreshSessionDraft({ preserveRoute: true, workspaceTarget: null }))

    expect(navigate).not.toHaveBeenCalled()
    expect($currentCwd.get()).toBe('')
    expect($newChatWorkspaceTarget.get()).toBeNull()
  })
})

describe('createBackendSessionForSend profile routing', () => {
  afterEach(() => {
    cleanup()
    $newChatProfile.set(null)
    $activeGatewayProfile.set('default')
    $projectScope.set(ALL_PROJECTS)
    $projectTree.set([])
    $currentCwd.set('')
    $currentFastMode.set(false)
    $currentModel.set('')
    $currentProvider.set('')
    $currentReasoningEffort.set('')
    setNewChatWorkspaceTarget(undefined)
    vi.restoreAllMocks()
  })

  it('routes a plain new chat (no explicit profile) to the live gateway profile', async () => {
    // The "rubberband to default" bug: the top New Session button clears
    // $newChatProfile to null. In global-remote mode one backend serves every
    // profile, so an omitted `profile` lands the chat on the launch (default)
    // profile. The session must instead carry the active gateway profile.
    const params = await createWith(() => {
      $activeGatewayProfile.set('coder')
      $newChatProfile.set(null)
    })

    expect(params).toMatchObject({ profile: 'coder' })
  })

  it('honours an explicit per-profile "+" selection', async () => {
    const params = await createWith(() => {
      $activeGatewayProfile.set('coder')
      $newChatProfile.set('analyst')
    })

    expect(params).toMatchObject({ profile: 'analyst' })
  })

  it('passes the default profile for single-profile users (backend resolves it to launch)', async () => {
    const params = await createWith(() => {
      $activeGatewayProfile.set('default')
      $newChatProfile.set(null)
    })

    expect(params).toMatchObject({ profile: 'default' })
  })

  it('tags new desktop chats as desktop sessions', async () => {
    const params = await createWith(() => {})

    expect(params).toMatchObject({ source: 'desktop' })
  })

  it('passes the current workspace cwd into session.create', async () => {
    const params = await createWith(() => {
      $currentCwd.set('/remote/worktree')
    })

    expect(params).toMatchObject({ cwd: '/remote/worktree' })
  })

  it('freezes the visible selector state before profile readiness and sends fast: false explicitly', async () => {
    const profileReady = deferred<void>()
    vi.mocked(ensureGatewayProfile).mockReturnValueOnce(profileReady.promise)

    setCurrentModel('anthropic/claude-sonnet-4.6')
    setCurrentProvider('anthropic')
    setCurrentReasoningEffort('high')
    setCurrentFastMode(false)

    let createParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.create') {
        createParams = params

        return { session_id: RUNTIME_SESSION_ID, stored_session_id: null } as never
      }

      return {} as never
    })

    let handle: HarnessHandle | null = null
    render(<Harness onReady={next => (handle = next)} requestGateway={requestGateway} />)
    await waitFor(() => expect(handle).not.toBeNull())

    let createPromise!: Promise<null | string>
    act(() => {
      createPromise = handle!.createBackendSessionForSend()
    })
    await waitFor(() => expect(ensureGatewayProfile).toHaveBeenCalled())

    // A background refresh or a second click can mutate the sticky atoms while
    // the profile is waking. This send must still use what was visible at Enter.
    setCurrentModel('openai/gpt-5.5')
    setCurrentProvider('openai-codex')
    setCurrentReasoningEffort('low')
    setCurrentFastMode(true)
    profileReady.resolve()

    await act(async () => {
      await createPromise
    })

    expect(createParams).toMatchObject({
      fast: false,
      model: 'anthropic/claude-sonnet-4.6',
      provider: 'anthropic',
      reasoning_effort: 'high'
    })
  })

  it('revalidates frozen draftauthority directly before session.create after profile readiness awaits', async () => {
    const profileReady = deferred<void>()
    vi.mocked(ensureGatewayProfile).mockReturnValueOnce(profileReady.promise)

    const requestGateway = vi.fn(async () => ({}) as never)
    const validateDraftAuthority = vi.fn(() => true)
    let handle: HarnessHandle | null = null

    render(<Harness onReady={next => (handle = next)} requestGateway={requestGateway} />)
    await waitFor(() => expect(handle).not.toBeNull())

    let createPromise!: Promise<null | string>
    act(() => {
      createPromise = handle!.createBackendSessionForSend(null, validateDraftAuthority)
    })
    await waitFor(() => expect(ensureGatewayProfile).toHaveBeenCalled())

    validateDraftAuthority.mockReturnValue(false)
    profileReady.resolve()

    let result: null | string = 'unexpected'
    await act(async () => {
      result = await createPromise
    })

    expect(result).toBeNull()
    expect(validateDraftAuthority).toHaveBeenCalled()
    expect(requestGateway).not.toHaveBeenCalledWith('session.create', expect.anything())
  })

  it('falls back to the entered project cwd when the current cwd is blank', async () => {
    const params = await createWith(() => {
      $projectTree.set([
        {
          id: 'p_app',
          label: 'App',
          path: '/repo/app',
          repos: [{ groups: [], id: '/repo/app', label: 'app', path: '/repo/app', sessionCount: 0 }],
          sessionCount: 0
        }
      ])
      $projectScope.set('p_app')
      $currentCwd.set('')
    })

    expect(params).toMatchObject({ cwd: '/repo/app' })
  })
})

// ── Resume failure recovery (the "stuck loading session window" bug) ──────────
// When session.resume rejects AND the REST transcript fallback ALSO fails, the
// hook must (a) not throw out of the fallback (which stranded the loader), and
// (b) arm $resumeFailedSessionId so use-route-resume can retry. A resume that
// succeeds must NOT leave the flag armed.
function ResumeHarness({
  onStateUpdate,
  onReady,
  requestGateway,
  runtimeIdByStoredSessionIdRef,
  selectedStoredSessionId = null,
  sessionStateByRuntimeIdRef
}: {
  onStateUpdate?: (sessionId: string, state: ClientSessionState) => void
  onReady: (resume: (storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) => void
  requestGateway: <T>(method: string, params?: Record<string, unknown>) => Promise<T>
  runtimeIdByStoredSessionIdRef?: MutableRefObject<Map<string, string>>
  selectedStoredSessionId?: string | null
  sessionStateByRuntimeIdRef?: MutableRefObject<Map<string, ClientSessionState>>
}) {
  const ref = <T,>(value: T): MutableRefObject<T> => ({ current: value })

  const actions = useSessionActions({
    activeSessionId: null,
    activeSessionIdRef: ref<string | null>(null),
    busyRef: ref(false),
    creatingSessionRef: ref(false),
    ensureSessionState: () => ({}) as ClientSessionState,
    getRouteToken: () => 'token',
    getRoutedStoredSessionId: () => null,
    navigate: vi.fn() as never,
    requestGateway,
    resetViewSync: vi.fn(),
    runtimeIdByStoredSessionIdRef: runtimeIdByStoredSessionIdRef ?? ref(new Map<string, string>()),
    selectedStoredSessionId,
    selectedStoredSessionIdRef: ref<string | null>(selectedStoredSessionId),
    sessionStateByRuntimeIdRef: sessionStateByRuntimeIdRef ?? ref(new Map<string, ClientSessionState>()),
    syncSessionStateToView: vi.fn(),
    updateSessionState: (sessionId, updater) => {
      const next = updater({} as ClientSessionState)
      onStateUpdate?.(sessionId, next)

      return next
    }
  })

  useEffect(() => {
    onReady(actions.resumeSession)
  }, [actions.resumeSession, onReady])

  return null
}

describe('resumeSession failure recovery', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(ensureGatewayProfile).mockResolvedValue(undefined)
    $activeGatewayProfile.set('default')
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
    setSessions([storedSession({ id: 'stored-1', profile: 'default', project_id: null })])
  })

  afterEach(() => {
    cleanup()
    setActiveSessionId(null)
    setResumeFailedSessionId(null)
    setMessages([])
    setSessions([])
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    vi.restoreAllMocks()
  })

  async function runResume(
    requestGateway: <T>(method: string, params?: Record<string, unknown>) => Promise<T>,
    options: {
      preserveAuthority?: boolean
      runtimeIdByStoredSessionIdRef?: MutableRefObject<Map<string, string>>
      sessionStateByRuntimeIdRef?: MutableRefObject<Map<string, ClientSessionState>>
    } = {}
  ): Promise<void> {
    const existing = $sessions.get().find(session => session.id === 'stored-1')
    const profile = existing?.profile || 'default'

    if (!existing) {
      setSessions([storedSession({ id: 'stored-1', profile, project_id: null })])
    }

    if (!options.preserveAuthority) {
      $activeGatewayProfile.set(profile)
      $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile })

      if (Object.keys($projectRuntimes.get()).length === 0 && projectRuntimeAuthority().scope !== profile) {
        configureProjectRuntimeRequester(
          vi.fn(async () => undefined),
          profile
        )
      } else if (Object.keys($projectRuntimes.get()).length > 0 && projectRuntimeAuthority().scope === null) {
        const runtimes = $projectRuntimes.get()
        configureProjectRuntimeRequester(
          vi.fn(async () => undefined),
          profile
        )
        $projectRuntimes.set(runtimes)
      }
    }

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    const { preserveAuthority: _preserveAuthority, ...harnessOptions } = options

    render(<ResumeHarness onReady={r => (resume = r)} requestGateway={requestGateway} {...harnessOptions} />)
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-1', true)
  }

  it('hydrates a managed primary route from ProjectRuntime without session.resume', async () => {
    setSessions([storedSession({ id: 'stored-1', project_id: 'project-managed' })])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: 'stored-1',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [{ content: 'canonical primary transcript', role: 'assistant' }],
          transcript_revision: 1,
          version: 1
        }
      }
    })
    const requestGateway = vi.fn()

    await runResume(requestGateway as never, {
      sessionStateByRuntimeIdRef: { current: new Map() }
    })

    expect(requestGateway).not.toHaveBeenCalled()
    expect(getSessionMessages).not.toHaveBeenCalled()
    expect($activeSessionId.get()).toBe('stored-1')
  })

  it('fails a catalog-managed primary boot closed while ProjectRuntime is unavailable', async () => {
    setSessions([storedSession({ id: 'stored-1', project_id: 'project-managed' })])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    const requestGateway = vi.fn()

    await runResume(requestGateway as never)

    expect(requestGateway.mock.calls.some(([method]) => method === 'session.resume')).toBe(false)
    expect(getSessionMessages).not.toHaveBeenCalled()
    expect($activeSessionId.get()).toBeNull()
  })

  it('switches to the exact owning profile before resuming an explicitly legacy session', async () => {
    setSessions([storedSession({ id: 'stored-1', profile: 'work', project_id: null })])
    const order: string[] = []

    vi.mocked(ensureGatewayProfile).mockImplementation(async profile => {
      order.push(`profile:${profile}`)
      $activeGatewayProfile.set(profile || 'default')
    })

    const requestGateway = vi.fn(async (method: string) => {
      order.push(method)

      return method === 'session.resume'
        ? ({ info: {}, messages: [], session_id: 'runtime-work' } as never)
        : ({} as never)
    })

    await runResume(requestGateway, { preserveAuthority: true })

    expect(order.indexOf('profile:work')).toBeLessThan(order.indexOf('session.resume'))
    expect(requestGateway).toHaveBeenCalledWith(
      'session.resume',
      expect.objectContaining({ profile: 'work', session_id: 'stored-1' })
    )
  })

  it('uses only profile-B authority when A and B reuse ids for a managed session', async () => {
    setSessions([storedSession({ id: 'stored-1', profile: 'work', project_id: 'shared-project' })])
    $projects.set([{ id: 'shared-project', managed: false } as never])
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })

    const profileBSnapshot = {
      active_run: null,
      artifacts: [],
      binding_id: 'binding-work',
      block: null,
      canonical_session_id: 'stored-1',
      current_phase: 'implementation',
      delivery_status: { error_code: null, state: 'caught_up' as const },
      last_sequence: 1,
      lifecycle: 'active' as const,
      pending_approval: null,
      project_id: 'shared-project',
      queue: [],
      transcript: [{ content: 'profile B canonical history', role: 'assistant' as const }],
      transcript_revision: 1,
      version: 1
    }

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'project.runtime.snapshot') {
        return profileBSnapshot as never
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-work', cursor: 1, project_id: 'shared-project' } as never
      }

      return {} as never
    })

    vi.mocked(ensureGatewayProfile).mockImplementation(async profile => {
      expect(profile).toBe('work')
      $activeGatewayProfile.set('work')
      $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'work' })
      $projects.set([{ id: 'shared-project', managed: true } as never])
    })

    await runResume(requestGateway as never, { preserveAuthority: true })

    expect(requestGateway.mock.calls.some(([method]) => method === 'session.resume')).toBe(false)
    expect(requestGateway.mock.calls.map(([method]) => method)).toEqual([
      'project.runtime.snapshot',
      'project.runtime.ack'
    ])
    expect(getSessionMessages).not.toHaveBeenCalled()
    expect($activeSessionId.get()).toBe('stored-1')
  })

  it('arms $resumeFailedSessionId when resume RPC and REST fallback both fail', async () => {
    // session.resume rejects (e.g. timeout against a wedged backend)...
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        throw new Error('request timed out: session.resume')
      }

      return {} as never
    })

    // ...and the REST transcript fallback also rejects (backend unreachable).
    vi.mocked(getSessionMessages).mockRejectedValue(new Error('network down'))

    await runResume(requestGateway)

    // The window is no longer silently stranded: the failure latch is armed for
    // the stored session, which use-route-resume consumes to retry.
    expect($resumeFailedSessionId.get()).toBe('stored-1')
  })

  it('does NOT arm the failure latch when the resume RPC fails but the REST fallback paints history', async () => {
    // session.resume rejects, but the REST transcript fallback succeeds and
    // hydrates a readable transcript — the window is NOT stranded.
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        throw new Error('request timed out: session.resume')
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [
        { content: 'hello', role: 'user', timestamp: 1 },
        { content: 'hi there', role: 'assistant', timestamp: 2 }
      ],
      session_id: 'stored-1'
    } as never)

    await runResume(requestGateway)

    // Arming here would auto-retry a window that already shows history and,
    // on exhaustion, blank that transcript behind the error overlay — a
    // regression vs. plain fallback-success. The latch must stay clear.
    expect($resumeFailedSessionId.get()).toBeNull()
    // The fallback transcript is visible.
    expect($messages.get().length).toBeGreaterThan(0)
  })

  it('preserves an optimistic user message during a same-session reconnect', async () => {
    setMessages([
      {
        id: 'stored-user',
        role: 'user',
        parts: [{ type: 'text', text: 'earlier question' }]
      },
      {
        id: 'stored-assistant',
        role: 'assistant',
        parts: [{ type: 'text', text: 'earlier answer' }]
      },
      {
        id: 'user-optimistic',
        role: 'user',
        parts: [{ type: 'text', text: 'message sent during reconnect' }]
      }
    ])

    const storedMessages = [
      { content: 'earlier question', role: 'user', timestamp: 1 },
      { content: 'earlier answer', role: 'assistant', timestamp: 2 }
    ]

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: storedMessages, session_id: 'stored-1' } as never)

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        return {
          session_id: 'runtime-1',
          session_key: 'stored-1',
          resumed: 'stored-1',
          message_count: 2,
          messages: storedMessages,
          info: {}
        } as never
      }

      return {} as never
    })

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness onReady={r => (resume = r)} requestGateway={requestGateway} selectedStoredSessionId="stored-1" />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-1', true)

    expect($messages.get().map(message => message.id)).toContain('user-optimistic')
  })

  it('restores the in-flight turn and queued user prompt after a full renderer restart', async () => {
    const storedMessages = [
      { content: 'earlier question', role: 'user', timestamp: 1 },
      { content: 'earlier answer', role: 'assistant', timestamp: 2 }
    ]

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: storedMessages, session_id: 'stored-1' } as never)

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        return {
          session_id: 'runtime-1',
          session_key: 'stored-1',
          resumed: 'stored-1',
          message_count: storedMessages.length,
          messages: storedMessages,
          running: true,
          inflight: {
            user: 'current prompt',
            assistant: 'partial answer',
            streaming: true
          },
          queued: { user: 'newest prompt' },
          info: {}
        } as never
      }

      return {} as never
    })

    let resumedState: ClientSessionState | undefined
    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={(_sessionId, state) => (resumedState = state)}
        requestGateway={requestGateway}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-1', true)

    const renderedMessages = JSON.stringify(resumedState?.messages)
    expect(renderedMessages).toContain('current prompt')
    expect(renderedMessages).toContain('partial answer')
    expect(renderedMessages).toContain('newest prompt')
  })

  it('uses the continuation projection when resume rotates an equal-length stored transcript', async () => {
    const parentMessages = [
      { content: 'question before compression', role: 'user', timestamp: 1 },
      { content: 'answer before compression', role: 'assistant', timestamp: 2 }
    ]

    const continuationMessages = [
      { content: 'prompt after compression', role: 'user', timestamp: 3 },
      { content: 'answer after compression', role: 'assistant', timestamp: 4 }
    ]

    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: parentMessages,
      session_id: 'stored-1'
    } as never)

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        return {
          session_id: 'runtime-continuation',
          session_key: 'stored-continuation',
          resumed: 'stored-continuation',
          message_count: continuationMessages.length,
          messages: continuationMessages,
          info: {}
        } as never
      }

      return {} as never
    })

    let resumedState: ClientSessionState | undefined
    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null

    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={(_sessionId, state) => (resumedState = state)}
        requestGateway={requestGateway}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-1', true)

    const renderedMessages = JSON.stringify(resumedState?.messages)
    expect(renderedMessages).toContain('prompt after compression')
    expect(renderedMessages).toContain('answer after compression')
    expect(renderedMessages).not.toContain('answer before compression')
  })

  it('does NOT throw out of the fallback when REST also fails (no unhandled rejection)', async () => {
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        throw new Error('request timed out: session.resume')
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockRejectedValue(new Error('network down'))

    // resumeSession must resolve (swallow the fallback failure), not reject.
    await expect(runResume(requestGateway)).resolves.toBeUndefined()
  })

  it('leaves the failure latch clear when resume succeeds', async () => {
    // Pre-arm to prove a successful resume clears it (entry-clear path).
    setResumeFailedSessionId('stored-1')

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.resume') {
        return { session_id: 'runtime-1', resumed: params?.session_id, messages: [], info: {} } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [] } as never)

    await runResume(requestGateway)

    expect($resumeFailedSessionId.get()).toBeNull()
  })

  it('resumes via the gateway default (deferred build) — not lazy, no eager opt-out', async () => {
    // The switch-latency fix lives backend-side: a normal cold resume gets the
    // gateway's default DEFERRED build (transcript returns immediately, agent
    // pre-warms in the background). The client must NOT force the synchronous
    // path (eager_build) and is only `lazy` for subagent watch windows.
    let resumeParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.resume') {
        resumeParams = params

        return { session_id: 'runtime-1', resumed: params?.session_id, messages: [], info: {} } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [] } as never)

    await runResume(requestGateway)

    expect(resumeParams).not.toHaveProperty('lazy')
    expect(resumeParams).not.toHaveProperty('eager_build')
    expect(resumeParams).toMatchObject({ source: 'desktop' })
  })

  it('arms the failure latch when resume succeeds with an empty transcript for a non-empty stored session', async () => {
    setSessions([storedSession({ message_count: 4 })])

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.resume') {
        return { session_id: 'runtime-1', resumed: params?.session_id, messages: [], info: {} } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [], session_id: 'stored-1' } as never)

    await runResume(requestGateway)

    expect($resumeFailedSessionId.get()).toBe('stored-1')
    expect($activeSessionId.get()).toBeNull()
    expect($messages.get()).toEqual([])
  })

  it('does not reuse an empty cached runtime view for a stored session with history', async () => {
    const runtimeIdByStoredSessionIdRef = {
      current: new Map([['stored-1', 'runtime-stale']])
    } satisfies MutableRefObject<Map<string, string>>

    const sessionStateByRuntimeIdRef = {
      current: new Map([
        [
          'runtime-stale',
          {
            awaitingResponse: false,
            branch: '',
            busy: false,
            cwd: '',
            fast: false,
            interimBoundaryPending: false,
            interrupted: false,
            messages: [],
            model: '',
            needsInput: false,
            pendingBranchGroup: null,
            personality: '',
            provider: '',
            reasoningEffort: '',
            sawAssistantPayload: false,
            serviceTier: '',
            storedSessionId: 'stored-1',
            streamId: null,
            turnStartedAt: null,
            usage: null,
            yolo: false
          }
        ]
      ])
    } satisfies MutableRefObject<Map<string, ClientSessionState>>

    setSessions([storedSession({ message_count: 4 })])

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.resume') {
        return { session_id: 'runtime-1', resumed: params?.session_id, messages: [], info: {} } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'existing text', role: 'user', timestamp: 1 }],
      session_id: 'stored-1'
    } as never)

    await runResume(requestGateway, {
      runtimeIdByStoredSessionIdRef,
      sessionStateByRuntimeIdRef
    })

    expect(requestGateway).not.toHaveBeenCalledWith('session.usage', { session_id: 'runtime-stale' })
    expect(runtimeIdByStoredSessionIdRef.current.has('stored-1')).toBe(false)
    expect(sessionStateByRuntimeIdRef.current.has('runtime-stale')).toBe(false)
    expect($activeSessionId.get()).toBe('runtime-1')
    expect($messages.get().length).toBe(1)
  })
})

function BranchHarness({
  navigate = vi.fn(),
  onReady,
  requestGateway
}: {
  navigate?: ReturnType<typeof vi.fn>
  onReady: (branchStoredSession: (storedSessionId: string, sessionProfile?: string | null) => Promise<boolean>) => void
  requestGateway: <T>(method: string, params?: Record<string, unknown>) => Promise<T>
}) {
  const ref = <T,>(value: T): MutableRefObject<T> => ({ current: value })

  const actions = useSessionActions({
    activeSessionId: null,
    activeSessionIdRef: ref<string | null>(null),
    busyRef: ref(false),
    creatingSessionRef: ref(false),
    ensureSessionState: () => ({}) as ClientSessionState,
    getRouteToken: () => 'token',
    getRoutedStoredSessionId: () => null,
    navigate: navigate as never,
    requestGateway,
    resetViewSync: vi.fn(),
    runtimeIdByStoredSessionIdRef: ref(new Map<string, string>()),
    selectedStoredSessionId: null,
    selectedStoredSessionIdRef: ref<string | null>(null),
    sessionStateByRuntimeIdRef: ref(new Map<string, ClientSessionState>()),
    syncSessionStateToView: vi.fn(),
    updateSessionState: () => ({}) as ClientSessionState
  })

  useEffect(() => {
    onReady(actions.branchStoredSession)
  }, [actions.branchStoredSession, onReady])

  return null
}

type StoredSessionMutationActions = Pick<
  ReturnType<typeof useSessionActions>,
  'archiveSession' | 'branchCurrentSession' | 'branchStoredSession' | 'removeSession'
>

function StoredSessionMutationHarness({
  activeSessionId = null,
  onReady,
  requestGateway,
  runtimeIdByStoredSessionIdRef = { current: new Map() },
  selectedStoredSessionId = null,
  sessionStateByRuntimeIdRef = { current: new Map() }
}: {
  activeSessionId?: null | string
  onReady: (actions: StoredSessionMutationActions) => void
  requestGateway: <T>(method: string, params?: Record<string, unknown>) => Promise<T>
  runtimeIdByStoredSessionIdRef?: MutableRefObject<Map<string, string>>
  selectedStoredSessionId?: null | string
  sessionStateByRuntimeIdRef?: MutableRefObject<Map<string, ClientSessionState>>
}) {
  const ref = <T,>(value: T): MutableRefObject<T> => ({ current: value })

  const actions = useSessionActions({
    activeSessionId,
    activeSessionIdRef: ref(activeSessionId),
    busyRef: ref(false),
    creatingSessionRef: ref(false),
    ensureSessionState: () => ({}) as ClientSessionState,
    getRouteToken: () => 'token',
    getRoutedStoredSessionId: () => null,
    navigate: vi.fn() as never,
    requestGateway,
    resetViewSync: vi.fn(),
    runtimeIdByStoredSessionIdRef,
    selectedStoredSessionId,
    selectedStoredSessionIdRef: ref(selectedStoredSessionId),
    sessionStateByRuntimeIdRef,
    syncSessionStateToView: vi.fn(),
    updateSessionState: () => ({}) as ClientSessionState
  })

  useEffect(() => {
    onReady(actions)
  }, [actions, onReady])

  return null
}

describe('branchStoredSession desktop source tagging', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(ensureGatewayProfile).mockResolvedValue(undefined)
    $activeGatewayProfile.set('default')
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
  })

  afterEach(() => {
    cleanup()
    setSessions([])
    $sessionTiles.set([])
    setSelectedStoredSessionId(null)
    vi.restoreAllMocks()
  })

  it.each(['branch', 'delete', 'archive'] as const)(
    'blocks direct managed-session %s before transcript, optimistic state, or legacy mutation',
    async operation => {
      const managed = storedSession({
        id: 'stored-managed',
        message_count: 1,
        profile: 'default',
        project_id: 'project-managed'
      })

      setSessions([managed])
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
            transcript_revision: 0,
            version: 1
          }
        }
      })

      const requestGateway = vi.fn()
      let actions: StoredSessionMutationActions | null = null

      render(
        <StoredSessionMutationHarness onReady={ready => (actions = ready)} requestGateway={requestGateway as never} />
      )
      await waitFor(() => expect(actions).not.toBeNull())

      if (operation === 'branch') {
        await expect(actions!.branchStoredSession('stored-managed')).resolves.toBe(false)
      } else if (operation === 'delete') {
        await expect(actions!.removeSession('stored-managed')).resolves.toBeUndefined()
      } else {
        await expect(actions!.archiveSession('stored-managed')).resolves.toBeUndefined()
      }

      expect(getSessionMessages).not.toHaveBeenCalled()
      expect(deleteSession).not.toHaveBeenCalled()
      expect(setSessionArchived).not.toHaveBeenCalled()
      expect(requestGateway).not.toHaveBeenCalled()
      expect($sessions.get()).toEqual([managed])
    }
  )

  it('blocks the direct current-session branch action for a managed project surface', async () => {
    const managed = storedSession({
      id: 'stored-managed',
      message_count: 1,
      profile: 'default',
      project_id: 'project-managed'
    })

    setSessions([managed])
    setMessages([{ id: 'user-1', parts: [{ text: 'managed work', type: 'text' }], role: 'user' }])
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
          transcript: [{ content: 'managed work', role: 'user' }],
          transcript_revision: 1,
          version: 1
        }
      }
    })
    const requestGateway = vi.fn()
    let actions: StoredSessionMutationActions | null = null

    render(
      <StoredSessionMutationHarness
        activeSessionId="runtime-managed"
        onReady={ready => (actions = ready)}
        requestGateway={requestGateway as never}
        selectedStoredSessionId="stored-managed"
      />
    )
    await waitFor(() => expect(actions).not.toBeNull())

    await expect(actions!.branchCurrentSession()).resolves.toBe(false)
    expect(requestGateway).not.toHaveBeenCalled()
  })

  it.each(['delete', 'archive'] as const)(
    'blocks %s when legacy C is paired with a live R owned by another managed canonical session',
    async operation => {
      const legacy = storedSession({ id: 'stored-legacy', project_id: null })

      setSessions([legacy])
      $projects.set([{ id: 'project-managed', managed: true } as never])
      $projectRuntimes.set({
        'project-managed': {
          events: [],
          snapshot: {
            active_run: null,
            artifacts: [],
            binding_id: 'binding-managed',
            block: null,
            canonical_session_id: 'runtime-managed',
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
      const requestGateway = vi.fn()
      let actions: StoredSessionMutationActions | null = null

      render(
        <StoredSessionMutationHarness
          activeSessionId="runtime-managed"
          onReady={ready => (actions = ready)}
          requestGateway={requestGateway as never}
          runtimeIdByStoredSessionIdRef={{ current: new Map([['stored-legacy', 'runtime-managed']]) }}
          selectedStoredSessionId="stored-legacy"
        />
      )
      await waitFor(() => expect(actions).not.toBeNull())

      if (operation === 'delete') {
        await actions!.removeSession('stored-legacy')
      } else {
        await actions!.archiveSession('stored-legacy')
      }

      expect(requestGateway).not.toHaveBeenCalled()
      expect(deleteSession).not.toHaveBeenCalled()
      expect(setSessionArchived).not.toHaveBeenCalled()
      expect($sessions.get()).toEqual([legacy])
    }
  )

  it.each(['delete', 'archive'] as const)(
    'blocks %s when any cached R for C is managed even when the reverse map still points at legacy R1',
    async operation => {
      const legacy = storedSession({ id: 'stored-legacy', project_id: null })
      const runtimeIds = { current: new Map([['stored-legacy', 'runtime-legacy']]) }
      const states = {
        current: new Map([
          ['runtime-legacy', { storedSessionId: 'stored-legacy' } as ClientSessionState],
          ['runtime-managed', { storedSessionId: 'stored-legacy' } as ClientSessionState]
        ])
      }

      setSessions([legacy])
      $projects.set([{ id: 'project-managed', managed: true } as never])
      $projectRuntimes.set({
        'project-managed': {
          events: [],
          snapshot: {
            active_run: null,
            artifacts: [],
            binding_id: 'binding-managed',
            block: null,
            canonical_session_id: 'runtime-managed',
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
      let actions: StoredSessionMutationActions | null = null

      render(
        <StoredSessionMutationHarness
          onReady={ready => (actions = ready)}
          requestGateway={vi.fn() as never}
          runtimeIdByStoredSessionIdRef={runtimeIds}
          sessionStateByRuntimeIdRef={states}
        />
      )
      await waitFor(() => expect(actions).not.toBeNull())

      if (operation === 'delete') {
        await actions!.removeSession('stored-legacy')
      } else {
        await actions!.archiveSession('stored-legacy')
      }

      expect(deleteSession).not.toHaveBeenCalled()
      expect(setSessionArchived).not.toHaveBeenCalled()
      expect($sessions.get()).toEqual([legacy])
    }
  )

  it.each(['delete', 'archive'] as const)(
    'does not evict a managed R2 rebound while legacy %s is awaiting the server',
    async operation => {
      const legacy = storedSession({ id: 'stored-legacy', project_id: null })
      const runtimeIds = { current: new Map([['stored-legacy', 'runtime-legacy']]) }

      const states = {
        current: new Map([
          ['runtime-legacy', { storedSessionId: 'stored-legacy' } as ClientSessionState],
          ['runtime-managed', { storedSessionId: 'managed-D' } as ClientSessionState]
        ])
      }

      const pending = deferred<{ ok: boolean }>()

      setSessions([legacy])

      if (operation === 'delete') {
        vi.mocked(deleteSession).mockReturnValue(pending.promise)
      } else {
        vi.mocked(setSessionArchived).mockReturnValue(pending.promise)
      }

      let actions: StoredSessionMutationActions | null = null

      render(
        <StoredSessionMutationHarness
          onReady={ready => (actions = ready)}
          requestGateway={vi.fn() as never}
          runtimeIdByStoredSessionIdRef={runtimeIds}
          sessionStateByRuntimeIdRef={states}
        />
      )
      await waitFor(() => expect(actions).not.toBeNull())

      const action =
        operation === 'delete' ? actions!.removeSession('stored-legacy') : actions!.archiveSession('stored-legacy')

      await waitFor(() => expect(operation === 'delete' ? deleteSession : setSessionArchived).toHaveBeenCalled())

      runtimeIds.current.set('stored-legacy', 'runtime-managed')
      $projects.set([{ id: 'project-managed', managed: true } as never])
      $projectRuntimes.set({
        'project-managed': {
          events: [],
          snapshot: {
            active_run: null,
            artifacts: [],
            binding_id: 'binding-managed',
            block: null,
            canonical_session_id: 'runtime-managed',
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
      pending.resolve({ ok: true })
      await action

      expect(runtimeIds.current.get('stored-legacy')).toBe('runtime-managed')
      expect(states.current.has('runtime-managed')).toBe(true)
    }
  )

  it.each(['delete', 'archive'] as const)(
    'does not evict a same-R/C replacement from another profile while legacy %s awaits REST',
    async operation => {
      const legacy = storedSession({ id: 'same-C', profile: 'default', project_id: null })
      const runtimeIds = { current: new Map([['same-C', 'same-R']]) }
      const states = {
        current: new Map([['same-R', { storedSessionId: 'same-C' } as ClientSessionState]])
      }
      const pending = deferred<{ ok: boolean }>()

      setSessions([legacy])

      if (operation === 'delete') {
        vi.mocked(deleteSession).mockReturnValue(pending.promise)
      } else {
        vi.mocked(setSessionArchived).mockReturnValue(pending.promise)
      }

      let actions: StoredSessionMutationActions | null = null

      render(
        <StoredSessionMutationHarness
          onReady={ready => (actions = ready)}
          requestGateway={vi.fn() as never}
          runtimeIdByStoredSessionIdRef={runtimeIds}
          sessionStateByRuntimeIdRef={states}
        />
      )
      await waitFor(() => expect(actions).not.toBeNull())

      const action = operation === 'delete' ? actions!.removeSession('same-C') : actions!.archiveSession('same-C')

      await waitFor(() => expect(operation === 'delete' ? deleteSession : setSessionArchived).toHaveBeenCalled())

      $activeGatewayProfile.set('work')
      configureProjectRuntimeRequester(
        vi.fn(async () => undefined),
        'work'
      )
      $projectCatalogAuthority.set({ catalogGeneration: 2, contextGeneration: 2, profile: 'work' })
      setSessions([storedSession({ id: 'same-C', profile: 'work', project_id: null })])

      pending.resolve({ ok: true })
      await action

      expect(runtimeIds.current.get('same-C')).toBe('same-R')
      expect(states.current.has('same-R')).toBe(true)
    }
  )

  it('opens the branch as a new tab and leaves the parent chat selected', async () => {
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.create') {
        return { session_id: 'branch-runtime', stored_session_id: 'branch-stored' } as never
      }

      return {} as never
    })

    // Parent is the currently-open (primary) chat.
    setSessions([storedSession({ id: 'stored-parent', message_count: 1 })])
    setSelectedStoredSessionId('stored-parent')
    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'branch me', role: 'user', timestamp: 1 }],
      session_id: 'stored-parent'
    } as never)

    const navigate = vi.fn()
    let branchStoredSession: ((storedSessionId: string) => Promise<boolean>) | null = null
    render(
      <BranchHarness
        navigate={navigate}
        onReady={branch => (branchStoredSession = branch)}
        requestGateway={requestGateway}
      />
    )
    await waitFor(() => expect(branchStoredSession).not.toBeNull())

    await expect(branchStoredSession!('stored-parent')).resolves.toBe(true)

    // The branch opened as its own tab...
    expect($sessionTiles.get().some(tile => tile.storedSessionId === 'branch-stored')).toBe(true)
    // ...without stealing the primary selection or navigating away from the parent.
    expect($selectedStoredSessionId.get()).toBe('stored-parent')
    expect(navigate).not.toHaveBeenCalledWith(sessionRoute('branch-stored'))
  })

  it('tags desktop branch sessions as desktop sessions', async () => {
    let createParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.create') {
        createParams = params

        return { session_id: 'branch-runtime', stored_session_id: 'branch-stored' } as never
      }

      return {} as never
    })

    setSessions([storedSession({ id: 'stored-parent', message_count: 1 })])
    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'branch me', role: 'user', timestamp: 1 }],
      session_id: 'stored-parent'
    } as never)

    let branchStoredSession: ((storedSessionId: string) => Promise<boolean>) | null = null
    render(<BranchHarness onReady={branch => (branchStoredSession = branch)} requestGateway={requestGateway} />)
    await waitFor(() => expect(branchStoredSession).not.toBeNull())

    await expect(branchStoredSession!('stored-parent')).resolves.toBe(true)

    expect(createParams).toMatchObject({
      parent_session_id: 'stored-parent',
      source: 'desktop'
    })
  })

  // #67603: right-clicking a session outside the paginated sidebar window is a
  // cache miss. Resolve its owning profile (cache → active → cross-profile) and
  // swap to it before reading the transcript / creating the branch, so the fork
  // is not created on whichever profile happens to be live.
  it('resolves and swaps to the parent profile when the branched session is not cached', async () => {
    setSessions([])
    vi.mocked(getSession).mockResolvedValue(storedSession({ id: 'stored-parent', message_count: 1, profile: 'work' }))
    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'branch me', role: 'user', timestamp: 1 }],
      session_id: 'stored-parent'
    } as never)

    let createParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.create') {
        createParams = params

        return { session_id: 'branch-runtime', stored_session_id: 'branch-stored' } as never
      }

      return {} as never
    })

    let branchStoredSession: ((storedSessionId: string, sessionProfile?: string | null) => Promise<boolean>) | null =
      null

    render(<BranchHarness onReady={branch => (branchStoredSession = branch)} requestGateway={requestGateway} />)
    await waitFor(() => expect(branchStoredSession).not.toBeNull())

    await expect(branchStoredSession!('stored-parent')).resolves.toBe(true)

    expect(ensureGatewayProfile).toHaveBeenCalledWith('work')
    expect(getSessionMessages).toHaveBeenCalledWith('stored-parent', 'work')
    // The create itself must carry the owning profile: in app-global remote
    // mode the soft gateway swap alone is not enough — an omitted profile
    // lands the branch on the launch (default) profile's state.db.
    expect(createParams).toMatchObject({ parent_session_id: 'stored-parent', profile: 'work' })

    vi.mocked(getSession).mockReset()
  })

  it('creates the branch on the cached parent session profile', async () => {
    setSessions([storedSession({ id: 'stored-parent', message_count: 1, profile: 'work' })])
    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'branch me', role: 'user', timestamp: 1 }],
      session_id: 'stored-parent'
    } as never)

    let createParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.create') {
        createParams = params

        return { session_id: 'branch-runtime', stored_session_id: 'branch-stored' } as never
      }

      return {} as never
    })

    let branchStoredSession: ((storedSessionId: string) => Promise<boolean>) | null = null
    render(<BranchHarness onReady={branch => (branchStoredSession = branch)} requestGateway={requestGateway} />)
    await waitFor(() => expect(branchStoredSession).not.toBeNull())

    await expect(branchStoredSession!('stored-parent')).resolves.toBe(true)

    expect(ensureGatewayProfile).toHaveBeenCalledWith('work')
    expect(createParams).toMatchObject({ profile: 'work' })
  })

  it('omits profile for a profile-less parent so single-profile users are unchanged', async () => {
    setSessions([storedSession({ id: 'stored-parent', message_count: 1 })])
    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: [{ content: 'branch me', role: 'user', timestamp: 1 }],
      session_id: 'stored-parent'
    } as never)

    let createParams: Record<string, unknown> | undefined

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.create') {
        createParams = params

        return { session_id: 'branch-runtime', stored_session_id: 'branch-stored' } as never
      }

      return {} as never
    })

    let branchStoredSession: ((storedSessionId: string) => Promise<boolean>) | null = null
    render(<BranchHarness onReady={branch => (branchStoredSession = branch)} requestGateway={requestGateway} />)
    await waitFor(() => expect(branchStoredSession).not.toBeNull())

    await expect(branchStoredSession!('stored-parent')).resolves.toBe(true)

    expect(createParams).toBeDefined()
    expect(createParams).not.toHaveProperty('profile')
  })
})

// ── Warm-cache mapping integrity (the "open chat A, chat B loads" bug) ─────────
// resumeSession's warm fast-path maps storedSessionId -> runtimeId -> cached
// state. A reaped/respawned pooled backend re-mints runtime ids, so a recycled
// id can resolve to a live-but-DIFFERENT session's cache entry. The fast-path
// must verify the cached state still BELONGS to the resumed session before it
// paints, or it shows a totally different thread under the current route.
const clientState = (storedSessionId: string | null): ClientSessionState => createClientSessionState(storedSessionId)

describe('resumeSession warm-cache mapping integrity', () => {
  const reboundToManaged = (canonicalSessionId: string) => {
    $projects.set([{ id: 'project-managed', managed: true } as never])
    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: canonicalSessionId,
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 2,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [{ content: 'managed replacement', role: 'assistant' }],
          transcript_revision: 2,
          version: 2
        }
      }
    })
  }

  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
    setSessions([storedSession({ id: 'stored-A', project_id: null })])
  })

  afterEach(() => {
    cleanup()
    setActiveSessionId(null)
    setResumeFailedSessionId(null)
    setMessages([])
    setSessions([])
    $activeProjectId.set(null)
    $projects.set([])
    resetProjectRuntimeStore()
    vi.restoreAllMocks()
  })

  it('rejects a cross-wired runtime mapping and falls through to a full resume', async () => {
    // A recycled runtime id ('rt-recycled') is mapped to 'stored-A', but its
    // cached state actually belongs to a DIFFERENT session ('stored-B') — the
    // exact "open chat A, chat B loads" corruption a reaped/respawned pooled
    // backend can leave behind.
    const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = {
      current: new Map([['stored-A', 'rt-recycled']])
    }

    const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = {
      current: new Map([['rt-recycled', clientState('stored-B')]])
    }

    const requestGateway = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'session.resume') {
        return { session_id: 'rt-A-fresh', resumed: params?.session_id, messages: [], info: {} } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [] } as never)

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={r => (resume = r)}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-A', true)

    // The fast-path did NOT short-circuit on the cross-wired cache — the full
    // resume RPC ran, for the session that was actually requested.
    const resumeCalls = requestGateway.mock.calls.filter(([method]) => method === 'session.resume')
    expect(resumeCalls.length).toBe(1)
    expect(resumeCalls[0][1]).toMatchObject({ session_id: 'stored-A' })

    // The corrupt mapping was purged so it can't mis-resolve again.
    expect(runtimeIdByStoredSessionIdRef.current.has('stored-A')).toBe(false)
    expect(sessionStateByRuntimeIdRef.current.has('rt-recycled')).toBe(false)
  })

  it('honours a warm cache entry whose stored id matches and refreshes its persisted transcript', async () => {
    // Correctly-wired mapping: 'rt-A' <-> 'stored-A'. The fast-path should trust
    // it and never reach session.resume. session.activate refreshes the live
    // projection and, critically, rebinds its event transport after reconnect.
    const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = {
      current: new Map([['stored-A', 'rt-A']])
    }

    const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = {
      current: new Map([['rt-A', clientState('stored-A')]])
    }

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        return {
          session_id: 'rt-A',
          session_key: 'stored-A',
          resumed: 'stored-A',
          message_count: 0,
          messages: [],
          running: false,
          info: {}
        } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [], session_id: 'stored-A' } as never)

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={r => (resume = r)}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-A', true)

    // Fast-path served the session from cache: no full resume RPC, mapping intact.
    // The persisted transcript still refreshes in parallel because the runtime
    // projection can differ even when its row count matches.
    const methods = requestGateway.mock.calls.map(([method]) => method)
    expect(methods).toContain('session.activate')
    expect(methods).not.toContain('session.resume')
    expect(getSessionMessages).toHaveBeenCalledWith('stored-A', undefined)
    expect(runtimeIdByStoredSessionIdRef.current.get('stored-A')).toBe('rt-A')
  })

  it('does not publish a warm session.activate result after C rebounds to managed on the same R', async () => {
    const runtimeIdByStoredSessionIdRef = {
      current: new Map([['stored-A', 'same-R']])
    } satisfies MutableRefObject<Map<string, string>>
    const sessionStateByRuntimeIdRef = {
      current: new Map([['same-R', clientState('stored-A')]])
    } satisfies MutableRefObject<Map<string, ClientSessionState>>
    const activated = deferred<{
      info: Record<string, never>
      messages: never[]
      running: boolean
      session_id: string
      session_key: string
    }>()
    const onStateUpdate = vi.fn()
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        return (await activated.promise) as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [], session_id: 'stored-A' } as never)

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={onStateUpdate}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())

    let action!: Promise<unknown>
    act(() => {
      action = resume!('stored-A', true)
    })
    await waitFor(() => expect(requestGateway).toHaveBeenCalledWith('session.activate', expect.anything()))

    reboundToManaged('same-R')
    activated.resolve({
      info: {},
      messages: [],
      running: false,
      session_id: 'same-R',
      session_key: 'stored-A'
    })
    await action

    expect(onStateUpdate).not.toHaveBeenCalled()
  })

  it('does not publish a warm REST prefetch after authority rebounds while activation is settling', async () => {
    const runtimeIdByStoredSessionIdRef = {
      current: new Map([['stored-A', 'same-R']])
    } satisfies MutableRefObject<Map<string, string>>
    const sessionStateByRuntimeIdRef = {
      current: new Map([['same-R', clientState('stored-A')]])
    } satisfies MutableRefObject<Map<string, ClientSessionState>>
    const prefetch = deferred<{ messages: never[]; session_id: string }>()
    const onStateUpdate = vi.fn()
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        return {
          info: {},
          messages: [],
          running: false,
          session_id: 'same-R',
          session_key: 'stored-A'
        } as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockReturnValue(prefetch.promise as never)

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={onStateUpdate}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())

    let action!: Promise<unknown>
    act(() => {
      action = resume!('stored-A', true)
    })
    await waitFor(() => expect(requestGateway).toHaveBeenCalledWith('session.activate', expect.anything()))

    reboundToManaged('same-R')
    prefetch.resolve({ messages: [], session_id: 'stored-A' })
    await action

    expect(onStateUpdate).not.toHaveBeenCalled()
  })

  it('does not publish a cold session.resume result after same-C/R is replaced by another profile', async () => {
    const resumed = deferred<{
      info: Record<string, never>
      messages: never[]
      running: boolean
      session_id: string
      session_key: string
    }>()
    const onStateUpdate = vi.fn()
    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.resume') {
        return (await resumed.promise) as never
      }

      return {} as never
    })

    vi.mocked(getSessionMessages).mockResolvedValue({ messages: [], session_id: 'stored-A' } as never)

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(<ResumeHarness onReady={ready => (resume = ready)} onStateUpdate={onStateUpdate} requestGateway={requestGateway} />)
    await waitFor(() => expect(resume).not.toBeNull())

    let action!: Promise<unknown>
    act(() => {
      action = resume!('stored-A', true)
    })
    await waitFor(() => expect(requestGateway).toHaveBeenCalledWith('session.resume', expect.anything()))

    $activeGatewayProfile.set('work')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'work'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 2, contextGeneration: 2, profile: 'work' })
    setSessions([storedSession({ id: 'stored-A', profile: 'work', project_id: 'project-managed' })])
    reboundToManaged('same-R')
    resumed.resolve({
      info: {},
      messages: [],
      running: false,
      session_id: 'same-R',
      session_key: 'stored-A'
    })
    await action

    expect(onStateUpdate).not.toHaveBeenCalled()
  })

  it('preserves cached image attachments through an idle persisted transcript refresh', async () => {
    const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = {
      current: new Map([['stored-A', 'rt-A']])
    }

    const state = clientState('stored-A')
    state.messages = [
      {
        id: 'cached-user',
        role: 'user',
        parts: [{ type: 'text', text: 'describe this image' }],
        attachmentRefs: ['@image:/tmp/photo.png']
      },
      {
        id: 'cached-assistant',
        role: 'assistant',
        parts: [{ type: 'text', text: 'It is a photo.' }]
      }
    ]

    const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = {
      current: new Map([['rt-A', state]])
    }

    const persistedMessages = [
      { content: 'describe this image', role: 'user', timestamp: 1 },
      { content: 'It is a photo.', role: 'assistant', timestamp: 2 }
    ]

    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: persistedMessages,
      session_id: 'stored-A'
    } as never)

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        return {
          session_id: 'rt-A',
          session_key: 'stored-A',
          resumed: 'stored-A',
          message_count: persistedMessages.length,
          messages: persistedMessages,
          running: false,
          info: {}
        } as never
      }

      return {} as never
    })

    let resumedState: ClientSessionState | undefined
    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null

    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={(_sessionId, next) => (resumedState = next)}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-A', true)

    expect(requestGateway.mock.calls.map(([method]) => method)).toContain('session.activate')
    expect(getSessionMessages).toHaveBeenCalledWith('stored-A', undefined)
    expect(resumedState?.messages[0]?.attachmentRefs).toEqual(['@image:/tmp/photo.png'])
  })

  it('repairs an idle warm cache from a divergent equal-length persisted transcript', async () => {
    const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = {
      current: new Map([['stored-A', 'rt-A']])
    }

    const state = clientState('stored-A')
    state.messages = [
      {
        id: 'cached-user',
        role: 'user',
        parts: [{ type: 'text', text: 'stale runtime prompt' }]
      },
      {
        id: 'cached-assistant',
        role: 'assistant',
        parts: [{ type: 'text', text: 'stale runtime answer' }]
      }
    ]

    const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = {
      current: new Map([['rt-A', state]])
    }

    const staleRuntimeMessages = [
      { content: 'stale runtime prompt', role: 'user', timestamp: 1 },
      { content: 'stale runtime answer', role: 'assistant', timestamp: 2 }
    ]

    const persistedMessages = [
      { content: 'prompt saved after compression', role: 'user', timestamp: 3 },
      { content: 'answer saved after compression', role: 'assistant', timestamp: 4 }
    ]

    vi.mocked(getSessionMessages).mockResolvedValue({
      messages: persistedMessages,
      session_id: 'stored-A'
    } as never)

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        return {
          session_id: 'rt-A',
          session_key: 'stored-A',
          resumed: 'stored-A',
          message_count: staleRuntimeMessages.length,
          messages: staleRuntimeMessages,
          running: false,
          info: {}
        } as never
      }

      return {} as never
    })

    let resumedState: ClientSessionState | undefined
    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null

    render(
      <ResumeHarness
        onReady={ready => (resume = ready)}
        onStateUpdate={(_sessionId, next) => (resumedState = next)}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-A', true)

    const renderedMessages = JSON.stringify(resumedState?.messages)
    expect(renderedMessages).toContain('prompt saved after compression')
    expect(renderedMessages).toContain('answer saved after compression')
    expect(renderedMessages).not.toContain('stale runtime answer')
  })

  it('keeps a warm runtime and optimistic turn on a transient activation timeout', async () => {
    const runtimeIdByStoredSessionIdRef: MutableRefObject<Map<string, string>> = {
      current: new Map([['stored-A', 'rt-A']])
    }

    const state = clientState('stored-A')
    state.messages = [
      {
        id: 'user-optimistic',
        role: 'user',
        parts: [{ type: 'text', text: 'do not lose me' }]
      }
    ]

    const sessionStateByRuntimeIdRef: MutableRefObject<Map<string, ClientSessionState>> = {
      current: new Map([['rt-A', state]])
    }

    const requestGateway = vi.fn(async (method: string) => {
      if (method === 'session.activate') {
        throw new Error('request timed out: session.activate')
      }

      return {} as never
    })

    let resume: ((storedSessionId: string, replaceRoute?: boolean) => Promise<unknown>) | null = null
    render(
      <ResumeHarness
        onReady={r => (resume = r)}
        requestGateway={requestGateway}
        runtimeIdByStoredSessionIdRef={runtimeIdByStoredSessionIdRef}
        sessionStateByRuntimeIdRef={sessionStateByRuntimeIdRef}
      />
    )
    await waitFor(() => expect(resume).not.toBeNull())
    await resume!('stored-A', true)

    expect(requestGateway.mock.calls.map(([method]) => method)).not.toContain('session.resume')
    expect(runtimeIdByStoredSessionIdRef.current.get('stored-A')).toBe('rt-A')
    expect(sessionStateByRuntimeIdRef.current.get('rt-A')?.messages[0]?.id).toBe('user-optimistic')
  })
})

describe('createBackendSessionForSend workspace target', () => {
  afterEach(() => {
    cleanup()
    $newChatProfile.set(null)
    $activeGatewayProfile.set('default')
    setCurrentCwd('')
    setNewChatWorkspaceTarget(undefined)
    vi.restoreAllMocks()
  })

  it('omits cwd for an explicit no-workspace draft even when global cwd changes before send', async () => {
    const params = await createWith(
      () => {
        $activeGatewayProfile.set('default')
      },
      handle => {
        handle.startFreshSessionDraft({ workspaceTarget: null })
        $currentCwd.set('/project-open-in-file-browser')
      }
    )

    expect(params).not.toHaveProperty('cwd')
    expect($newChatWorkspaceTarget.get()).toBeUndefined()
  })

  it('uses the clicked workspace target instead of a later global cwd value', async () => {
    const params = await createWith(
      () => {
        $activeGatewayProfile.set('default')
      },
      handle => {
        handle.startFreshSessionDraft({ workspaceTarget: '/clicked-workspace' })
        $currentCwd.set('/project-open-in-file-browser')
      }
    )

    expect(params).toMatchObject({ cwd: '/clicked-workspace' })
  })
})
