import { QueryClient } from '@tanstack/react-query'
import { act, cleanup, render, waitFor } from '@testing-library/react'
import { type MutableRefObject, useEffect, useRef } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ClientSessionState } from '@/app/types'
import { createClientSessionState } from '@/lib/chat-runtime'
import { $clarifyRequests, clearClarifyRequest, setClarifyRequest } from '@/store/clarify'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import { clearAllPrompts, sessionApprovalRequest, setApprovalRequest } from '@/store/prompts'
import { $sessions } from '@/store/session'
import type { ProjectInfo, ProjectRuntimeSnapshot, RpcEvent, SessionInfo } from '@/types/hermes'

const nativeNotifications = vi.hoisted(() => ({
  dispatchNativeNotification: vi.fn()
}))

vi.mock('@/store/native-notifications', () => nativeNotifications)

import { useMessageStream } from './index'

// A `clarify.request` must leave an answerable inline row even when the
// `tool.start` that normally mounts it was missed (stream reconnect /
// hydration race). Without it the sidebar says "needs input" but the
// transcript has nowhere to render the choices, so the agent blocks forever.

const SID = 'session-1'

let handleEvent: ((event: RpcEvent) => void) | null = null
let stateRef: MutableRefObject<Map<string, ClientSessionState>> | null = null
let harnessStoredSessionId: null | string = SID

function Harness() {
  const activeSessionIdRef = useRef<string | null>(SID)

  const sessionStateByRuntimeIdRef = useRef(
    new Map<string, ClientSessionState>([
      [SID, { ...createClientSessionState(), storedSessionId: harnessStoredSessionId }]
    ])
  )

  const queryClientRef = useRef(new QueryClient())

  const stream = useMessageStream({
    activeSessionIdRef,
    hydrateFromStoredSession: vi.fn(async () => undefined),
    queryClient: queryClientRef.current,
    refreshHermesConfig: vi.fn(async () => undefined),
    refreshSessions: vi.fn(async () => undefined),
    sessionStateByRuntimeIdRef,
    updateSessionState: (sessionId, updater) => {
      const current = sessionStateByRuntimeIdRef.current.get(sessionId) ?? createClientSessionState()
      const next = updater(current)
      sessionStateByRuntimeIdRef.current.set(sessionId, next)

      return next
    }
  })

  useEffect(() => {
    handleEvent = stream.handleGatewayEvent
    stateRef = sessionStateByRuntimeIdRef
  }, [stream.handleGatewayEvent])

  return null
}

async function mountStream() {
  render(<Harness />)
  await waitFor(() => expect(handleEvent).not.toBeNull())
}

const clarifyRequest = (payload: Record<string, unknown>, profile?: string) =>
  act(() => handleEvent!({ payload, profile, session_id: SID, type: 'clarify.request' }))

const toolStart = (payload: Record<string, unknown>) =>
  act(() => handleEvent!({ payload, session_id: SID, type: 'tool.start' }))

function clarifyParts() {
  const messages = stateRef?.current.get(SID)?.messages ?? []

  return messages.flatMap(m => m.parts).filter(p => p.type === 'tool-call' && p.toolName === 'clarify')
}

const managedSnapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: { control_state: 'awaiting_approval', control_version: 3, turn_id: 'turn-blocking' },
  artifacts: [],
  binding_id: 'binding-blocking',
  block: null,
  canonical_session_id: 'canonical-session',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 7,
  lifecycle: 'active',
  pending_approval: { approval_id: 'approval-blocking', kind: 'tool' },
  project_id: 'project-blocking',
  queue: [],
  transcript: [],
  transcript_revision: 2,
  version: 4,
  ...overrides
})

describe('clarify.request stream hydration', () => {
  beforeEach(() => {
    handleEvent = null
    stateRef = null
    harnessStoredSessionId = SID
    clearClarifyRequest()
    clearAllPrompts()
    $activeGatewayProfile.set('default')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'default' })
    $projects.set([])
    $sessions.set([{ id: SID, profile: 'default', project_id: null } as SessionInfo])
    nativeNotifications.dispatchNativeNotification.mockReset()
  })

  afterEach(() => {
    cleanup()
    clearClarifyRequest()
    clearAllPrompts()
    $sessions.set([])
    $projects.set([])
    $projectCatalogAuthority.set({ catalogGeneration: null, contextGeneration: 0, profile: null })
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
    vi.restoreAllMocks()
  })

  it('mounts an answerable clarify row when the tool.start row was missed', async () => {
    await mountStream()

    clarifyRequest({ choices: ['yes', 'no'], question: 'Ship it?', request_id: 'req-1' })

    const parts = clarifyParts()
    expect(parts).toHaveLength(1)
    expect(parts[0].type === 'tool-call' && parts[0].toolCallId).toBe('req-1')
    expect(parts[0].type === 'tool-call' && parts[0].args).toMatchObject({
      choices: ['yes', 'no'],
      question: 'Ship it?'
    })
  })

  it('merges with the real tool.start row even though its id differs from the request id', async () => {
    await mountStream()

    // Reality: tool.start carries the model's tool_call_id, clarify.request a
    // separately-generated request_id. They must still collapse to ONE card
    // (correlated by question), not two.
    toolStart({ args: { choices: ['a'], question: 'Pick' }, name: 'clarify', tool_id: 'call-abc' })
    clarifyRequest({ choices: ['a'], question: 'Pick', request_id: 'req-2' })

    expect(clarifyParts()).toHaveLength(1)
  })

  it('does not duplicate when clarify.request arrives before the tool.start row', async () => {
    await mountStream()

    clarifyRequest({ choices: ['a'], question: 'Pick', request_id: 'req-3' })
    toolStart({ args: { choices: ['a'], question: 'Pick' }, name: 'clarify', tool_id: 'call-xyz' })

    expect(clarifyParts()).toHaveLength(1)
  })

  it('suppresses a managed clarify request when live and durable session ids differ', async () => {
    harnessStoredSessionId = 'canonical-session'
    $sessions.set([
      {
        id: 'canonical-session',
        profile: 'default',
        project_id: 'project-blocking'
      } as SessionInfo
    ])
    $projects.set([{ id: 'project-blocking', managed: true } as ProjectInfo])
    $projectRuntimes.set({
      'project-blocking': { events: [], snapshot: managedSnapshot() }
    })
    await mountStream()

    clarifyRequest({ choices: ['yes'], question: 'Use legacy clarify?', request_id: 'req-managed' }, 'default')

    expect($clarifyRequests.get()).toEqual({})
    expect(clarifyParts()).toHaveLength(0)
    expect(stateRef?.current.get(SID)?.needsInput).toBe(true)
    expect(nativeNotifications.dispatchNativeNotification).not.toHaveBeenCalled()
  })

  it('suppresses clarify during the managed catalog boot gap instead of storing a legacy request', async () => {
    harnessStoredSessionId = 'canonical-session'
    $sessions.set([
      {
        id: 'canonical-session',
        profile: 'default',
        project_id: 'project-blocking'
      } as SessionInfo
    ])
    $projects.set([{ id: 'project-blocking', managed: true } as ProjectInfo])
    $projectRuntimes.set({})
    await mountStream()

    clarifyRequest({ question: 'Boot gap?', request_id: 'req-boot' })

    expect($clarifyRequests.get()).toEqual({})
    expect(clarifyParts()).toHaveLength(0)
  })

  it('suppresses both legacy blocking inputs when canonical ownership is ambiguous', async () => {
    harnessStoredSessionId = 'canonical-session'
    const first = managedSnapshot({ binding_id: 'binding-a', project_id: 'project-a' })
    const second = managedSnapshot({ binding_id: 'binding-b', project_id: 'project-b' })
    $projectRuntimes.set({
      'project-a': { events: [], snapshot: first },
      'project-b': { events: [], snapshot: second }
    })
    await mountStream()

    clarifyRequest({ question: 'Ambiguous?', request_id: 'req-ambiguous' })
    act(() =>
      handleEvent!({
        payload: { command: 'legacy command', description: 'legacy approval' },
        profile: 'default',
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect($clarifyRequests.get()).toEqual({})
    expect(sessionApprovalRequest(SID).get()).toBeNull()
    expect(clarifyParts()).toHaveLength(0)
  })

  it('suppresses a managed legacy approval event while canonical approval remains authoritative', async () => {
    harnessStoredSessionId = 'canonical-session'
    $projectRuntimes.set({
      'project-blocking': { events: [], snapshot: managedSnapshot() }
    })
    await mountStream()

    act(() =>
      handleEvent!({
        payload: { command: 'legacy command', description: 'legacy approval' },
        profile: 'default',
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect(sessionApprovalRequest(SID).get()).toBeNull()
    expect(stateRef?.current.get(SID)?.needsInput).toBe(true)
    expect(nativeNotifications.dispatchNativeNotification).toHaveBeenCalledWith(
      expect.objectContaining({
        approvalSource: {
          approval: expect.objectContaining({
            approvalId: 'approval-blocking',
            bindingId: 'binding-blocking',
            runtimeSessionId: SID,
            sessionId: 'canonical-session',
            storedSessionId: 'canonical-session',
            version: 4
          }),
          kind: 'managed'
        },
        kind: 'approval',
        sessionId: SID
      })
    )
  })

  it('does not notify from an unstamped managed approval event', async () => {
    harnessStoredSessionId = 'canonical-session'
    $projectRuntimes.set({
      'project-blocking': { events: [], snapshot: managedSnapshot() }
    })
    await mountStream()

    act(() =>
      handleEvent!({
        payload: { command: 'unstamped', description: 'unstamped approval' },
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect(sessionApprovalRequest(SID).get()).toBeNull()
    expect(stateRef?.current.get(SID)?.needsInput).toBe(false)
    expect(nativeNotifications.dispatchNativeNotification).not.toHaveBeenCalled()
  })

  it.each([
    { label: 'old-profile', profile: 'default' },
    { label: 'missing-profile', profile: undefined }
  ])('preserves replacement clarify B when rejected event A is $label with the same R/C', async ({ profile }) => {
    await mountStream()
    setClarifyRequest({
      choices: ['replacement'],
      question: 'Replacement B?',
      requestId: 'replacement-b',
      sessionId: SID
    })
    stateRef!.current.set(SID, { ...stateRef!.current.get(SID)!, needsInput: true })

    $activeGatewayProfile.set('work')
    clarifyRequest({ question: 'Stale A?', request_id: 'stale-a' }, profile)

    expect($clarifyRequests.get()[SID]).toEqual({
      choices: ['replacement'],
      question: 'Replacement B?',
      requestId: 'replacement-b',
      sessionId: SID
    })
    expect(clarifyParts()).toHaveLength(0)
    expect(stateRef?.current.get(SID)?.needsInput).toBe(true)
    expect(nativeNotifications.dispatchNativeNotification).not.toHaveBeenCalled()
  })

  it.each([
    { label: 'old-profile', profile: 'default' },
    { label: 'missing-profile', profile: undefined }
  ])('preserves replacement approval B when rejected event A is $label with the same R/C', async ({ profile }) => {
    await mountStream()
    setApprovalRequest({ command: 'replacement-b', description: 'replacement approval B', sessionId: SID })
    stateRef!.current.set(SID, { ...stateRef!.current.get(SID)!, needsInput: true })

    $activeGatewayProfile.set('work')
    act(() =>
      handleEvent!({
        payload: { command: 'stale-a', description: 'stale approval A' },
        profile,
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect(sessionApprovalRequest(SID).get()).toMatchObject({
      command: 'replacement-b',
      description: 'replacement approval B'
    })
    expect(stateRef?.current.get(SID)?.needsInput).toBe(true)
    expect(nativeNotifications.dispatchNativeNotification).not.toHaveBeenCalled()
  })

  it('accepts unstamped blocking events only for a proven same-profile legacy session', async () => {
    await mountStream()

    clarifyRequest({ question: 'Current?', request_id: 'current-clarify' })
    act(() =>
      handleEvent!({
        payload: { command: 'current', description: 'current approval' },
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect($clarifyRequests.get()[SID]?.requestId).toBe('current-clarify')
    expect(sessionApprovalRequest(SID).get()?.command).toBe('current')
    expect(nativeNotifications.dispatchNativeNotification).toHaveBeenCalledWith(
      expect.objectContaining({
        approvalSource: {
          kind: 'legacy',
          request: sessionApprovalRequest(SID).get()
        },
        kind: 'approval',
        sessionId: SID
      })
    )
  })

  it('fails unstamped blocking events closed when durable session identity is missing', async () => {
    harnessStoredSessionId = null
    $sessions.set([])
    await mountStream()

    clarifyRequest({ question: 'Unknown?', request_id: 'unknown-clarify' })
    act(() =>
      handleEvent!({
        payload: { command: 'unknown', description: 'unknown approval' },
        session_id: SID,
        type: 'approval.request'
      })
    )

    expect($clarifyRequests.get()).toEqual({})
    expect(sessionApprovalRequest(SID).get()).toBeNull()
    expect(nativeNotifications.dispatchNativeNotification).not.toHaveBeenCalled()
  })
})
