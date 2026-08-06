import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { createClientSessionState } from '@/lib/chat-runtime'
import type { ProjectRuntimeSnapshot, SessionInfo } from '@/types/hermes'

const projectCommands = vi.hoisted(() => ({
  executeProjectMutation: vi.fn(),
  retryProjectMutation: vi.fn()
}))

vi.mock('./project-command-runtime', () => projectCommands)

import { $gateway } from './gateway'
import {
  dispatchNativeNotification,
  NATIVE_NOTIFICATION_KINDS,
  respondToApprovalAction,
  sendTestNativeNotification,
  setNativeNotifyEnabled,
  setNativeNotifyKind
} from './native-notifications'
import { $activeGatewayProfile } from './profile'
import { managedProjectApprovalForSurface } from './project-approval'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from './project-runtime'
import { $projectCatalogAuthority, $projects } from './projects'
import { $approvalRequest, sessionApprovalRequest, setApprovalRequest } from './prompts'
import { $activeSessionId, $sessions, setActiveSessionId } from './session'
import { $sessionStates } from './session-states'

const desktopWindow = window as unknown as { hermesDesktop?: Window['hermesDesktop'] }
const initialHermesDesktop = desktopWindow.hermesDesktop

const notify = vi.fn().mockResolvedValue(true)

function setWindowState({ focused = true, hidden = false }: { focused?: boolean; hidden?: boolean }) {
  Object.defineProperty(document, 'hidden', { configurable: true, value: hidden })
  Object.defineProperty(document, 'hasFocus', { configurable: true, value: () => focused })
}

let counter = 0

// Unique session id per call dodges the per-(kind,session) throttle so each
// assertion starts clean.
function freshSession(): string {
  counter += 1

  return `session-${counter}`
}

const managedSnapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: { control_state: 'awaiting_approval', control_version: 3, turn_id: 'turn-native' },
  artifacts: [],
  binding_id: 'binding-native',
  block: null,
  canonical_session_id: 'canonical-native',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 7,
  lifecycle: 'active',
  pending_approval: { approval_id: 'approval-native', kind: 'tool' },
  project_id: 'project-native',
  queue: [],
  transcript: [],
  transcript_revision: 2,
  version: 4,
  ...overrides
})

beforeEach(() => {
  notify.mockClear()
  desktopWindow.hermesDesktop = { notify } as unknown as Window['hermesDesktop']
  setNativeNotifyEnabled(true)

  for (const kind of NATIVE_NOTIFICATION_KINDS) {
    setNativeNotifyKind(kind, true)
  }

  setActiveSessionId(null)
  setWindowState({ focused: false, hidden: true })
  $activeGatewayProfile.set('default')
  configureProjectRuntimeRequester(
    vi.fn(async () => undefined),
    'default'
  )
  $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'default' })
  $projects.set([])
  $sessions.set([])
  $sessionStates.set({})
  projectCommands.executeProjectMutation.mockReset()
  projectCommands.retryProjectMutation.mockReset()
})

afterEach(() => {
  if (initialHermesDesktop) {
    desktopWindow.hermesDesktop = initialHermesDesktop
  } else {
    delete desktopWindow.hermesDesktop
  }

  $sessions.set([])
  $sessionStates.set({})
  $projects.set([])
  $projectCatalogAuthority.set({ catalogGeneration: null, contextGeneration: 0, profile: null })
  resetProjectRuntimeStore()
  configureProjectRuntimeRequester(undefined)
})

describe('dispatchNativeNotification focus gating', () => {
  it('fires a completion notification for the active session when the window is hidden', () => {
    const sessionId = freshSession()
    setActiveSessionId(sessionId)
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done' })
    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('fires a completion notification when the window is visible but unfocused (alt-tab)', () => {
    const sessionId = freshSession()
    setActiveSessionId(sessionId)
    setWindowState({ focused: false, hidden: false })
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done' })
    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('suppresses a completion notification when the window is focused', () => {
    const sessionId = freshSession()
    setActiveSessionId(sessionId)
    setWindowState({ focused: true, hidden: false })
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done' })
    expect(notify).not.toHaveBeenCalled()
  })

  it('suppresses a completion notification for a non-active background session (no gateway spam)', () => {
    setActiveSessionId('on-screen')
    dispatchNativeNotification({ kind: 'turnDone', sessionId: 'busy-bot-session', title: 'done' })
    expect(notify).not.toHaveBeenCalled()
  })

  it('fires an attention notification for an off-screen session even when focused', () => {
    setWindowState({ focused: true, hidden: false })
    setActiveSessionId('on-screen')
    dispatchNativeNotification({ kind: 'approval', sessionId: 'background', title: 'approve' })
    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('suppresses an attention notification for the active session when focused', () => {
    setWindowState({ focused: true, hidden: false })
    setActiveSessionId('on-screen')
    dispatchNativeNotification({ kind: 'approval', sessionId: 'on-screen', title: 'approve' })
    expect(notify).not.toHaveBeenCalled()
  })

  it('fires a global completion notification while away with no active session (pet gen)', () => {
    setActiveSessionId(null)
    dispatchNativeNotification({ global: true, kind: 'backgroundDone', title: 'Your pet hatched' })
    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('suppresses a global notification when the window is focused', () => {
    setWindowState({ focused: true, hidden: false })
    setActiveSessionId(null)
    dispatchNativeNotification({ global: true, kind: 'backgroundDone', title: 'Your pet hatched' })
    expect(notify).not.toHaveBeenCalled()
  })
})

describe('dispatchNativeNotification preferences', () => {
  it('suppresses everything when the master switch is off', () => {
    setNativeNotifyEnabled(false)
    dispatchNativeNotification({ kind: 'approval', sessionId: freshSession(), title: 'approve' })
    dispatchNativeNotification({ kind: 'turnDone', sessionId: freshSession(), title: 'done' })
    expect(notify).not.toHaveBeenCalled()
  })

  it('suppresses only the disabled kind', () => {
    const sessionId = freshSession()
    setActiveSessionId(sessionId)
    setNativeNotifyKind('turnDone', false)
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done' })
    expect(notify).not.toHaveBeenCalled()

    dispatchNativeNotification({ kind: 'turnError', sessionId, title: 'boom' })
    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('forwards kind and sessionId to the bridge', () => {
    setActiveSessionId('abc')
    dispatchNativeNotification({ body: 'hi', kind: 'turnError', sessionId: 'abc', title: 'boom' })
    expect(notify).toHaveBeenCalledWith(
      expect.objectContaining({ body: 'hi', kind: 'turnError', sessionId: 'abc', title: 'boom' })
    )
  })
})

describe('dispatchNativeNotification throttle', () => {
  it('collapses duplicate kind+session within the throttle window', () => {
    const sessionId = freshSession()
    setActiveSessionId(sessionId)
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done' })
    dispatchNativeNotification({ kind: 'turnDone', sessionId, title: 'done again' })
    expect(notify).toHaveBeenCalledTimes(1)
  })
})

describe('sendTestNativeNotification', () => {
  it('fires regardless of focus or active session', () => {
    setWindowState({ focused: true, hidden: false })
    setActiveSessionId('on-screen')
    sendTestNativeNotification('Hermes', 'works')
    expect(notify).toHaveBeenCalledTimes(1)
  })
})

describe('$activeSessionId wiring', () => {
  it('reflects the setter used for gating', () => {
    setActiveSessionId('xyz')
    expect($activeSessionId.get()).toBe('xyz')
  })
})

describe('respondToApprovalAction', () => {
  const request = vi.fn().mockResolvedValue({ resolved: true })

  function installLegacySession(sessionId: string): void {
    $sessionStates.set({
      ...$sessionStates.get(),
      [sessionId]: { ...createClientSessionState(), storedSessionId: sessionId }
    })
    $sessions.set([...$sessions.get(), { id: sessionId, profile: 'default', project_id: null } as SessionInfo])
  }

  function dispatchLegacyApproval(sessionId: string, command: string) {
    const approvalRequest = { command, description: `${command} approval`, sessionId }
    setApprovalRequest(approvalRequest)
    dispatchNativeNotification({
      actions: [
        { id: 'approve', text: 'Approve' },
        { id: 'reject', text: 'Reject' }
      ],
      approvalSource: { kind: 'legacy', request: approvalRequest },
      kind: 'approval',
      sessionId,
      title: 'Approval'
    })

    return notify.mock.lastCall?.[0]?.approvalContext
  }

  beforeEach(() => {
    request.mockClear()
    $gateway.set({ request } as unknown as ReturnType<typeof $gateway.get>)
    const authority = $projectCatalogAuthority.get()
    $projectCatalogAuthority.set({
      catalogGeneration: authority.contextGeneration,
      contextGeneration: authority.contextGeneration,
      profile: 'default'
    })
    $sessionStates.set({
      bg: { ...createClientSessionState(), storedSessionId: 'bg' }
    })
    $sessions.set([{ id: 'bg', profile: 'default', project_id: null } as SessionInfo])
  })

  afterEach(() => {
    $gateway.set(null)
  })

  it('approves via approval.respond {choice: "once"} and clears the prompt', async () => {
    setActiveSessionId('bg')
    const approvalContext = dispatchLegacyApproval('bg', 'rm -rf /')

    await respondToApprovalAction('bg', 'approve', approvalContext)

    expect(request).toHaveBeenCalledWith('approval.respond', { choice: 'once', session_id: 'bg' })
    expect($approvalRequest.get()).toBeNull()
  })

  it('rejects via approval.respond {choice: "deny"}', async () => {
    installLegacySession('bg-reject')
    const approvalContext = dispatchLegacyApproval('bg-reject', 'reject me')

    await respondToApprovalAction('bg-reject', 'reject', approvalContext)

    expect(request).toHaveBeenCalledWith('approval.respond', { choice: 'deny', session_id: 'bg-reject' })
  })

  it('resolves a managed approval canonically from the durable runtime cache identity', async () => {
    const snapshot = managedSnapshot()
    $sessionStates.set({
      'runtime-native': { ...createClientSessionState(), storedSessionId: 'canonical-native' }
    })
    $projectRuntimes.set({ 'project-native': { events: [], snapshot } })
    setApprovalRequest({ command: 'legacy', description: 'legacy', sessionId: 'runtime-native' })
    projectCommands.executeProjectMutation.mockResolvedValue({ status: 'conflict' })
    const approval = managedProjectApprovalForSurface('runtime-native', 'canonical-native').get().approval!
    const approvalContext = { approval, kind: 'managed' as const }

    await respondToApprovalAction('runtime-native', 'approve', approvalContext)

    expect(projectCommands.executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 4,
      name: 'approval.resolve',
      payload: { approval_id: 'approval-native', outcome: 'approved' },
      project_id: 'project-native'
    })
    expect(request).not.toHaveBeenCalled()
  })

  it('transports the immutable managed approval context to the native action callback', () => {
    const snapshot = managedSnapshot({ canonical_session_id: 'canonical-transport' })
    $sessionStates.set({
      'runtime-transport': { ...createClientSessionState(), storedSessionId: 'canonical-transport' }
    })
    $projectRuntimes.set({ 'project-native': { events: [], snapshot } })
    const approval = managedProjectApprovalForSurface('runtime-transport', 'canonical-transport').get().approval!
    const approvalSource = { approval, kind: 'managed' as const }

    dispatchNativeNotification({
      actions: [{ id: 'approve', text: 'Approve' }],
      approvalSource,
      kind: 'approval',
      sessionId: 'runtime-transport',
      title: 'Approval'
    })

    expect(notify).toHaveBeenCalledWith(
      expect.objectContaining({
        approvalContext: approvalSource,
        sessionId: 'runtime-transport'
      })
    )
  })

  it('does not let notification A resolve replacement managed approval B', async () => {
    const first = managedSnapshot({
      binding_id: 'binding-a',
      canonical_session_id: 'canonical-stale',
      pending_approval: { approval_id: 'approval-a', kind: 'tool' },
      version: 4
    })

    $sessionStates.set({
      'runtime-stale': { ...createClientSessionState(), storedSessionId: 'canonical-stale' }
    })
    $projectRuntimes.set({ 'project-native': { events: [], snapshot: first } })
    const approvalA = managedProjectApprovalForSurface('runtime-stale', 'canonical-stale').get().approval!

    $projectRuntimes.set({
      'project-native': {
        events: [],
        snapshot: managedSnapshot({
          binding_id: 'binding-b',
          canonical_session_id: 'canonical-stale',
          pending_approval: { approval_id: 'approval-b', kind: 'tool' },
          version: 5
        })
      }
    })
    projectCommands.executeProjectMutation.mockResolvedValue({ status: 'conflict' })

    await respondToApprovalAction('runtime-stale', 'approve', {
      approval: approvalA,
      kind: 'managed'
    })

    expect(projectCommands.executeProjectMutation).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
  })

  it('does not let a legacy notification token resolve a replacement prompt', async () => {
    installLegacySession('bg-stale')
    const approvalContext = dispatchLegacyApproval('bg-stale', 'command-a')

    setApprovalRequest({ command: 'command-b', description: 'approval B', sessionId: 'bg-stale' })
    await respondToApprovalAction('bg-stale', 'approve', approvalContext)

    expect(request).not.toHaveBeenCalled()
    expect(sessionApprovalRequest('bg-stale').get()?.command).toBe('command-b')
  })

  it('does not let a legacy notification cross a profile and gateway replacement', async () => {
    installLegacySession('bg-profile')
    const approvalContext = dispatchLegacyApproval('bg-profile', 'old profile')
    const replacementRequest = vi.fn().mockResolvedValue({ resolved: true })

    $activeGatewayProfile.set('work')
    $gateway.set({ request: replacementRequest } as unknown as ReturnType<typeof $gateway.get>)
    $projectCatalogAuthority.set({ catalogGeneration: 2, contextGeneration: 2, profile: 'work' })
    $sessions.set([{ id: 'bg-profile', profile: 'work', project_id: null } as SessionInfo])

    await respondToApprovalAction('bg-profile', 'approve', approvalContext)

    expect(request).not.toHaveBeenCalled()
    expect(replacementRequest).not.toHaveBeenCalled()
    expect(sessionApprovalRequest('bg-profile').get()?.command).toBe('old profile')
  })

  it('suppresses a stale native approval after a profile switch clears durable authority', async () => {
    $sessionStates.set({
      'runtime-old': { ...createClientSessionState(), storedSessionId: 'canonical-old' }
    })
    $projectRuntimes.set({
      'project-old': {
        events: [],
        snapshot: managedSnapshot({
          canonical_session_id: 'canonical-old',
          project_id: 'project-old'
        })
      }
    })

    const approvalContext = {
      approval: managedProjectApprovalForSurface('runtime-old', 'canonical-old').get().approval!,
      kind: 'managed' as const
    }

    $activeGatewayProfile.set('new-profile')
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'new-profile'
    )
    $projectCatalogAuthority.set({
      catalogGeneration: 2,
      contextGeneration: 2,
      profile: 'new-profile'
    })
    $sessionStates.set({})

    await respondToApprovalAction('runtime-old', 'approve', approvalContext)

    expect(projectCommands.executeProjectMutation).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
  })

  it('suppresses native approval when two runtimes claim the durable session', async () => {
    $sessionStates.set({
      'runtime-native': { ...createClientSessionState(), storedSessionId: 'canonical-native' }
    })
    const first = managedSnapshot({ binding_id: 'binding-a', project_id: 'project-a' })
    const second = managedSnapshot({ binding_id: 'binding-b', project_id: 'project-b' })
    $projectRuntimes.set({
      'project-a': { events: [], snapshot: first }
    })

    const approvalContext = {
      approval: managedProjectApprovalForSurface('runtime-native', 'canonical-native').get().approval!,
      kind: 'managed' as const
    }

    $projectRuntimes.set({
      'project-a': { events: [], snapshot: first },
      'project-b': { events: [], snapshot: second }
    })

    await respondToApprovalAction('runtime-native', 'approve', approvalContext)

    expect(projectCommands.executeProjectMutation).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
  })

  it('suppresses a native action with no durable cache identity during boot', async () => {
    $sessionStates.set({
      'runtime-unknown': { ...createClientSessionState(), storedSessionId: 'canonical-unknown' }
    })
    $projectRuntimes.set({
      'project-native': {
        events: [],
        snapshot: managedSnapshot({ canonical_session_id: 'canonical-unknown' })
      }
    })

    const approvalContext = {
      approval: managedProjectApprovalForSurface('runtime-unknown', 'canonical-unknown').get().approval!,
      kind: 'managed' as const
    }

    $sessionStates.set({})
    $sessions.set([])

    await respondToApprovalAction('runtime-unknown', 'approve', approvalContext)

    expect(projectCommands.executeProjectMutation).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
  })

  it('suppresses an older session-only notification action without immutable context', async () => {
    setApprovalRequest({ command: 'legacy', description: 'legacy approval', sessionId: 'bg' })

    await respondToApprovalAction('bg', 'approve')

    expect(projectCommands.executeProjectMutation).not.toHaveBeenCalled()
    expect(request).not.toHaveBeenCalled()
    expect(sessionApprovalRequest('bg').get()?.command).toBe('legacy')
  })

  it('ignores unknown action ids', async () => {
    await respondToApprovalAction('bg', 'snooze')
    expect(request).not.toHaveBeenCalled()
  })

  it('no-ops without a gateway', async () => {
    installLegacySession('bg-offline')
    const approvalContext = dispatchLegacyApproval('bg-offline', 'offline')
    $gateway.set(null)

    await respondToApprovalAction('bg-offline', 'approve', approvalContext)

    expect(request).not.toHaveBeenCalled()
  })
})
