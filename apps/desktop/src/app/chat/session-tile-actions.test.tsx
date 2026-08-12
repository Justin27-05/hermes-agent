import { act, cleanup, renderHook } from '@testing-library/react'
import { atom } from 'nanostores'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { textPart } from '@/lib/chat-messages'
import { createComposerAttachmentScope } from '@/store/composer'
import { clearNotifications } from '@/store/notifications'
import { $activeGatewayProfile } from '@/store/profile'
import { $managedComposerActionsBySession, resetOptimisticProjectPrompts } from '@/store/project-composer-queue'
import {
  $projectRuntimes,
  configureProjectRuntimeRequester,
  projectRuntimeAuthority,
  resetProjectRuntimeStore
} from '@/store/project-runtime'
import { $projectCatalogAuthority } from '@/store/projects'
import { $sessionStates } from '@/store/session-states'
import type * as SessionStatesModule from '@/store/session-states'

import type { ComposerScope } from './composer/scope'
import { useSessionTileActions } from './session-tile-actions'

const gateway = vi.hoisted(() => ({ request: vi.fn() }))
const legacySubmit = vi.hoisted(() => vi.fn(async () => true))

const commandRuntime = vi.hoisted(() => ({
  executeProjectMutation: vi.fn(),
  isProjectMutationRetryAvailable: vi.fn(() => true),
  retryProjectMutation: vi.fn()
}))

const tileDelegate = vi.hoisted(() => ({
  archiveSession: vi.fn(),
  branchSession: vi.fn(),
  deleteSession: vi.fn(),
  executeSlash: vi.fn(),
  interruptSession: vi.fn(),
  resumeTile: vi.fn(),
  submitToSession: vi.fn(),
  updateSession: vi.fn()
}))

vi.mock('@/app/gateway/hooks/use-gateway-request', () => ({
  useGatewayRequest: () => ({ requestGateway: gateway.request })
}))
vi.mock('@/store/project-command-runtime', () => commandRuntime)

const projectFeedback = vi.hoisted(() => ({
  executeProjectMutationWithFeedback: vi.fn()
}))

vi.mock('@/store/projects', async importOriginal => ({
  ...(await importOriginal<Record<string, unknown>>()),
  ...projectFeedback
}))

vi.mock('@/store/session-states', async importOriginal => ({
  ...(await importOriginal<typeof SessionStatesModule>()),
  sessionTileDelegate: () => tileDelegate
}))
vi.mock('../session/hooks/use-prompt-actions/submit', () => ({
  useSubmitPrompt: () => legacySubmit
}))

const RUNTIME_ID = 'runtime-tile'
const STORED_ID = 'stored-tile'

const managedSnapshot = () => ({
  active_run: { control_state: 'running' as const, control_version: 1, turn_id: 'turn-running' },
  artifacts: [],
  binding_id: 'binding-tile',
  block: null,
  canonical_session_id: STORED_ID,
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' as const },
  last_sequence: 2,
  lifecycle: 'active' as const,
  pending_approval: null,
  project_id: 'project-tile',
  queue: [],
  transcript: [],
  transcript_revision: 1,
  version: 7
})

function renderManagedTile() {
  const snapshot = managedSnapshot()
  $projectRuntimes.set({ [snapshot.project_id]: { events: [], snapshot } })
  $sessionStates.set({
    [RUNTIME_ID]: {
      awaitingResponse: false,
      busy: true,
      interrupted: false,
      messages: [],
      storedSessionId: STORED_ID
    } as never
  })

  const scope: ComposerScope = {
    $awaitingInput: atom(false),
    attachments: createComposerAttachmentScope(),
    popoutAllowed: false,
    readMessages: () => $sessionStates.get()[RUNTIME_ID]?.messages ?? [],
    target: `tile:${STORED_ID}`
  }

  return renderHook(() => useSessionTileActions({ runtimeId: RUNTIME_ID, scope, storedSessionId: STORED_ID }))
}

describe('useSessionTileActions managed project routing', () => {
  beforeEach(() => {
    $activeGatewayProfile.set('default')
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    gateway.request.mockReset()
    legacySubmit.mockClear()
    commandRuntime.executeProjectMutation.mockReset().mockResolvedValue({
      result: { accepted_turn_id: 'turn-tile-accepted' },
      status: 'succeeded'
    })
    projectFeedback.executeProjectMutationWithFeedback.mockReset().mockResolvedValue({})
    tileDelegate.executeSlash.mockReset()
    tileDelegate.updateSession.mockImplementation((runtimeId, updater) => {
      const current = $sessionStates.get()[runtimeId]!
      const next = updater(current)
      $sessionStates.set({ ...$sessionStates.get(), [runtimeId]: next })

      return next
    })
    clearNotifications()
  })

  afterEach(() => {
    cleanup()
    resetOptimisticProjectPrompts()
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
    $sessionStates.set({})
  })

  it('submits a managed tile message through canonical turn.enqueue', async () => {
    const hook = renderManagedTile()

    await act(async () => expect(await hook.result.current.submitText('tile canonical message')).toBe(true))

    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'turn.enqueue',
      payload: { message: 'tile canonical message' },
      project_id: 'project-tile'
    })
    expect(gateway.request).not.toHaveBeenCalled()
    expect($sessionStates.get()[RUNTIME_ID]?.messages).toEqual([
      expect.objectContaining({ pending: true, role: 'user' })
    ])
  })

  it('does not route an explicit legacy profile-B tile into same-C managed profile A', async () => {
    const snapshot = managedSnapshot()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-a'
    )
    $projectRuntimes.set({ [snapshot.project_id]: { events: [], snapshot } })
    $activeGatewayProfile.set('profile-b')
    $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'profile-b' })

    const scope: ComposerScope = {
      $awaitingInput: atom(false),
      attachments: createComposerAttachmentScope(),
      popoutAllowed: false,
      readMessages: () => [],
      target: `tile:${STORED_ID}`
    }

    const hook = renderHook(() =>
      useSessionTileActions({
        runtimeId: RUNTIME_ID,
        scope,
        storedSession: { id: STORED_ID, profile: 'profile-b', project_id: null } as never,
        storedSessionId: STORED_ID
      })
    )

    await act(async () => expect(await hook.result.current.submitText('stay in profile B')).toBe(true))

    expect(commandRuntime.executeProjectMutation).not.toHaveBeenCalled()
    expect(legacySubmit).toHaveBeenCalled()
  })

  it('enqueues a managed tile correction instead of calling session.redirect', async () => {
    const hook = renderManagedTile()

    await act(async () => expect(await hook.result.current.steerPrompt('tile correction')).toBe(true))

    expect(commandRuntime.executeProjectMutation).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'turn.enqueue', payload: { message: 'tile correction' } })
    )
    expect(gateway.request).not.toHaveBeenCalledWith('session.redirect', expect.anything())
  })

  it('blocks managed tile reload, restore and edit without a legacy history mutation', async () => {
    const hook = renderManagedTile()

    const messages = [
      { id: 'u1', parts: [textPart('tile original')], role: 'user' as const },
      { id: 'a1', parts: [textPart('tile reply')], role: 'assistant' as const }
    ]

    $sessionStates.set({
      [RUNTIME_ID]: {
        ...$sessionStates.get()[RUNTIME_ID]!,
        busy: false,
        messages
      }
    })

    await act(async () => {
      await hook.result.current.reloadFromMessage('a1')
      await hook.result.current.restoreToMessage('u1')
      await hook.result.current.editMessage({
        content: [{ text: 'tile edited', type: 'text' }],
        parentId: null,
        role: 'user',
        sourceId: 'u1'
      } as never)
    })

    expect(gateway.request.mock.calls.map(([method]) => method)).not.toEqual(
      expect.arrayContaining(['prompt.submit', 'session.interrupt', 'session.redirect', 'session.resume'])
    )
    expect($managedComposerActionsBySession.get()[STORED_ID]).toEqual(
      expect.objectContaining({
        message: 'Managed project history changes are not supported yet.'
      })
    )
  })

  it('stops a uniquely managed tile run through canonical run.stop only', async () => {
    const runtime = managedSnapshot()
    const hook = renderManagedTile()

    await act(async () => hook.result.current.cancelRun())

    expect(projectFeedback.executeProjectMutationWithFeedback).toHaveBeenCalledWith({
      expected_version: runtime.version,
      name: 'run.stop',
      payload: {
        expected_control_version: runtime.active_run.control_version,
        turn_id: runtime.active_run.turn_id
      },
      project_id: runtime.project_id
    })
    expect(gateway.request).not.toHaveBeenCalledWith('session.interrupt', expect.anything())
  })

  it('fails ambiguous tile history and stop closed without legacy calls', async () => {
    const hook = renderManagedTile()
    const first = managedSnapshot()
    const duplicate = { ...first, binding_id: 'binding-duplicate', project_id: 'project-duplicate' }
    $projectRuntimes.set({
      [first.project_id]: { events: [], snapshot: first },
      [duplicate.project_id]: { events: [], snapshot: duplicate }
    })

    await act(async () => {
      await hook.result.current.reloadFromMessage(null)
      await hook.result.current.cancelRun()
    })

    expect(projectFeedback.executeProjectMutationWithFeedback).not.toHaveBeenCalled()
    expect(gateway.request).not.toHaveBeenCalled()
  })

  it('honors a frozen tile voice target instead of the replacement runtime ref', async () => {
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      ' profile-a '
    )
    const hook = renderManagedTile()
    const initial = managedSnapshot()
    const requesterAuthority = projectRuntimeAuthority()

    const frozenAuthority = {
      bindingId: initial.binding_id,
      projectId: initial.project_id,
      requesterGeneration: requesterAuthority.requesterGeneration,
      requesterScope: requesterAuthority.scope,
      sessionId: initial.canonical_session_id,
      status: 'managed'
    }

    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-b'
    )
    const replacement = { ...initial, binding_id: 'binding-replacement' }
    $projectRuntimes.set({ [replacement.project_id]: { events: [], snapshot: replacement } })

    await act(async () =>
      expect(
        await hook.result.current.submitText('tile voice bound to A', {
          projectAuthority: frozenAuthority,
          sessionId: initial.canonical_session_id
        } as never)
      ).toBe(false)
    )

    expect(commandRuntime.executeProjectMutation).not.toHaveBeenCalled()
    expect(gateway.request).not.toHaveBeenCalled()
  })
})
