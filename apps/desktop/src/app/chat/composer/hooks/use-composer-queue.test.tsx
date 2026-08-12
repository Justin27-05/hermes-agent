import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  $parkedQueueSessions,
  $queuedPromptsBySession,
  enqueueQueuedPrompt,
  getQueuedPrompts,
  isQueueParked,
  parkQueuedPrompts
} from '@/store/composer-queue'
import {
  $managedVoiceRecoveries,
  captureProjectSubmitAuthority,
  quarantineProjectVoicePrompt
} from '@/store/project-composer-queue'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'

import type { QueueEditState } from '../composer-utils'
import type { ChatBarProps } from '../types'

import { useComposerQueue } from './use-composer-queue'

// The park ↔ drain contract at the hook level. The store tests pin the pure
// pieces (shouldAutoDrain, park bookkeeping); these pin the wiring — the
// auto-drain effect honoring the park, and send-now-while-busy lifting it so
// the settle drain still flows (the regression that sank the old blanket
// interrupt latch).

const SESSION_KEY = 'stored-session-queue-hook'

function renderQueueHook(overrides: { busy?: boolean; draft?: string; onCancel?: () => void } = {}) {
  const onSubmit = vi.fn<ChatBarProps['onSubmit']>(async () => true)
  const onCancel = overrides.onCancel ?? vi.fn()
  const loadIntoComposer = vi.fn()
  const queueEditRef: { current: QueueEditState | null } = { current: null }

  const hook = renderHook(
    ({ busy }: { busy: boolean }) =>
      useComposerQueue({
        activeQueueSessionKey: SESSION_KEY,
        attachments: [],
        busy,
        clearDraft: () => undefined,
        draftRef: { current: overrides.draft ?? '' },
        focusInput: () => undefined,
        loadIntoComposer,
        onCancel,
        onSubmit,
        queueEditRef,
        queueSessionKey: SESSION_KEY,
        sessionId: 'rt-session-queue-hook'
      }),
    { initialProps: { busy: overrides.busy ?? false } }
  )

  return { hook, loadIntoComposer, onCancel, onSubmit }
}

describe('useComposerQueue park integration', () => {
  beforeEach(() => {
    window.localStorage.clear()
    $queuedPromptsBySession.set({})
    $parkedQueueSessions.set({})
    resetProjectRuntimeStore()
    $managedVoiceRecoveries.set({})
  })

  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    $queuedPromptsBySession.set({})
    $parkedQueueSessions.set({})
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
    $managedVoiceRecoveries.set({})
  })

  it('auto-drains an unparked queue once idle', async () => {
    enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'flows' })

    const { onSubmit } = renderQueueHook()

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1))
    expect(getQueuedPrompts(SESSION_KEY)).toHaveLength(0)
  })

  it('holds a parked queue at the idle settle (the Stop edge)', async () => {
    enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'halted' })
    parkQueuedPrompts(SESSION_KEY)

    const { hook, onSubmit } = renderQueueHook({ busy: true })

    // The Stop settle: busy flips false with the park in place.
    hook.rerender({ busy: false })

    await act(async () => {
      await Promise.resolve()
    })

    expect(onSubmit).not.toHaveBeenCalled()
    expect(getQueuedPrompts(SESSION_KEY)).toHaveLength(1)
  })

  it('drainNextQueued sends a parked entry and lifts the park (manual resume)', async () => {
    enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'resumed' })
    parkQueuedPrompts(SESSION_KEY)

    const { hook, onSubmit } = renderQueueHook()

    await act(async () => {
      await hook.result.current.drainNextQueued()
    })

    expect(onSubmit).toHaveBeenCalledTimes(1)
    expect(isQueueParked(SESSION_KEY)).toBe(false)
  })

  it('sendQueuedNow while busy unparks so the settle drain flows (no stale latch)', async () => {
    const first = enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'first' })
    enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'send me now' })
    parkQueuedPrompts(SESSION_KEY)

    const { hook, onCancel, onSubmit } = renderQueueHook({ busy: true })
    const target = getQueuedPrompts(SESSION_KEY).find(e => e.id !== first!.id)!

    act(() => {
      hook.result.current.sendQueuedNow(target.id)
    })

    // The interrupt fired and the park lifted — this interrupt exists to reach
    // the queue, not to halt it.
    expect(onCancel).toHaveBeenCalledTimes(1)
    expect(isQueueParked(SESSION_KEY)).toBe(false)

    // Turn settles → the promoted entry drains.
    hook.rerender({ busy: false })

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1))
    expect(onSubmit.mock.calls[0]?.[0]).toBe('send me now')
  })

  it('never persists a managed project draft into the legacy composer queue', () => {
    $projectRuntimes.set({
      'project-a': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-a',
          block: null,
          canonical_session_id: 'rt-session-queue-hook',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-a',
          queue: [],
          transcript: [],
          transcript_revision: 0,
          version: 1
        }
      }
    })
    const { hook } = renderQueueHook({ draft: 'canonical only' })

    act(() => {
      expect(hook.result.current.queueCurrentDraft()).toBe(false)
    })
    expect(getQueuedPrompts(SESSION_KEY)).toEqual([])
  })

  it('quarantines a legacy queue on managed transition and restores it only as a draft', async () => {
    const legacy = enqueueQueuedPrompt(SESSION_KEY, { attachments: [], text: 'recover this text' })!
    const { hook, loadIntoComposer, onCancel, onSubmit } = renderQueueHook({ busy: true })

    act(() => {
      $projectRuntimes.set({
        'project-a': {
          events: [],
          snapshot: {
            active_run: { control_state: 'running', control_version: 1, turn_id: 'turn-a' },
            artifacts: [],
            binding_id: 'binding-a',
            block: null,
            canonical_session_id: 'rt-session-queue-hook',
            current_phase: 'implementation',
            delivery_status: { error_code: null, state: 'caught_up' },
            last_sequence: 1,
            lifecycle: 'active',
            pending_approval: null,
            project_id: 'project-a',
            queue: [],
            transcript: [],
            transcript_revision: 0,
            version: 1
          }
        }
      })
    })

    await waitFor(() => expect(hook.result.current.queuedPrompts).toEqual([]))
    expect(hook.result.current.quarantinedManagedPrompts).toEqual([legacy])
    expect(onSubmit).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()

    act(() => {
      expect(hook.result.current.restoreManagedLegacyPrompt(legacy.id)).toBe(true)
    })

    expect(loadIntoComposer).toHaveBeenCalledWith('recover this text', [])
    expect(getQueuedPrompts(SESSION_KEY)).toEqual([])
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('reveals an in-memory voice quarantine only to its exact managed scope and restores it as a draft', () => {
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-a'
    )
    $projectRuntimes.set({
      'project-a': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-a',
          block: null,
          canonical_session_id: 'rt-session-queue-hook',
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-a',
          queue: [],
          transcript: [],
          transcript_revision: 0,
          version: 1
        }
      }
    })
    const captured = captureProjectSubmitAuthority('rt-session-queue-hook')
    quarantineProjectVoicePrompt(captured, 'recover voice only here')
    const { hook, loadIntoComposer } = renderQueueHook()
    const entry = hook.result.current.quarantinedVoicePrompts[0]

    expect(entry?.text).toBe('recover voice only here')

    act(() => {
      const snapshot = $projectRuntimes.get()['project-a']!.snapshot
      configureProjectRuntimeRequester(
        vi.fn(async () => undefined),
        'profile-b'
      )
      $projectRuntimes.set({
        'project-a': {
          events: [],
          snapshot: {
            ...snapshot,
            binding_id: 'binding-b'
          }
        }
      })
    })

    expect(hook.result.current.quarantinedVoicePrompts).toEqual([])
    expect(hook.result.current.restoreManagedVoicePrompt(entry!.id)).toBe(false)
    expect(loadIntoComposer).not.toHaveBeenCalled()

    act(() => {
      const snapshot = $projectRuntimes.get()['project-a']!.snapshot
      configureProjectRuntimeRequester(
        vi.fn(async () => undefined),
        'profile-a'
      )
      $projectRuntimes.set({
        'project-a': {
          events: [],
          snapshot: {
            ...snapshot,
            binding_id: 'binding-a'
          }
        }
      })
    })

    expect(hook.result.current.quarantinedVoicePrompts).toEqual([])
    expect(hook.result.current.restoreManagedVoicePrompt(entry!.id)).toBe(false)
    expect(loadIntoComposer).not.toHaveBeenCalled()
    expect($managedVoiceRecoveries.get()).not.toEqual({})
    expect(window.localStorage.getItem('hermes-composer-queue-v1')).toBeNull()
  })
})
