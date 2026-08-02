import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { atom } from 'nanostores'
import type { PropsWithChildren } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { type ComposerAttachment, createComposerAttachmentScope } from '@/store/composer'
import { $projectRuntimes, resetProjectRuntimeStore } from '@/store/project-runtime'

import { type ComposerTarget, requestComposerSubmit } from '../focus'
import { ComposerScopeProvider, MAIN_COMPOSER_SCOPE } from '../scope'

import { useComposerSubmit } from './use-composer-submit'

interface SubmitHarnessOptions {
  attachments?: ComposerAttachment[]
  busy?: boolean
  compacting?: boolean
  managedCanonicalSessionId?: string
  target?: ComposerTarget
  submitResult?: boolean
  text?: string
}

function renderSubmitHook({
  attachments = [],
  busy = false,
  compacting = false,
  managedCanonicalSessionId,
  target = 'main',
  submitResult = true,
  text = ''
}: SubmitHarnessOptions = {}) {
  if (managedCanonicalSessionId) {
    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: { control_state: 'running', control_version: 1, turn_id: 'turn-active' },
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: managedCanonicalSessionId,
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [],
          transcript_revision: 0,
          version: 2
        }
      }
    })
  }

  const draftRef = { current: text }
  const editor = globalThis.document.createElement('div')
  editor.dataset.slot = 'composer-rich-input'
  editor.textContent = text
  const editorRef = { current: editor }
  const onCancel = vi.fn()
  const onSteer = vi.fn(async () => true)
  const onSubmit = vi.fn(async () => submitResult)
  const queueCurrentDraft = vi.fn(() => true)
  const loadIntoComposer = vi.fn()
  const stashAt = vi.fn()

  const clearDraft = vi.fn(() => {
    draftRef.current = ''
    editorRef.current!.textContent = ''
  })

  const scope =
    target === 'main'
      ? MAIN_COMPOSER_SCOPE
      : {
          $awaitingInput: atom(false),
          attachments: createComposerAttachmentScope(),
          popoutAllowed: false,
          readMessages: () => [],
          target
        }

  const wrapper = ({ children }: PropsWithChildren) => (
    <ComposerScopeProvider value={scope}>{children}</ComposerScopeProvider>
  )

  const hook = renderHook(
    () =>
      useComposerSubmit({
        activeQueueSessionKey: 'stored-session',
        activeQueueSessionKeyRef: { current: 'stored-session' },
        attachments,
        busy,
        compacting,
        clearDraft,
        disabled: false,
        draftRef,
        drainNextQueued: vi.fn(async () => false),
        editorRef,
        exitQueuedEdit: vi.fn(() => false),
        focusInput: vi.fn(),
        inputDisabled: false,
        loadIntoComposer,
        onCancel,
        onSteer,
        onSubmit,
        queueCurrentDraft,
        queueEdit: null,
        queuedPrompts: [],
        sessionId: 'runtime-session',
        setComposerText: vi.fn(),
        stashAt
      }),
    { wrapper }
  )

  return { clearDraft, hook, loadIntoComposer, onCancel, onSteer, onSubmit, queueCurrentDraft, stashAt }
}

describe('useComposerSubmit busy-turn routing', () => {
  afterEach(() => {
    cleanup()
    resetProjectRuntimeStore()
    vi.restoreAllMocks()
  })

  it('routes busy managed text through canonical submit before steer or the legacy queue', async () => {
    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      managedCanonicalSessionId: 'runtime-session',
      text: 'canonical follow-up'
    })

    act(() => hook.result.current.submitDraft())

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith('canonical follow-up', {
        attachments: [],
        composerScope: 'stored-session'
      })
    )
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('delivers an external main submit only to the exact main composer target', async () => {
    const main = renderSubmitHook()
    const tile = renderSubmitHook({ target: 'tile:stored-a' })

    act(() => requestComposerSubmit('external main task', { target: 'main' }))

    await waitFor(() =>
      expect(main.onSubmit).toHaveBeenCalledWith('external main task', { composerScope: 'stored-session' })
    )
    expect(tile.onSubmit).not.toHaveBeenCalled()
  })

  it('delivers an external tile submit only to that exact tile composer target', async () => {
    const main = renderSubmitHook()
    const tile = renderSubmitHook({ target: 'tile:stored-a' })

    act(() => requestComposerSubmit('external tile task', { target: 'tile:stored-a' }))

    await waitFor(() =>
      expect(tile.onSubmit).toHaveBeenCalledWith('external tile task', { composerScope: 'stored-session' })
    )
    expect(main.onSubmit).not.toHaveBeenCalled()
  })

  it.each([
    ['while compacting', { compacting: true, text: 'wait for canonical state' }],
    ['when the text resembles a slash command', { compacting: false, text: '/compress preserve context' }]
  ])('routes managed text through canonical submit %s', async (_label, scenario) => {
    const { hook, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      managedCanonicalSessionId: 'runtime-session',
      ...scenario
    })

    act(() => hook.result.current.submitDraft())

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith(scenario.text, {
        attachments: [],
        composerScope: 'stored-session'
      })
    )
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
  })

  it('routes the explicit queue control through canonical submit for a managed project', async () => {
    const { hook, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      managedCanonicalSessionId: 'runtime-session',
      text: 'send from the queue control'
    })

    act(() => hook.result.current.queueDraft())

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith('send from the queue control', {
        attachments: [],
        composerScope: 'stored-session'
      })
    )
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
  })

  it('fails managed attachments closed through canonical submit and restores the draft visibly', async () => {
    const attachment: ComposerAttachment = { id: 'doc', kind: 'file', label: 'notes.txt', path: 'C:\\notes.txt' }

    const { hook, loadIntoComposer, onSteer, onSubmit, queueCurrentDraft, stashAt } = renderSubmitHook({
      attachments: [attachment],
      busy: true,
      managedCanonicalSessionId: 'runtime-session',
      submitResult: false,
      text: 'read this'
    })

    act(() => hook.result.current.submitDraft())

    await waitFor(() => expect(loadIntoComposer).toHaveBeenCalledWith('read this', [attachment]))
    expect(stashAt).toHaveBeenCalledWith('stored-session', 'read this', [attachment])
    expect(onSubmit).toHaveBeenCalledWith('read this', {
      attachments: [attachment],
      composerScope: 'stored-session'
    })
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
  })

  it('steers a plain-text follow-up instead of queueing or stopping', async () => {
    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      text: 'change course'
    })

    act(() => {
      hook.result.current.submitDraft()
    })

    await waitFor(() => expect(onSteer).toHaveBeenCalledWith('change course'))
    expect(queueCurrentDraft).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('queues a plain-text follow-up while the active turn is compacting', () => {
    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      compacting: true,
      text: 'wait for the summary'
    })

    act(() => {
      hook.result.current.submitDraft()
    })

    expect(queueCurrentDraft).toHaveBeenCalledTimes(1)
    expect(onSteer).not.toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('runs slash commands immediately while busy', async () => {
    const { clearDraft, hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      busy: true,
      text: '/compress preserve context'
    })

    act(() => {
      hook.result.current.submitDraft()
    })

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith('/compress preserve context', { composerScope: 'stored-session' })
    )
    expect(clearDraft).toHaveBeenCalledTimes(1)
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('queues an attachment-bearing follow-up while busy', () => {
    const attachment: ComposerAttachment = { id: 'doc', kind: 'file', label: 'notes.txt' }

    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({
      attachments: [attachment],
      busy: true,
      text: 'read this'
    })

    act(() => {
      hook.result.current.submitDraft()
    })

    expect(queueCurrentDraft).toHaveBeenCalledTimes(1)
    expect(onSteer).not.toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('stops an active turn only with an empty composer', () => {
    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({ busy: true })

    act(() => {
      hook.result.current.submitDraft()
    })

    expect(onCancel).toHaveBeenCalledTimes(1)
    expect(onSteer).not.toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
  })

  it('submits a normal turn while idle', async () => {
    const { hook, onCancel, onSteer, onSubmit, queueCurrentDraft } = renderSubmitHook({ text: 'ordinary question' })

    act(() => {
      hook.result.current.submitDraft()
    })

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith('ordinary question', {
        attachments: [],
        composerScope: 'stored-session'
      })
    )
    expect(onSteer).not.toHaveBeenCalled()
    expect(queueCurrentDraft).not.toHaveBeenCalled()
    expect(onCancel).not.toHaveBeenCalled()
  })

  it('threads the loaded composer scope through onSubmit for the #59305 submit-time guard', async () => {
    const { hook, onSubmit } = renderSubmitHook({ text: 'hello' })

    act(() => {
      hook.result.current.submitDraft()
    })

    await waitFor(() =>
      expect(onSubmit).toHaveBeenCalledWith('hello', expect.objectContaining({ composerScope: 'stored-session' }))
    )
  })
})
