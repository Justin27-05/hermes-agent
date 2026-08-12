import { afterEach, describe, expect, it, vi } from 'vitest'

import { en } from '@/i18n/en'
import type { ProjectRuntimeSnapshot } from '@/types/hermes'

import {
  $managedComposerActionsBySession,
  $managedComposerAmbiguitiesBySession,
  $optimisticProjectPrompts,
  addOptimisticProjectPrompt,
  bindOptimisticProjectPrompt,
  markManagedComposerRetry,
  projectComposerMessages,
  reconcileManagedComposerState,
  reconcileOptimisticProjectPrompts,
  resetOptimisticProjectPrompts,
  resolveManagedProjectSession,
  retryManagedComposerPrompt,
  submitManagedProjectPrompt
} from './project-composer-queue'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from './project-runtime'

const commandRuntime = vi.hoisted(() => ({
  executeProjectMutation: vi.fn(),
  isProjectMutationRetryAvailable: vi.fn(() => true),
  retryProjectMutation: vi.fn()
}))

vi.mock('./project-command-runtime', () => commandRuntime)

const snapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: null,
  artifacts: [],
  binding_id: 'binding-a',
  block: null,
  canonical_session_id: 'session-a',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 1,
  lifecycle: 'active',
  pending_approval: null,
  project_id: 'project-a',
  queue: [],
  transcript: [],
  transcript_revision: 1,
  version: 2,
  ...overrides
})

describe('project composer optimistic queue', () => {
  afterEach(() => {
    resetOptimisticProjectPrompts()
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
    commandRuntime.executeProjectMutation.mockReset()
    commandRuntime.retryProjectMutation.mockReset()
    commandRuntime.isProjectMutationRetryAvailable.mockReset().mockReturnValue(true)
  })

  it('keeps one non-persisted user row bound to the accepted canonical turn', () => {
    const initial = snapshot()
    const optimistic = addOptimisticProjectPrompt(initial, 'Ship this')

    bindOptimisticProjectPrompt(initial.project_id, optimistic.local_id, 'turn-a')
    reconcileOptimisticProjectPrompts(
      snapshot({ active_run: { control_state: 'running', control_version: 1, turn_id: 'turn-a' } })
    )

    expect($optimisticProjectPrompts.get()['project-a']).toEqual([
      expect.objectContaining({ accepted_turn_id: 'turn-a', text: 'Ship this' })
    ])
    expect(
      projectComposerMessages(
        snapshot({ active_run: { control_state: 'running', control_version: 1, turn_id: 'turn-a' } })
      )
    ).toHaveLength(1)
  })

  it('does not project or retain an optimistic row under a replacement binding with the same project id', () => {
    const initial = snapshot()
    const optimistic = addOptimisticProjectPrompt(initial, 'Old profile text')
    const replacement = snapshot({ binding_id: 'binding-replacement' })

    expect(optimistic).toEqual(
      expect.objectContaining({
        binding_id: 'binding-a',
        project_id: 'project-a',
        session_id: 'session-a'
      })
    )
    expect(projectComposerMessages(replacement)).toEqual([])

    reconcileManagedComposerState({
      [replacement.project_id]: { events: [], snapshot: replacement }
    })
    expect($optimisticProjectPrompts.get()[replacement.project_id]).toBeUndefined()
  })

  it('fails closed when more than one runtime claims the same canonical session', () => {
    const first = snapshot()
    const second = snapshot({ binding_id: 'binding-b', project_id: 'project-b' })

    expect(
      resolveManagedProjectSession(
        {
          [first.project_id]: { events: [], snapshot: first },
          [second.project_id]: { events: [], snapshot: second }
        },
        first.canonical_session_id
      )
    ).toEqual({ status: 'ambiguous' })
  })

  it('actively removes every scoped composer row when a unique owner becomes ambiguous', () => {
    const current = snapshot()
    const associated = addOptimisticProjectPrompt(current, 'Retry-scoped row')
    addOptimisticProjectPrompt(current, 'Unassociated optimistic row')
    markManagedComposerRetry(current, associated.local_id, 'intent-before-ambiguity')

    const duplicate = snapshot({ binding_id: 'binding-b', project_id: 'project-b' })

    const ambiguousRuntimes = {
      [current.project_id]: { events: [], snapshot: current },
      [duplicate.project_id]: { events: [], snapshot: duplicate }
    }

    reconcileManagedComposerState(ambiguousRuntimes)

    expect($managedComposerActionsBySession.get()).toEqual({})
    expect($optimisticProjectPrompts.get()).toEqual({})
    expect($managedComposerAmbiguitiesBySession.get()).toEqual({
      [current.canonical_session_id]: true
    })

    reconcileManagedComposerState({
      [current.project_id]: { events: [], snapshot: current }
    })

    expect($managedComposerActionsBySession.get()).toEqual({})
    expect($optimisticProjectPrompts.get()).toEqual({})
    expect($managedComposerAmbiguitiesBySession.get()).toEqual({})

    const fresh = addOptimisticProjectPrompt(current, 'Only fresh state may return')
    expect($optimisticProjectPrompts.get()[current.project_id]).toEqual([fresh])
  })

  it('replaces the optimistic row with the canonical transcript after its turn is no longer live', () => {
    const initial = snapshot()
    const optimistic = addOptimisticProjectPrompt(initial, 'Ship this')

    bindOptimisticProjectPrompt(initial.project_id, optimistic.local_id, 'turn-a')

    const canonical = snapshot({
      transcript: [{ content: 'Ship this', role: 'user' }],
      transcript_revision: initial.transcript_revision + 1
    })

    reconcileOptimisticProjectPrompts(canonical)

    expect($optimisticProjectPrompts.get()['project-a']).toBeUndefined()
    expect(projectComposerMessages(canonical)).toEqual([expect.objectContaining({ role: 'user' })])
  })

  it('retries the same frozen intent and binds its original optimistic row to the receipt', async () => {
    const current = snapshot()
    const optimistic = addOptimisticProjectPrompt(current, 'Retry me')
    $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
    markManagedComposerRetry(current, optimistic.local_id, 'intent-frozen')
    commandRuntime.retryProjectMutation.mockResolvedValue({
      result: { accepted_turn_id: 'turn-retried' },
      status: 'succeeded'
    })

    await retryManagedComposerPrompt(current.canonical_session_id)

    expect(commandRuntime.retryProjectMutation).toHaveBeenCalledWith('intent-frozen')
    expect(commandRuntime.executeProjectMutation).not.toHaveBeenCalled()
    expect($optimisticProjectPrompts.get()[current.project_id]).toEqual([
      expect.objectContaining({ accepted_turn_id: 'turn-retried', local_id: optimistic.local_id })
    ])
    expect($managedComposerActionsBySession.get()[current.canonical_session_id]).toBeUndefined()
  })

  it('blocks an existing retry when a second runtime claims its canonical session', async () => {
    const current = snapshot()
    const optimistic = addOptimisticProjectPrompt(current, 'Do not retry ambiguously')
    markManagedComposerRetry(current, optimistic.local_id, 'intent-ambiguous')
    const duplicate = snapshot({ binding_id: 'binding-b', project_id: 'project-b' })
    $projectRuntimes.set({
      [current.project_id]: { events: [], snapshot: current },
      [duplicate.project_id]: { events: [], snapshot: duplicate }
    })

    await expect(retryManagedComposerPrompt(current.canonical_session_id)).rejects.toThrow(
      en.statusStack.managedProject.retryScopeChanged
    )
    expect(commandRuntime.retryProjectMutation).not.toHaveBeenCalled()
    expect($managedComposerActionsBySession.get()).toEqual({})
    expect($optimisticProjectPrompts.get()).toEqual({})
  })

  it('clears a stale retry and optimistic row when its exact canonical runtime scope disappears', () => {
    const current = snapshot()
    const optimistic = addOptimisticProjectPrompt(current, 'Do not leak profiles')
    markManagedComposerRetry(current, optimistic.local_id, 'intent-stale')

    reconcileManagedComposerState({})

    expect($managedComposerActionsBySession.get()).toEqual({})
    expect($optimisticProjectPrompts.get()).toEqual({})
  })

  it('fences an in-flight retry when the profile reuses project and session ids under a new binding', async () => {
    const current = snapshot()
    const optimistic = addOptimisticProjectPrompt(current, 'Fence me')
    $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
    markManagedComposerRetry(current, optimistic.local_id, 'intent-fenced')
    let resolveRetry: ((value: unknown) => void) | undefined
    commandRuntime.retryProjectMutation.mockReturnValue(
      new Promise(resolve => {
        resolveRetry = resolve
      })
    )

    const pending = retryManagedComposerPrompt(current.canonical_session_id)
    await vi.waitFor(() => expect(commandRuntime.retryProjectMutation).toHaveBeenCalledTimes(1))
    const replacement = snapshot({ binding_id: 'binding-replacement' })
    const replacementRuntimes = { [replacement.project_id]: { events: [], snapshot: replacement } }
    $projectRuntimes.set(replacementRuntimes)
    reconcileManagedComposerState(replacementRuntimes)
    resolveRetry?.({ result: { accepted_turn_id: 'turn-stale' }, status: 'succeeded' })

    await expect(pending).rejects.toThrow(en.statusStack.managedProject.retryScopeChanged)
    expect($managedComposerActionsBySession.get()).toEqual({})
    expect($optimisticProjectPrompts.get()).toEqual({})
  })

  it('cannot accept an old submit after ambiguity invalidates its lease and a new submit starts', async () => {
    const current = snapshot()
    const duplicate = snapshot({ binding_id: 'binding-b', project_id: 'project-b' })
    $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
    let resolveFirst: ((value: unknown) => void) | undefined
    commandRuntime.executeProjectMutation
      .mockReturnValueOnce(
        new Promise(resolve => {
          resolveFirst = resolve
        })
      )
      .mockResolvedValueOnce({ result: { accepted_turn_id: 'turn-new' }, status: 'succeeded' })

    const submit = (text: string) =>
      submitManagedProjectPrompt({
        attachmentsPresent: false,
        copy: en.statusStack.managedProject,
        fromQueue: false,
        onOptimistic: () => undefined,
        snapshot: current,
        text
      })

    const oldSubmit = submit('old in-flight submit')
    await vi.waitFor(() => expect(commandRuntime.executeProjectMutation).toHaveBeenCalledTimes(1))

    const ambiguousRuntimes = {
      [current.project_id]: { events: [], snapshot: current },
      [duplicate.project_id]: { events: [], snapshot: duplicate }
    }

    $projectRuntimes.set(ambiguousRuntimes)
    reconcileManagedComposerState(ambiguousRuntimes)

    const restoredRuntimes = { [current.project_id]: { events: [], snapshot: current } }
    $projectRuntimes.set(restoredRuntimes)
    reconcileManagedComposerState(restoredRuntimes)

    await expect(submit('new authoritative submit')).resolves.toBe(true)
    resolveFirst?.({ result: { accepted_turn_id: 'turn-old' }, status: 'succeeded' })

    await expect(oldSubmit).resolves.toBe(false)
    expect($optimisticProjectPrompts.get()[current.project_id]).toEqual([
      expect.objectContaining({ accepted_turn_id: 'turn-new', text: 'new authoritative submit' })
    ])
  })

  it.each(['success', 'reject'] as const)(
    'does not let late generation-A %s mutate identical generation-B composer state',
    async outcome => {
      const current = snapshot()
      configureProjectRuntimeRequester(
        vi.fn(async () => undefined),
        'profile-a'
      )
      $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
      let resolveA: ((value: unknown) => void) | undefined
      let rejectA: ((reason: unknown) => void) | undefined
      commandRuntime.executeProjectMutation
        .mockReturnValueOnce(
          new Promise((resolve, reject) => {
            resolveA = resolve
            rejectA = reject
          })
        )
        .mockResolvedValueOnce({ result: { accepted_turn_id: 'turn-b' }, status: 'succeeded' })

      const submit = (text: string) =>
        submitManagedProjectPrompt({
          attachmentsPresent: false,
          copy: en.statusStack.managedProject,
          fromQueue: false,
          onOptimistic: () => undefined,
          snapshot: current,
          text
        })

      const pendingA = submit('generation A')
      await vi.waitFor(() => expect(commandRuntime.executeProjectMutation).toHaveBeenCalledTimes(1))
      configureProjectRuntimeRequester(
        vi.fn(async () => undefined),
        'profile-a'
      )
      $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
      reconcileManagedComposerState($projectRuntimes.get())

      await expect(submit('generation B')).resolves.toBe(true)

      if (outcome === 'success') {
        resolveA?.({ result: { accepted_turn_id: 'turn-a' }, status: 'succeeded' })
      } else {
        rejectA?.(new Error('late A failed'))
      }

      await expect(pendingA).resolves.toBe(false)
      expect(projectComposerMessages(current)).toEqual([
        expect.objectContaining({ parts: [expect.objectContaining({ text: 'generation B' })] })
      ])
      expect($managedComposerActionsBySession.get()).toEqual({})
    }
  )

  it('keeps raw submit failure details out of the visible managed status', async () => {
    const current = snapshot()
    $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
    commandRuntime.executeProjectMutation.mockRejectedValue(new Error('Bearer sk-secret C:\\Users\\Justin\\private'))

    await expect(
      submitManagedProjectPrompt({
        attachmentsPresent: false,
        copy: en.statusStack.managedProject,
        fromQueue: false,
        onOptimistic: () => undefined,
        snapshot: current,
        text: 'Keep this safe'
      })
    ).resolves.toBe(false)

    const action = $managedComposerActionsBySession.get()[current.canonical_session_id]
    expect(action).toEqual(
      expect.objectContaining({ message: en.statusStack.managedProject.messageFailed, status: 'failed' })
    )
    expect(action?.message).not.toMatch(/sk-secret|Users/)
  })

  it('keeps a same-scope retry failure visible without exposing transport details', async () => {
    const current = snapshot()
    const optimistic = addOptimisticProjectPrompt(current, 'Show failure')
    $projectRuntimes.set({ [current.project_id]: { events: [], snapshot: current } })
    markManagedComposerRetry(current, optimistic.local_id, 'intent-error')
    commandRuntime.retryProjectMutation.mockRejectedValue(new Error('Bearer sk-secret C:\\Users\\Justin\\private'))

    await expect(retryManagedComposerPrompt(current.canonical_session_id)).rejects.toThrow('sk-secret')

    const action = $managedComposerActionsBySession.get()[current.canonical_session_id]
    expect(action).toEqual(
      expect.objectContaining({ message: en.statusStack.managedProject.retryFailed, status: 'failed' })
    )
    expect(action?.message).not.toMatch(/sk-secret|Users/)
    expect($optimisticProjectPrompts.get()).toEqual({})
  })
})
