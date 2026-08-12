import { cleanup, render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import {
  $managedComposerActionsBySession,
  $managedComposerAmbiguitiesBySession,
  resetOptimisticProjectPrompts
} from '@/store/project-composer-queue'

import { ComposerStatusStack } from '.'

vi.mock('@/i18n', () => ({
  useI18n: () => ({
    t: {
      common: { retry: 'i18n retry frozen intent' },
      statusStack: {
        managedProject: {
          ambiguousSession: 'i18n ambiguous project authority',
          title: 'i18n project message'
        }
      }
    }
  })
}))

describe('ComposerStatusStack managed project actions', () => {
  beforeEach(() => {
    vi.stubGlobal(
      'ResizeObserver',
      class {
        disconnect() {}
        observe() {}
      }
    )
  })

  afterEach(() => {
    cleanup()
    resetOptimisticProjectPrompts()
    vi.unstubAllGlobals()
  })

  it('surfaces a session-scoped explicit retry above the composer', () => {
    $managedComposerActionsBySession.set({
      'session-a': {
        binding_id: 'binding-a',
        intent_id: 'intent-a',
        local_id: 'local-a',
        message: 'Canonical enqueue needs confirmation.',
        project_id: 'project-a',
        session_id: 'session-a',
        status: 'retry_required',
        text: 'Ship it'
      }
    })

    render(
      <MemoryRouter>
        <ComposerStatusStack queue={null} sessionId="session-a" />
      </MemoryRouter>
    )

    expect(screen.getByText('i18n project message')).not.toBeNull()
    expect(screen.getByText('Canonical enqueue needs confirmation.')).not.toBeNull()
    expect(screen.getByRole('button', { name: 'i18n retry frozen intent' })).not.toBeNull()
  })

  it('surfaces fail-closed ambiguity for the affected canonical session', () => {
    $managedComposerAmbiguitiesBySession.set({ 'canonical-session-a': true })

    render(
      <MemoryRouter>
        <ComposerStatusStack managedSessionId="canonical-session-a" queue={null} sessionId="runtime-session-a" />
      </MemoryRouter>
    )

    expect(screen.getByText('i18n project message')).not.toBeNull()
    expect(screen.getByText('i18n ambiguous project authority')).not.toBeNull()
  })
})
