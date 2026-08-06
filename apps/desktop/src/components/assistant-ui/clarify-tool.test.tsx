import type { ToolCallMessagePartProps } from '@assistant-ui/react'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { atom } from 'nanostores'
import type { ReactNode } from 'react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { onComposerInsertRequest } from '@/app/chat/composer/focus'
import { PRIMARY_SESSION_VIEW, type SessionView, SessionViewProvider } from '@/app/chat/session-view'
import { I18nProvider } from '@/i18n'
import { clearClarifyRequest, setClarifyRequest } from '@/store/clarify'
import { $gateway } from '@/store/gateway'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import { $activeSessionId, $selectedStoredSessionId, $sessions } from '@/store/session'
import type { ProjectInfo, ProjectRuntimeSnapshot, SessionInfo } from '@/types/hermes'

import { ClarifyTool, readClarifyResult } from './clarify-tool'

// The live pending card only renders while its message is running. Force that so
// keyboard-navigation tests can exercise ClarifyToolPending directly.
vi.mock('@assistant-ui/react', () => ({
  useAuiState: () => true
}))

beforeEach(() => {
  $activeGatewayProfile.set('default')
  configureProjectRuntimeRequester(
    vi.fn(async () => undefined),
    'default'
  )
  $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'default' })
  $projects.set([])
  $sessions.set([{ id: 'session-1', profile: 'default', project_id: null } as SessionInfo])
  $selectedStoredSessionId.set('session-1')
})

afterEach(() => {
  cleanup()
  clearClarifyRequest()
  $activeSessionId.set(null)
  $selectedStoredSessionId.set(null)
  $gateway.set(null)
  $sessions.set([])
  $projects.set([])
  $projectCatalogAuthority.set({ catalogGeneration: null, contextGeneration: 0, profile: null })
  resetProjectRuntimeStore()
  configureProjectRuntimeRequester(undefined)
  vi.clearAllMocks()
})

function renderClarify(ui: ReactNode) {
  return render(
    <I18nProvider configClient={null} initialLocale="en">
      {ui}
    </I18nProvider>
  )
}

function renderClarifyForSurface(ui: ReactNode, runtimeId: string, storedId: null | string) {
  const view: SessionView = {
    ...PRIMARY_SESSION_VIEW,
    kind: 'tile',
    $runtimeId: atom(runtimeId),
    $storedId: atom(storedId)
  }

  return renderClarify(<SessionViewProvider value={view}>{ui}</SessionViewProvider>)
}

function markCatalogCurrent(): void {
  const authority = $projectCatalogAuthority.get()
  $projectCatalogAuthority.set({
    catalogGeneration: authority.contextGeneration,
    contextGeneration: authority.contextGeneration,
    profile: 'default'
  })
}

function settledClarifyProps(
  args: ToolCallMessagePartProps['args'],
  result: ToolCallMessagePartProps['result'],
  toolCallId: string
): ToolCallMessagePartProps {
  return {
    addResult: vi.fn(),
    args,
    argsText: JSON.stringify(args),
    isError: false,
    respondToApproval: vi.fn(),
    result,
    resume: vi.fn(),
    status: { type: 'complete' },
    toolCallId,
    toolName: 'clarify',
    type: 'tool-call'
  }
}

function liveClarifyProps(choices = ['staging', 'production']): ToolCallMessagePartProps {
  const args = { choices, question: 'Which deployment target?' }

  return {
    addResult: vi.fn(),
    args,
    argsText: JSON.stringify(args),
    isError: false,
    respondToApproval: vi.fn(),
    result: undefined,
    resume: vi.fn(),
    status: { type: 'running' },
    toolCallId: 'clarify-live',
    toolName: 'clarify',
    type: 'tool-call'
  }
}

function renderLiveClarify() {
  const request = vi.fn().mockResolvedValue({ ok: true })

  $activeSessionId.set('session-1')
  $gateway.set({ request } as never)
  markCatalogCurrent()
  setClarifyRequest({
    choices: ['staging', 'production'],
    question: 'Which deployment target?',
    requestId: 'request-1',
    sessionId: 'session-1'
  })
  renderClarify(<ClarifyTool {...liveClarifyProps()} />)

  return request
}

const managedSnapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: { control_state: 'running', control_version: 3, turn_id: 'turn-clarify' },
  artifacts: [],
  binding_id: 'binding-clarify',
  block: null,
  canonical_session_id: 'canonical-clarify',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 7,
  lifecycle: 'active',
  pending_approval: null,
  project_id: 'project-clarify',
  queue: [],
  transcript: [],
  transcript_revision: 2,
  version: 4,
  ...overrides
})

describe('readClarifyResult', () => {
  it('reads question + user_response from the tool JSON payload', () => {
    expect(
      readClarifyResult({
        question: 'Which target?',
        choices_offered: ['staging', 'prod'],
        user_response: 'staging'
      })
    ).toEqual({
      question: 'Which target?',
      answer: 'staging',
      error: undefined
    })
  })

  it('parses a JSON string result the same way as an object', () => {
    expect(
      readClarifyResult(
        JSON.stringify({
          question: 'Ship it?',
          user_response: 'yes'
        })
      )
    ).toEqual({
      question: 'Ship it?',
      answer: 'yes',
      error: undefined
    })
  })

  it('keeps an empty user_response so Skip can render as skipped', () => {
    expect(readClarifyResult({ question: 'Ok?', user_response: '' })).toEqual({
      question: 'Ok?',
      answer: '',
      error: undefined
    })
  })
})

describe('ClarifyTool settled view', () => {
  it('keeps the question and answer visible after the tool completes', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Which deployment target?', choices: ['staging', 'prod'] },
          {
            question: 'Which deployment target?',
            choices_offered: ['staging', 'prod'],
            user_response: 'staging'
          },
          'clarify-1'
        )}
      />
    )

    expect(screen.getByText('Which deployment target?')).toBeTruthy()
    expect(screen.getByText('staging')).toBeTruthy()
    expect(document.querySelector('[data-clarify-settled]')).toBeTruthy()
    expect(document.querySelector('[data-clarify-answer]')?.textContent).toBe('staging')
  })

  it('labels an empty response as Skipped', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Anything else?' },
          { question: 'Anything else?', user_response: '' },
          'clarify-2'
        )}
      />
    )

    expect(screen.getByText('Anything else?')).toBeTruthy()
    expect(screen.getByText('Skipped')).toBeTruthy()
  })

  it('keeps the original choices visible and clickable after a skip', async () => {
    const inserts: string[] = []

    const stop = onComposerInsertRequest(detail => {
      inserts.push(detail.text)
    })

    try {
      renderClarify(
        <ClarifyTool
          {...settledClarifyProps(
            { question: 'Which deployment target?', choices: ['staging', 'prod'] },
            { question: 'Which deployment target?', user_response: '' },
            'clarify-3'
          )}
        />
      )

      // The skip label renders AND the original options are still on screen.
      expect(screen.getByText('Skipped')).toBeTruthy()
      const group = document.querySelector('[data-clarify-late-choices]')
      expect(group).toBeTruthy()
      expect(screen.getByText('staging')).toBeTruthy()
      expect(screen.getByText('prod')).toBeTruthy()

      // Picking one drafts a quoted follow-up into the composer. The insert
      // bus defers dispatch by a macrotask, so flush one tick.
      fireEvent.click(screen.getByText('prod'))
      await new Promise(resolve => window.setTimeout(resolve, 0))

      expect(inserts).toHaveLength(1)
      expect(inserts[0]).toContain('Which deployment target?')
      expect(inserts[0]).toContain('prod')
    } finally {
      stop()
    }
  })

  it('does not render late choices on an answered clarify', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Which deployment target?', choices: ['staging', 'prod'] },
          { question: 'Which deployment target?', user_response: 'staging' },
          'clarify-4'
        )}
      />
    )

    expect(document.querySelector('[data-clarify-late-choices]')).toBeNull()
  })

  it('does not render late choices for a free-text (no-choice) skip', () => {
    renderClarify(
      <ClarifyTool
        {...settledClarifyProps(
          { question: 'Anything else?' },
          { question: 'Anything else?', user_response: '' },
          'clarify-5'
        )}
      />
    )

    expect(document.querySelector('[data-clarify-late-choices]')).toBeNull()
  })
})

describe('ClarifyTool keyboard navigation', () => {
  it('cycles through choices and Other with the arrow keys', () => {
    renderLiveClarify()

    const staging = screen.getByRole('button', { name: /staging/ })
    const production = screen.getByRole('button', { name: /production/ })
    const other = screen.getByPlaceholderText(/Other/)

    expect(staging.getAttribute('data-highlighted')).toBe('true')
    expect(staging.getAttribute('aria-current')).toBe('true')
    expect(staging.getAttribute('aria-keyshortcuts')).toBe('A 1')

    fireEvent.keyDown(window, { key: 'ArrowDown' })
    expect(production.getAttribute('data-highlighted')).toBe('true')

    fireEvent.keyDown(window, { key: 'ArrowDown' })
    expect(other.closest('label')?.getAttribute('data-highlighted')).toBe('true')
    expect(other.getAttribute('aria-current')).toBe('true')
    expect(other.getAttribute('aria-keyshortcuts')).toBe('C 3')

    fireEvent.keyDown(window, { key: 'ArrowDown' })
    expect(staging.getAttribute('data-highlighted')).toBe('true')

    fireEvent.keyDown(window, { key: 'ArrowUp' })
    expect(other.closest('label')?.getAttribute('data-highlighted')).toBe('true')
  })

  it('selects by number and confirms the answer with Enter', async () => {
    const request = renderLiveClarify()

    fireEvent.keyDown(window, { key: '2' })
    fireEvent.keyDown(window, { key: 'Enter' })

    await waitFor(() => {
      expect(request).toHaveBeenCalledWith('clarify.respond', {
        answer: 'production',
        request_id: 'request-1'
      })
    })
  })

  it('focuses Other when its number is pressed and leaves typing keys alone', () => {
    renderLiveClarify()

    const other = screen.getByPlaceholderText(/Other/)

    fireEvent.keyDown(window, { key: '3' })
    expect(document.activeElement).toBe(other)

    fireEvent.change(other, { target: { value: 'canary' } })
    fireEvent.keyDown(window, { key: 'ArrowUp' })
    expect(document.activeElement).toBe(other)
    expect((other as HTMLTextAreaElement).value).toBe('canary')
  })

  it('does not intercept keyboard events while an action button has focus', () => {
    const request = renderLiveClarify()
    const skip = screen.getByRole('button', { name: 'Skip' })

    skip.focus()

    expect(fireEvent.keyDown(window, { key: 'Enter' })).toBe(true)
    expect(fireEvent.keyDown(window, { key: 'ArrowDown' })).toBe(true)
    expect(request).not.toHaveBeenCalled()
  })
})

describe('ClarifyTool pending marker', () => {
  it('marks a live choices card so type-to-focus yields its shortcut keys', () => {
    renderLiveClarify()

    // The marker is what `composerFocusBlockedBySurface` keys off of, so the
    // global type-to-focus listener stands down and A/B/C… + 1-9 + Enter reach
    // the card instead of the composer.
    expect(document.querySelector('[data-clarify-choices]')).toBeTruthy()
  })

  it('does not mark a free-text (no-choice) pending card', () => {
    $activeSessionId.set('session-1')
    $gateway.set({ request: vi.fn().mockResolvedValue({ ok: true }) } as never)
    markCatalogCurrent()
    setClarifyRequest({
      choices: null,
      question: 'Anything else?',
      requestId: 'request-1',
      sessionId: 'session-1'
    })

    const args = { question: 'Anything else?' }
    renderClarify(
      <ClarifyTool
        addResult={vi.fn()}
        args={args}
        argsText={JSON.stringify(args)}
        isError={false}
        respondToApproval={vi.fn()}
        result={undefined}
        resume={vi.fn()}
        status={{ type: 'running' }}
        toolCallId="clarify-free"
        toolName="clarify"
        type="tool-call"
      />
    )

    // No shortcuts → nothing to protect → composer type-to-focus stays live.
    expect(document.querySelector('[data-clarify-choices]')).toBeNull()
  })
})

describe('ClarifyTool managed project boundary', () => {
  it('renders a typed unsupported block and never sends clarify.respond when live and durable ids differ', () => {
    const request = vi.fn().mockResolvedValue({ ok: true })
    $gateway.set({ request } as never)
    $sessions.set([
      {
        id: 'canonical-clarify',
        profile: 'default',
        project_id: 'project-clarify'
      } as SessionInfo
    ])
    $projects.set([{ id: 'project-clarify', managed: true } as ProjectInfo])
    $projectRuntimes.set({
      'project-clarify': { events: [], snapshot: managedSnapshot() }
    })
    setClarifyRequest({
      choices: ['staging', 'production'],
      question: 'Which deployment target?',
      requestId: 'legacy-request',
      sessionId: 'runtime-clarify'
    })

    renderClarifyForSurface(<ClarifyTool {...liveClarifyProps()} />, 'runtime-clarify', 'canonical-clarify')

    const status = screen.getByRole('status')
    expect(status.getAttribute('data-clarify-managed-block')).toBe('unsupported')
    expect(status.textContent).toMatch(/not supported.*managed project/i)
    expect(screen.queryByRole('button', { name: /Continue|Skip|staging|production/i })).toBeNull()
    expect(request).not.toHaveBeenCalled()
  })

  it('fails closed while a catalog-managed runtime is unavailable', () => {
    $sessions.set([
      {
        id: 'canonical-clarify',
        profile: 'default',
        project_id: 'project-clarify'
      } as SessionInfo
    ])
    $projects.set([{ id: 'project-clarify', managed: true } as ProjectInfo])
    $projectRuntimes.set({})

    renderClarifyForSurface(<ClarifyTool {...liveClarifyProps()} />, 'runtime-clarify', 'canonical-clarify')

    expect(screen.getByRole('status').getAttribute('data-clarify-managed-block')).toBe('unsupported')
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('fails closed during boot when the surface has no durable stored identity', () => {
    $sessions.set([])
    $projectRuntimes.set({})

    renderClarifyForSurface(<ClarifyTool {...liveClarifyProps()} />, 'runtime-unknown', null)

    expect(screen.getByRole('status').getAttribute('data-clarify-managed-block')).toBe('unsupported')
    expect(screen.queryByRole('button')).toBeNull()
  })
})
