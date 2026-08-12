// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import type * as ReactRouterDom from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type * as HermesApi from '@/hermes'
import type * as ProfileStore from '@/store/profile'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes } from '@/store/project-runtime'
import { $projects, $projectTree } from '@/store/projects'
import type { SessionInfo } from '@/types/hermes'

import { sessionRoute } from '../routes'

const getSessionMessages = vi.fn()
const listAllProfileSessions = vi.fn()
const ensureGatewayProfile = vi.fn()
const navigate = vi.fn()
const notifyError = vi.fn()
let profileAuthorityGeneration = 0
let profileAuthorityTarget = 'default'

vi.mock('@/hermes', async importOriginal => ({
  ...(await importOriginal<typeof HermesApi>()),
  getSessionMessages: (sessionId: string, profile?: string) => getSessionMessages(sessionId, profile),
  listAllProfileSessions: () => listAllProfileSessions()
}))

vi.mock('@/store/profile', async importOriginal => ({
  ...(await importOriginal<typeof ProfileStore>()),
  ensureGatewayProfile: (profile: string) => {
    profileAuthorityGeneration += 1
    profileAuthorityTarget = profile
    const generation = profileAuthorityGeneration

    return ensureGatewayProfile(profile).then(() => {
      if (generation === profileAuthorityGeneration) {
        $activeGatewayProfile.set(profile)
      }
    })
  },
  gatewayProfileAuthorityGeneration: () => profileAuthorityGeneration,
  gatewayProfileAuthorityTarget: () => profileAuthorityTarget
}))

vi.mock('react-router-dom', async importOriginal => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useNavigate: () => navigate
}))

vi.mock('@/store/notifications', () => ({
  notifyError: (...args: unknown[]) => notifyError(...args)
}))

function session(profile: string, title: string): SessionInfo {
  return {
    ended_at: null,
    id: 'shared-session',
    input_tokens: 0,
    is_active: false,
    last_active: 1000,
    message_count: 1,
    model: null,
    output_tokens: 0,
    preview: null,
    profile,
    source: null,
    started_at: 1000,
    title,
    tool_call_count: 0
  }
}

async function renderArtifacts(sessions: SessionInfo[]) {
  listAllProfileSessions.mockResolvedValue({ sessions })
  getSessionMessages.mockImplementation(async (_sessionId: string, profile?: string) => ({
    messages: [
      {
        content: `https://example.com/${profile || 'default'}-result.pdf`,
        role: 'assistant',
        timestamp: profile === 'work' ? 2000 : 1000
      }
    ],
    session_id: 'shared-session'
  }))
  const { ArtifactsView } = await import('./index')

  await act(async () => {
    render(
      <MemoryRouter initialEntries={['/artifacts']}>
        <ArtifactsView />
      </MemoryRouter>
    )
  })
}

function chatButton(title: string): HTMLButtonElement {
  const label = screen.getByText(title)
  const button = label.closest('button')

  if (!(button instanceof HTMLButtonElement)) {
    throw new Error(`No chat button for ${title}`)
  }

  return button
}

beforeEach(() => {
  profileAuthorityGeneration = 0
  profileAuthorityTarget = 'default'
  $activeGatewayProfile.set('default')
  $projectRuntimes.set({})
  $projects.set([])
  $projectTree.set([])
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('ArtifactsView profile-aware chat navigation', () => {
  it('activates the artifact profile before navigating to its session', async () => {
    let releaseProfile: (() => void) | undefined
    ensureGatewayProfile.mockImplementation(
      () =>
        new Promise<void>(resolve => {
          releaseProfile = resolve
        })
    )
    await renderArtifacts([session('work', 'Work session')])
    const button = await waitFor(() => chatButton('Work session'))

    fireEvent.click(button)

    expect(ensureGatewayProfile).toHaveBeenCalledWith('work')
    expect(navigate).not.toHaveBeenCalled()

    await act(async () => {
      releaseProfile?.()
    })

    expect(navigate).toHaveBeenCalledWith(sessionRoute('shared-session'))
  })

  it('does not let a slower profile activation win over a newer artifact click', async () => {
    let releaseDefault: (() => void) | undefined
    ensureGatewayProfile.mockImplementation(profile =>
      profile === 'default'
        ? new Promise<void>(resolve => {
            releaseDefault = resolve
          })
        : Promise.resolve()
    )
    await renderArtifacts([session('default', 'Default session'), session('work', 'Work session')])
    const defaultButton = await waitFor(() => chatButton('Default session'))
    const workButton = chatButton('Work session')

    fireEvent.click(defaultButton)
    fireEvent.click(workButton)

    await waitFor(() => expect(navigate).toHaveBeenCalledTimes(1))

    await act(async () => {
      releaseDefault?.()
    })

    expect(ensureGatewayProfile.mock.calls.map(([profile]) => profile)).toEqual(['default', 'work'])
    expect(navigate).toHaveBeenCalledTimes(1)
    expect(navigate).toHaveBeenCalledWith(sessionRoute('shared-session'))
  })

  it('invalidates a pending artifact navigation when an external profile activation supersedes it', async () => {
    let releaseWork: (() => void) | undefined

    ensureGatewayProfile.mockImplementation(
      () =>
        new Promise<void>(resolve => {
          releaseWork = resolve
        })
    )
    await renderArtifacts([session('work', 'Work session')])
    const workButton = await waitFor(() => chatButton('Work session'))

    fireEvent.click(workButton)
    act(() => {
      profileAuthorityGeneration += 1
      profileAuthorityTarget = 'external'
      $activeGatewayProfile.set('external')
    })

    await act(async () => {
      releaseWork?.()
    })

    expect(navigate).not.toHaveBeenCalled()
  })

  it('does not surface a late activation error after an external profile switch supersedes the click', async () => {
    let rejectWork: ((error: Error) => void) | undefined

    ensureGatewayProfile.mockImplementation(
      () =>
        new Promise<void>((_resolve, reject) => {
          rejectWork = reject
        })
    )
    await renderArtifacts([session('work', 'Work session')])
    const workButton = await waitFor(() => chatButton('Work session'))

    fireEvent.click(workButton)
    act(() => {
      profileAuthorityGeneration += 1
      profileAuthorityTarget = 'external'
      $activeGatewayProfile.set('external')
    })

    await act(async () => {
      rejectWork?.(new Error('late work activation failed'))
    })

    expect(navigate).not.toHaveBeenCalled()
    expect(notifyError).not.toHaveBeenCalled()
  })

  it('surfaces an activation error while that artifact click remains authoritative', async () => {
    ensureGatewayProfile.mockRejectedValue(new Error('current work activation failed'))
    await renderArtifacts([session('work', 'Work session')])
    const workButton = await waitFor(() => chatButton('Work session'))

    fireEvent.click(workButton)

    await waitFor(() => expect(notifyError).toHaveBeenCalledWith(expect.any(Error), 'Open failed'))
    expect(navigate).not.toHaveBeenCalled()
  })
})
