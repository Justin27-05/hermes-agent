import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { deleteSession, setSessionArchived } from '@/hermes'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import { setSessions } from '@/store/session'
import type { SessionInfo } from '@/types/hermes'

import { deleteArchivedSessionWithAuthority, unarchiveSessionWithAuthority } from './sessions-settings'

vi.mock('@/hermes', async importOriginal => ({
  ...(await importOriginal<Record<string, unknown>>()),
  deleteSession: vi.fn(),
  setSessionArchived: vi.fn()
}))

const row = (overrides: Partial<SessionInfo> = {}): SessionInfo =>
  ({
    ended_at: null,
    id: 'stored-managed',
    input_tokens: 0,
    is_active: false,
    last_active: 1,
    message_count: 1,
    model: null,
    output_tokens: 0,
    preview: null,
    profile: 'default',
    project_id: 'project-managed',
    source: 'desktop',
    started_at: 1,
    title: 'managed',
    tool_call_count: 0,
    ...overrides
  }) as SessionInfo

describe('archived session authority', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    setSessions([])
    $projects.set([{ id: 'project-managed', managed: true } as never])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'default' })
  })

  afterEach(() => {
    setSessions([])
    $projects.set([])
    resetProjectRuntimeStore()
  })

  it('blocks restore and permanent delete for an exact managed archived row', async () => {
    const managed = row()

    $projectRuntimes.set({
      'project-managed': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [],
          binding_id: 'binding-managed',
          block: null,
          canonical_session_id: managed.id,
          current_phase: 'implementation',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 1,
          lifecycle: 'completed',
          pending_approval: null,
          project_id: 'project-managed',
          queue: [],
          transcript: [],
          transcript_revision: 1,
          version: 1
        }
      }
    })

    await expect(unarchiveSessionWithAuthority(managed)).resolves.toBe(false)
    await expect(deleteArchivedSessionWithAuthority(managed)).resolves.toBe(false)
    expect(setSessionArchived).not.toHaveBeenCalled()
    expect(deleteSession).not.toHaveBeenCalled()
  })
})
