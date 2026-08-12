import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import { setSessions } from '@/store/session'
import type { SessionInfo } from '@/types/hermes'

import { isManagedProjectComposerTarget } from './managed-project-target'

const storedSession = (projectId: null | string): SessionInfo =>
  ({
    ended_at: null,
    id: 'stored-profile-b',
    input_tokens: 0,
    is_active: false,
    last_active: 0,
    message_count: 0,
    model: null,
    output_tokens: 0,
    preview: null,
    profile: 'profile-b',
    project_id: projectId,
    source: 'desktop',
    started_at: 0,
    title: null,
    tool_call_count: 0
  }) as SessionInfo

describe('isManagedProjectComposerTarget', () => {
  beforeEach(() => {
    $activeGatewayProfile.set('profile-a')
    $projectCatalogAuthority.set({
      catalogGeneration: 1,
      contextGeneration: 1,
      profile: 'profile-a'
    })
    $projects.set([{ id: 'project-a', managed: true } as never])
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-a'
    )
    $projectRuntimes.set({})
  })

  afterEach(() => {
    setSessions([])
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
  })

  it('keeps cross-profile explicit legacy sessions on legacy composer controls', () => {
    setSessions([storedSession(null)])

    expect(isManagedProjectComposerTarget('runtime-profile-b', 'stored-profile-b')).toBe(false)
  })

  it('keeps cross-profile project-owned sessions blocked without target-profile authority', () => {
    setSessions([storedSession('project-b')])

    expect(isManagedProjectComposerTarget('runtime-profile-b', 'stored-profile-b')).toBe(true)
  })
})
