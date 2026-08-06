import { afterEach, describe, expect, it, vi } from 'vitest'

import type { ProjectRuntimeState } from '@/store/project-runtime'
import { $connection } from '@/store/session'
import type { ProjectInfo, SessionInfo, SessionMessage } from '@/types/hermes'

import {
  artifactImageSrc,
  artifactsForCurrentAuthority,
  collectArtifactsForProjectRuntimes,
  collectArtifactsForSession,
  legacyArtifactSessions
} from './artifact-utils'

function makeSession(overrides: Partial<SessionInfo> = {}): SessionInfo {
  return {
    ended_at: null,
    id: 'session-1',
    input_tokens: 0,
    is_active: false,
    last_active: 1000,
    message_count: 1,
    model: null,
    output_tokens: 0,
    preview: null,
    source: null,
    started_at: 1000,
    title: 'Session',
    tool_call_count: 0,
    ...overrides
  }
}

function makeProject(overrides: Partial<ProjectInfo> = {}): ProjectInfo {
  return {
    archived: false,
    board_slug: null,
    color: null,
    created_at: 1,
    description: null,
    folders: [{ added_at: 1, is_primary: true, label: null, path: '/srv/demo' }],
    icon: null,
    id: 'project-a',
    managed: true,
    name: 'Demo',
    primary_path: '/srv/demo',
    slug: 'demo',
    ...overrides
  }
}

describe('collectArtifactsForSession', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
    vi.clearAllMocks()
    $connection.set(null)
  })

  it('indexes plain https links from assistant text', () => {
    const artifacts = collectArtifactsForSession(makeSession(), [
      {
        content: 'Reference: https://example.com/docs/getting-started',
        role: 'assistant',
        timestamp: 2000
      }
    ])

    expect(artifacts).toHaveLength(1)
    expect(artifacts[0]).toMatchObject({
      href: 'https://example.com/docs/getting-started',
      kind: 'link',
      value: 'https://example.com/docs/getting-started'
    })
  })

  it('indexes http links present in tool JSON payloads', () => {
    const messages: SessionMessage[] = [
      {
        content: JSON.stringify({ source_url: 'https://example.com/changelog/latest' }),
        role: 'tool',
        timestamp: 3000
      }
    ]

    const artifacts = collectArtifactsForSession(makeSession({ id: 'session-2' }), messages)

    expect(artifacts).toHaveLength(1)
    expect(artifacts[0]).toMatchObject({
      href: 'https://example.com/changelog/latest',
      kind: 'link',
      value: 'https://example.com/changelog/latest'
    })
  })

  it('keeps the same session id and artifact value isolated by normalized profile', () => {
    const message: SessionMessage = {
      content: '/private/shared-result.pdf',
      role: 'assistant',
      timestamp: 3000
    }

    const [defaultArtifact] = collectArtifactsForSession(makeSession({ id: 'shared-session', profile: ' default ' }), [
      message
    ])

    const [workArtifact] = collectArtifactsForSession(makeSession({ id: 'shared-session', profile: 'work' }), [message])

    expect(defaultArtifact).toMatchObject({ profile: 'default', sessionId: 'shared-session' })
    expect(workArtifact).toMatchObject({ profile: 'work', sessionId: 'shared-session' })
    expect(defaultArtifact.id).not.toBe(workArtifact.id)
  })

  it('resolves remote image artifact thumbnails through the desktop fs bridge', async () => {
    const api = vi.fn(async ({ path }: { path: string }) => {
      if (path.startsWith('/api/fs/read-data-url?')) {
        return { dataUrl: 'data:image/jpeg;base64,cmVtb3Rl' }
      }

      throw new Error(`unexpected path ${path}`)
    })

    vi.stubGlobal('window', { hermesDesktop: { api } })
    $connection.set({ baseUrl: 'https://gw', mode: 'remote', token: 'secret' } as never)

    const path = '/Users/me/.hermes/skills/work-esab/references/images/manual-step03.jpeg'
    const downloadHref = `https://gw/api/files/download?path=${encodeURIComponent(path)}&token=secret`

    await expect(artifactImageSrc(path, downloadHref)).resolves.toBe('data:image/jpeg;base64,cmVtb3Rl')

    expect(api).toHaveBeenCalledWith({
      path: '/api/fs/read-data-url?path=%2FUsers%2Fme%2F.hermes%2Fskills%2Fwork-esab%2Freferences%2Fimages%2Fmanual-step03.jpeg'
    })
  })
})

describe('collectArtifactsForProjectRuntimes', () => {
  it('uses the canonical managed snapshot instead of a stale transcript and keeps local artifacts closed', () => {
    const runtimes: Record<string, ProjectRuntimeState> = {
      'project-a': {
        events: [],
        snapshot: {
          active_run: null,
          artifacts: [
            {
              artifact_id: 'artifact-local',
              presentation: {
                created_at: 2000,
                kind: 'file',
                label: 'report.pdf',
                open_target: null,
                sha256: 'a'.repeat(64),
                size_bytes: 42
              }
            },
            {
              artifact_id: 'artifact-link',
              presentation: {
                created_at: 3000,
                kind: 'link',
                label: 'release-notes',
                open_target: { href: 'https://example.com/releases/1', kind: 'external_url' },
                sha256: null,
                size_bytes: null
              }
            }
          ],
          binding_id: 'binding-a',
          block: null,
          canonical_session_id: 'managed-session',
          current_phase: 'working',
          delivery_status: { error_code: null, state: 'caught_up' },
          last_sequence: 2,
          lifecycle: 'active',
          pending_approval: null,
          project_id: 'project-a',
          queue: [],
          transcript: [
            {
              content: 'stale local path /private/should-not-appear.pdf',
              role: 'assistant',
              timestamp: 1000
            }
          ],
          transcript_revision: 1,
          version: 1
        }
      }
    }

    const artifacts = collectArtifactsForProjectRuntimes(
      runtimes,
      [makeSession({ id: 'managed-session', title: 'Managed project' })],
      'default'
    )

    expect(artifacts).toEqual([
      expect.objectContaining({
        href: null,
        kind: 'file',
        label: 'report.pdf',
        sha256: 'a'.repeat(64),
        sizeBytes: 42,
        source: 'canonical',
        timestamp: 2000,
        value: ''
      }),
      expect.objectContaining({
        href: 'https://example.com/releases/1',
        kind: 'link',
        label: 'release-notes',
        source: 'canonical',
        timestamp: 3000,
        value: 'https://example.com/releases/1'
      })
    ])
    expect(JSON.stringify(artifacts)).not.toContain('/private/should-not-appear.pdf')
  })

  it('never schedules a managed canonical session for transcript indexing', () => {
    const runtimes = {
      'project-a': {
        events: [],
        snapshot: { canonical_session_id: 'managed-session' }
      }
    }

    const sessions = [makeSession({ id: 'managed-session' }), makeSession({ id: 'legacy-session' })]

    expect(legacyArtifactSessions(sessions, runtimes, [])).toEqual([sessions[1]])
  })

  it('does not schedule transcript indexing for a managed project whose runtime has not loaded yet', () => {
    const managedSession = makeSession({
      id: 'managed-before-runtime',
      project_id: 'project-a'
    } as Partial<SessionInfo>)

    const projectTree = [
      {
        id: 'project-a',
        label: 'Demo',
        path: '/srv/demo',
        repos: [
          {
            groups: [
              {
                id: 'main',
                label: 'main',
                path: '/srv/demo',
                sessions: []
              }
            ],
            id: '/srv/demo',
            label: 'demo',
            path: '/srv/demo',
            sessionCount: 1
          }
        ],
        previewSessions: [
          makeSession({ id: 'newer-preview-1' }),
          makeSession({ id: 'newer-preview-2' }),
          makeSession({ id: 'newer-preview-3' })
        ],
        sessionCount: 1
      }
    ]

    expect(legacyArtifactSessions([managedSession], {}, [makeProject()], 'default', projectTree)).toEqual([])
  })

  it('uses the runtime profile when duplicate session ids have different titles', () => {
    const runtime = {
      'project-a': {
        events: [],
        snapshot: {
          ...({} as ProjectRuntimeState['snapshot']),
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'file' as const,
                label: 'result.pdf',
                open_target: null,
                sha256: null,
                size_bytes: null
              }
            }
          ],
          canonical_session_id: 'shared-session',
          project_id: 'project-a'
        }
      }
    }

    const sessions = [
      makeSession({ id: 'shared-session', profile: 'work', title: 'Work title' }),
      makeSession({ id: 'shared-session', profile: 'default', title: 'Default title' })
    ]

    expect(collectArtifactsForProjectRuntimes(runtime, sessions, 'work')).toEqual([
      expect.objectContaining({
        profile: 'work',
        sessionId: 'shared-session',
        sessionTitle: 'Work title'
      })
    ])
  })

  it('preserves legacy indexing for an explicitly unmanaged session in another profile', () => {
    const otherProfileSession = makeSession({
      cwd: '/srv/demo',
      git_repo_root: '/srv/demo',
      id: 'other-profile-session',
      profile: 'other'
    })

    expect(legacyArtifactSessions([otherProfileSession], {}, [makeProject()], 'default')).toEqual([otherProfileSession])
  })

  it('fails closed for a project-bound session from another profile without that profile authority', () => {
    const foreignManagedSession = makeSession({
      id: 'foreign-managed-session',
      profile: 'work',
      project_id: 'project-a'
    })

    expect(
      legacyArtifactSessions([foreignManagedSession], {}, [makeProject({ id: 'project-a', managed: false })], 'default')
    ).toEqual([])
  })

  it('treats a non-empty project binding as opaque and fails closed when it is padded or malformed', () => {
    const padded = makeSession({ id: 'padded', project_id: ' project-a ' })
    const whitespaceOnly = makeSession({ id: 'whitespace', project_id: '   ' })

    expect(legacyArtifactSessions([padded, whitespaceOnly], {}, [makeProject({ managed: false })], 'default')).toEqual(
      []
    )
  })

  it('prunes a rendered legacy path immediately when its project becomes managed while refresh is pending', () => {
    const session = makeSession({
      cwd: '/srv/demo',
      git_repo_root: '/srv/demo',
      id: 'transition-session',
      project_id: 'project-a'
    })

    const [legacyArtifact] = collectArtifactsForSession(session, [
      { content: '/private/stale-result.pdf', role: 'assistant', timestamp: 4 }
    ])

    let finishRefresh: (() => void) | undefined

    const pendingRefresh = new Promise<void>(resolve => {
      finishRefresh = resolve
    })

    expect(
      artifactsForCurrentAuthority([legacyArtifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject({ managed: false })],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([legacyArtifact])
    expect(
      artifactsForCurrentAuthority([legacyArtifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject({ managed: true })],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([])
    expect(pendingRefresh).toBeInstanceOf(Promise)
    finishRefresh?.()
  })

  it('cannot retain an artifact row across a profile or runtime authority transition', () => {
    const session = makeSession({ id: 'managed-session' })

    const [canonicalArtifact] = collectArtifactsForProjectRuntimes(
      {
        'project-a': {
          events: [],
          snapshot: {
            ...({} as ProjectRuntimeState['snapshot']),
            artifacts: [
              {
                artifact_id: 'artifact-a',
                presentation: {
                  created_at: 1,
                  kind: 'file',
                  label: 'result.pdf',
                  open_target: null,
                  sha256: null,
                  size_bytes: null
                }
              }
            ],
            canonical_session_id: session.id,
            project_id: 'project-a'
          }
        }
      },
      [session],
      'default'
    )

    expect(
      artifactsForCurrentAuthority([canonicalArtifact], {
        currentScope: 'profile-b',
        loadedScope: 'profile-a',
        projects: [],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'profile-a',
        runtimeScope: 'profile-a',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([])
    expect(
      artifactsForCurrentAuthority([canonicalArtifact], {
        currentScope: 'profile-a',
        loadedScope: 'profile-a',
        projects: [],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'profile-a',
        runtimeScope: 'profile-a',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([])
  })

  it('does not let another profile with the same session id validate a managed transcript row', () => {
    const managedSession = makeSession({
      id: 'shared-session',
      profile: 'default',
      project_id: 'project-a',
      title: 'Managed default'
    } as Partial<SessionInfo>)

    const workSession = makeSession({
      id: 'shared-session',
      profile: 'work',
      title: 'Unmanaged work'
    })

    const [managedArtifact] = collectArtifactsForSession(managedSession, [
      { content: '/private/managed-secret.pdf', role: 'assistant', timestamp: 2 }
    ])

    const [workArtifact] = collectArtifactsForSession(workSession, [
      { content: 'https://example.com/work-result.pdf', role: 'assistant', timestamp: 1 }
    ])

    expect(
      artifactsForCurrentAuthority([managedArtifact, workArtifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject()],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: {},
        sessions: [managedSession, workSession]
      })
    ).toEqual([workArtifact])
  })

  it('does not present canonical artifacts from a stale runtime profile', () => {
    const session = makeSession({ id: 'managed-session', profile: 'default' })

    const runtime = {
      'project-a': {
        events: [],
        snapshot: {
          ...({} as ProjectRuntimeState['snapshot']),
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'file' as const,
                label: 'result.pdf',
                open_target: null,
                sha256: null,
                size_bytes: null
              }
            }
          ],
          binding_id: 'binding-a',
          canonical_session_id: session.id,
          project_id: 'project-a'
        }
      }
    }

    expect(
      artifactsForCurrentAuthority([], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject()],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'work',
        runtimes: runtime,
        sessions: [session]
      })
    ).toEqual([])
  })

  it('does not let a stale runtime profile prune a current-profile legacy row with the same session id', () => {
    const session = makeSession({ id: 'shared-session', profile: 'default' })

    const [artifact] = collectArtifactsForSession(session, [
      { content: 'https://example.com/default-result.pdf', role: 'assistant', timestamp: 1 }
    ])

    const staleRuntime = {
      'project-work': {
        events: [],
        snapshot: {
          ...({} as ProjectRuntimeState['snapshot']),
          artifacts: [],
          canonical_session_id: session.id,
          project_id: 'project-work'
        }
      }
    }

    expect(
      artifactsForCurrentAuthority([artifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'work',
        runtimes: staleRuntime,
        sessions: [session]
      })
    ).toEqual([artifact])
  })

  it('fails closed for exact project membership while the project catalog belongs to another profile', () => {
    const session = makeSession({
      id: 'current-session',
      profile: 'default',
      project_id: 'shared-project'
    })

    const [artifact] = collectArtifactsForSession(session, [
      { content: '/private/current-project.pdf', role: 'assistant', timestamp: 1 }
    ])

    expect(
      artifactsForCurrentAuthority([artifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject({ id: 'shared-project', managed: false })],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'work',
        runtimeScope: 'default',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([])
  })

  it('fails closed when an exact project binding is absent from the current project catalog', () => {
    const session = makeSession({
      id: 'unknown-project-session',
      profile: 'default',
      project_id: 'missing-project'
    })

    expect(legacyArtifactSessions([session], {}, [], 'default')).toEqual([])
  })

  it('fails closed when an equal-profile project catalog belongs to an older context generation', () => {
    const session = makeSession({
      id: 'aba-session',
      profile: 'default',
      project_id: 'project-a'
    })

    const [artifact] = collectArtifactsForSession(session, [
      { content: '/private/aba-secret.pdf', role: 'assistant', timestamp: 1 }
    ])

    expect(
      artifactsForCurrentAuthority([artifact], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject({ managed: false })],
        projectsContextGeneration: 3,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: {},
        sessions: [session]
      })
    ).toEqual([])
  })

  it('changes canonical identity when the canonical session and binding change under equal project and artifact ids', () => {
    const runtimeA = {
      'project-a': {
        events: [],
        snapshot: {
          ...({} as ProjectRuntimeState['snapshot']),
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'file' as const,
                label: 'result.pdf',
                open_target: null,
                sha256: null,
                size_bytes: null
              }
            }
          ],
          binding_id: 'binding-a',
          canonical_session_id: 'session-a',
          project_id: 'project-a'
        }
      }
    }

    const runtimeB = {
      'project-a': {
        events: [],
        snapshot: {
          ...runtimeA['project-a'].snapshot,
          binding_id: 'binding-b',
          canonical_session_id: 'session-b'
        }
      }
    }

    const sessions = [makeSession({ id: 'session-a' }), makeSession({ id: 'session-b' })]
    const [artifactA] = collectArtifactsForProjectRuntimes(runtimeA, sessions, 'default')
    const [artifactB] = collectArtifactsForProjectRuntimes(runtimeB, sessions, 'default')

    expect(artifactB.id).not.toBe(artifactA.id)
    expect(
      artifactsForCurrentAuthority([artifactA], {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject()],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: runtimeB,
        sessions
      })
    ).toEqual([artifactB])
  })

  it('replaces canonical rows synchronously when the same runtime snapshot changes its artifact set', () => {
    const session = makeSession({ id: 'managed-session' })

    const runtimeA = {
      'project-a': {
        events: [],
        snapshot: {
          ...({} as ProjectRuntimeState['snapshot']),
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'file' as const,
                label: 'old.pdf',
                open_target: null,
                sha256: null,
                size_bytes: null
              }
            }
          ],
          binding_id: 'binding-a',
          canonical_session_id: session.id,
          project_id: 'project-a'
        }
      }
    }

    const runtimeB = {
      'project-a': {
        events: [],
        snapshot: {
          ...runtimeA['project-a'].snapshot,
          artifacts: [
            {
              artifact_id: 'artifact-b',
              presentation: {
                created_at: 2,
                kind: 'file' as const,
                label: 'new.pdf',
                open_target: null,
                sha256: 'b'.repeat(64),
                size_bytes: 10
              }
            }
          ]
        }
      }
    }

    const staleRows = collectArtifactsForProjectRuntimes(runtimeA, [session], 'default')

    expect(
      artifactsForCurrentAuthority(staleRows, {
        currentScope: 'default',
        loadedScope: 'default',
        projects: [makeProject()],
        projectsContextGeneration: 1,
        projectsGeneration: 1,
        projectsScope: 'default',
        runtimeScope: 'default',
        runtimes: runtimeB,
        sessions: [session]
      })
    ).toEqual([
      expect.objectContaining({
        id: '["canonical","default","project-a","managed-session","binding-a","artifact-b"]',
        label: 'new.pdf',
        sha256: 'b'.repeat(64),
        sizeBytes: 10
      })
    ])
  })
})
