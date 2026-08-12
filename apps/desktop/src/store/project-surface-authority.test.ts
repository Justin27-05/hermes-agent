import { describe, expect, it } from 'vitest'

import type { ProjectRuntimeSnapshot, SessionInfo } from '@/types/hermes'

import { type ManagedProjectSurfaceInput, resolveManagedProjectSurface } from './project-surface-authority'

const snapshot = (overrides: Partial<ProjectRuntimeSnapshot> = {}): ProjectRuntimeSnapshot => ({
  active_run: null,
  artifacts: [],
  binding_id: 'binding-a',
  block: null,
  canonical_session_id: 'stored-a',
  current_phase: 'implementation',
  delivery_status: { error_code: null, state: 'caught_up' },
  last_sequence: 1,
  lifecycle: 'active',
  pending_approval: null,
  project_id: 'project-a',
  queue: [],
  transcript: [],
  transcript_revision: 0,
  version: 1,
  ...overrides
})

const session = (overrides: Partial<SessionInfo> = {}): SessionInfo =>
  ({
    ended_at: null,
    id: 'stored-a',
    input_tokens: 0,
    is_active: false,
    last_active: 0,
    message_count: 0,
    model: null,
    output_tokens: 0,
    preview: null,
    profile: 'default',
    project_id: 'project-a',
    source: 'desktop',
    started_at: 0,
    title: null,
    tool_call_count: 0,
    ...overrides
  }) as SessionInfo

const input = (overrides: Partial<ManagedProjectSurfaceInput> = {}): ManagedProjectSurfaceInput => ({
  activeProfile: 'default',
  activeProjectId: null,
  catalogAuthority: { catalogGeneration: 2, contextGeneration: 2, profile: 'default' },
  projects: [{ id: 'project-a', managed: true } as never],
  runtimeAuthority: { requesterGeneration: 3, scope: 'default' },
  runtimes: { 'project-a': { events: [], snapshot: snapshot() } },
  runtimeSessionId: 'runtime-random',
  sessions: [session()],
  storedSessionId: 'stored-a',
  ...overrides
})

describe('resolveManagedProjectSurface', () => {
  it('keeps the durable canonical owner when the live runtime id is unrelated', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          runtimeSessionId: 'unrelated-runtime-id'
        })
      )
    ).toEqual({ snapshot: snapshot(), status: 'managed' })
  })

  it('treats a same-project live id owned by another canonical session as ambiguous', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          runtimeSessionId: 'stored-b',
          runtimes: {
            'project-a': { events: [], snapshot: snapshot() },
            'project-a-shadow': {
              events: [],
              snapshot: snapshot({
                binding_id: 'binding-b',
                canonical_session_id: 'stored-b'
              })
            }
          }
        })
      )
    ).toEqual({ status: 'ambiguous' })
  })

  it('does not let an explicit legacy stored row overrule a different live managed owner', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          runtimeSessionId: 'live-managed-session',
          runtimes: {
            'project-live': {
              events: [],
              snapshot: snapshot({
                canonical_session_id: 'live-managed-session',
                project_id: 'project-live'
              })
            }
          },
          sessions: [session({ project_id: null })]
        })
      )
    ).toEqual({ status: 'ambiguous' })
  })

  it('treats a live managed owner that conflicts with the active project as ambiguous', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProjectId: 'project-b',
          projects: [{ id: 'project-a', managed: true } as never, { id: 'project-b', managed: true } as never],
          runtimeSessionId: 'stored-a',
          sessions: [],
          storedSessionId: null
        })
      )
    ).toEqual({ status: 'ambiguous' })
  })

  it('keeps padded opaque session ids distinct instead of trimming into authority', () => {
    expect(resolveManagedProjectSurface(input({ storedSessionId: ' stored-a ' }))).toEqual({
      projectId: null,
      status: 'unavailable'
    })
  })

  it('fails a stored session closed while the catalog generation is not loaded', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          catalogAuthority: { catalogGeneration: null, contextGeneration: 2, profile: 'default' },
          runtimes: {},
          storedSessionId: 'stored-a'
        })
      )
    ).toEqual({ projectId: null, status: 'unavailable' })
  })

  it('fails a managed catalog row closed when its runtime is missing', () => {
    expect(resolveManagedProjectSurface(input({ runtimes: {} }))).toEqual({
      projectId: 'project-a',
      status: 'unavailable'
    })
  })

  it('accepts an explicit current unmanaged marker as conclusively legacy', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          projects: [{ id: 'project-a', managed: false } as never],
          runtimes: {}
        })
      )
    ).toEqual({ status: 'conclusively-legacy' })
  })

  it('does not accept an unmanaged marker from another profile', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProfile: 'profile-b',
          catalogAuthority: {
            catalogGeneration: 2,
            contextGeneration: 2,
            profile: 'profile-a'
          },
          projects: [{ id: 'project-a', managed: false } as never],
          runtimes: {},
          sessions: [session({ profile: 'profile-b' })]
        })
      )
    ).toEqual({ projectId: null, status: 'unavailable' })
  })

  it('accepts an explicit legacy stored row from the target profile without borrowing active-profile authority', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProfile: 'profile-a',
          catalogAuthority: {
            catalogGeneration: 2,
            contextGeneration: 2,
            profile: 'profile-a'
          },
          projects: [{ id: 'project-a', managed: true } as never],
          runtimes: {},
          sessions: [session({ profile: 'profile-b', project_id: null })],
          targetProfile: 'profile-b'
        })
      )
    ).toEqual({ status: 'conclusively-legacy' })
  })

  it('fails a project-owned stored row from the target profile closed without target-profile authority', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProfile: 'profile-a',
          catalogAuthority: {
            catalogGeneration: 2,
            contextGeneration: 2,
            profile: 'profile-a'
          },
          projects: [{ id: 'project-a', managed: false } as never],
          runtimes: {},
          sessions: [session({ profile: 'profile-b' })],
          targetProfile: 'profile-b'
        })
      )
    ).toEqual({ projectId: null, status: 'unavailable' })
  })

  it('accepts only an exact current target-profile runtime for a cross-profile stored row', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProfile: 'profile-a',
          runtimeAuthority: { requesterGeneration: 3, scope: 'profile-b' },
          sessions: [session({ profile: 'profile-b' })],
          targetProfile: 'profile-b'
        })
      )
    ).toEqual({ snapshot: snapshot(), status: 'managed' })
  })

  it.each([
    {
      expectedProjectId: null,
      label: 'project ownership is omitted',
      projects: [{ id: 'project-a', managed: true } as never],
      session: session({ project_id: undefined })
    },
    {
      expectedProjectId: 'project-a',
      label: 'the project managed marker is omitted',
      projects: [{ id: 'project-a' } as never],
      session: session()
    }
  ])('fails closed when $label', ({ expectedProjectId, projects, session: storedSession }) => {
    expect(
      resolveManagedProjectSurface(
        input({
          projects,
          runtimes: {},
          sessions: [storedSession]
        })
      )
    ).toEqual({ projectId: expectedProjectId, status: 'unavailable' })
  })

  it('accepts a current exact managed runtime without loaded catalog evidence', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          catalogAuthority: {
            catalogGeneration: null,
            contextGeneration: 2,
            profile: 'default'
          }
        })
      )
    ).toEqual({ snapshot: snapshot(), status: 'managed' })
  })

  it('treats a new global draft with no project evidence as conclusively legacy', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProjectId: null,
          runtimeSessionId: null,
          sessions: [],
          storedSessionId: null
        })
      )
    ).toEqual({ status: 'conclusively-legacy' })
  })

  it('resolves a new active-project draft from its current managed runtime', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProjectId: 'project-a',
          runtimeSessionId: null,
          sessions: [],
          storedSessionId: null
        })
      )
    ).toEqual({ snapshot: snapshot(), status: 'managed' })
  })

  it('fails a new active managed-project draft closed while its runtime is missing', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          activeProjectId: 'project-a',
          runtimeSessionId: null,
          runtimes: {},
          sessions: [],
          storedSessionId: null
        })
      )
    ).toEqual({ projectId: 'project-a', status: 'unavailable' })
  })

  it('rejects an exact runtime snapshot from a stale runtime scope', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          runtimeAuthority: { requesterGeneration: 3, scope: 'profile-b' }
        })
      )
    ).toEqual({ projectId: 'project-a', status: 'unavailable' })
  })

  it('treats duplicate exact and lineage session rows as ambiguous', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          runtimes: {},
          sessions: [
            session(),
            session({
              _lineage_root_id: 'stored-a',
              id: 'stored-a-tip'
            })
          ]
        })
      )
    ).toEqual({ status: 'ambiguous' })
  })

  it('keeps duplicate exact and lineage session rows ambiguous when an exact runtime exists', () => {
    expect(
      resolveManagedProjectSurface(
        input({
          sessions: [
            session(),
            session({
              _lineage_root_id: 'stored-a',
              id: 'stored-a-tip',
              project_id: 'project-conflict'
            })
          ]
        })
      )
    ).toEqual({ status: 'ambiguous' })
  })
})
