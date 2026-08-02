import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  $projectRuntimes,
  applyProjectEvent,
  configureProjectRuntimeRequester,
  isProjectRuntimeEvent,
  isProjectRuntimeSnapshot,
  projectRuntimeAuthority,
  type ProjectRuntimeEvent,
  type ProjectRuntimeSnapshot,
  resetProjectRuntimeStore,
  syncProjectRuntime
} from './project-runtime'

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
  transcript_revision: 0,
  version: 1,
  ...overrides
})

const event = (sequence: number, overrides: Partial<ProjectRuntimeEvent> = {}): ProjectRuntimeEvent => ({
  created_at: '2026-07-30T10:00:00Z',
  event_id: `event-${sequence}`,
  kind: 'turn.queued',
  payload: {},
  project_id: 'project-a',
  sequence,
  turn_id: null,
  ...overrides
})

describe('project runtime store', () => {
  beforeEach(() => {
    resetProjectRuntimeStore()
    configureProjectRuntimeRequester(undefined)
  })

  it('exposes normalized requester authority and advances it on replacement or reset', () => {
    const firstRequester = vi.fn(async () => undefined)

    configureProjectRuntimeRequester(firstRequester, ' profile-a ')
    const first = projectRuntimeAuthority()

    expect(first).toEqual({
      requesterGeneration: expect.any(Number),
      scope: 'profile-a'
    })

    configureProjectRuntimeRequester(firstRequester, 'profile-a')
    expect(projectRuntimeAuthority()).toEqual(first)

    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-a'
    )
    const replaced = projectRuntimeAuthority()

    expect(replaced).toEqual({
      requesterGeneration: first.requesterGeneration + 1,
      scope: 'profile-a'
    })

    resetProjectRuntimeStore()
    expect(projectRuntimeAuthority()).toEqual({
      requesterGeneration: replaced.requesterGeneration + 1,
      scope: 'profile-a'
    })
  })

  it('replaces a project atomically from one valid immutable snapshot', async () => {
    const first = snapshot({ canonical_session_id: 'old', last_sequence: 1 })
    const second = snapshot({ canonical_session_id: 'new', last_sequence: 4 })

    const request = vi.fn(async (method: string) => {
      if (method === 'project.runtime.events') {
        return { after_sequence: 1, events: [], last_sequence: 1, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return second
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 4, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })

    configureProjectRuntimeRequester(request)

    $projectRuntimes.set({ 'project-a': { events: [event(1)], snapshot: first } })
    await syncProjectRuntime('project-a')

    expect($projectRuntimes.get()['project-a']).toEqual({ events: [], snapshot: second })
    expect(Object.isFrozen($projectRuntimes.get()['project-a'].snapshot)).toBe(true)
    expect(request).toHaveBeenCalledWith('project.runtime.snapshot', { project_id: 'project-a' })
  })

  it('applies exactly the next canonical event', () => {
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: snapshot() } })

    expect(applyProjectEvent(event(2))).toBe('applied')
    expect($projectRuntimes.get()['project-a'].snapshot.last_sequence).toBe(2)
    expect($projectRuntimes.get()['project-a'].events).toEqual([event(2)])
  })

  it('keeps duplicate and older events out of the transcript of applied events', () => {
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: snapshot({ last_sequence: 2 }) } })

    expect(applyProjectEvent(event(2))).toBe('stale')
    expect(applyProjectEvent(event(1))).toBe('stale')
    expect($projectRuntimes.get()['project-a'].events).toEqual([])
  })

  it('leaves state untouched for a sequence gap', () => {
    const current = { events: [], snapshot: snapshot({ last_sequence: 1 }) }
    $projectRuntimes.set({ 'project-a': current })

    expect(applyProjectEvent(event(3))).toBe('gap')
    expect($projectRuntimes.get()['project-a']).toEqual(current)
  })

  it('replays from the persisted cursor in bounded pages and acknowledges only the reached cursor', async () => {
    const request = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.events') {
        if (params?.after_sequence === 1) {
          return { after_sequence: 1, events: [event(2)], last_sequence: 3, project_id: 'project-a' }
        }

        return { after_sequence: 2, events: [event(3)], last_sequence: 3, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return snapshot({ last_sequence: 3 })
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 3, project_id: 'project-a' }
      }

      throw new Error(`unexpected ${method}`)
    })

    configureProjectRuntimeRequester(request)
    $projectRuntimes.set({ 'project-a': { events: [event(1)], snapshot: snapshot() } })

    await syncProjectRuntime('project-a')

    expect($projectRuntimes.get()['project-a']).toEqual({ events: [], snapshot: snapshot({ last_sequence: 3 }) })
    expect(request).toHaveBeenCalledWith('project.runtime.events', {
      after_sequence: 1,
      limit: 100,
      project_id: 'project-a'
    })
    expect(request).toHaveBeenCalledWith('project.runtime.ack', {
      binding_id: 'binding-a',
      cursor: 3,
      project_id: 'project-a'
    })
  })

  it('recovers a replay gap with one authoritative snapshot', async () => {
    const recovered = snapshot({ last_sequence: 3 })

    const request = vi.fn(async (method: string) => {
      if (method === 'project.runtime.events') {
        return { after_sequence: 1, events: [event(3)], last_sequence: 3, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return recovered
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 3, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })

    configureProjectRuntimeRequester(request)
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: snapshot() } })

    await syncProjectRuntime('project-a')

    expect($projectRuntimes.get()['project-a']).toEqual({ events: [], snapshot: recovered })
  })

  it('rejects malformed authoritative values without partial mutation', async () => {
    const current = { events: [], snapshot: snapshot() }
    configureProjectRuntimeRequester(async () => ({ ...snapshot(), unexpected: true }))
    $projectRuntimes.set({ 'project-a': current })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('invalid project runtime snapshot')
    expect($projectRuntimes.get()['project-a']).toEqual(current)
  })

  it('rejects a malformed acknowledgement receipt', async () => {
    configureProjectRuntimeRequester(async method => {
      if (method === 'project.runtime.snapshot') {
        return snapshot()
      }

      if (method === 'project.runtime.ack') {
        return { cursor: 0, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('invalid project runtime acknowledgement')
  })

  it('fails closed on malformed optional transcript data and unsafe artifact presentation', () => {
    const malformedTranscript = snapshot({
      transcript: [{ content: 'hello', role: 'user', text: () => 'not JSON' }]
    })

    const deepArtifact = snapshot({
      artifacts: [
        {
          artifact_id: 'artifact-a',
          presentation: {
            created_at: 1,
            kind: 'file',
            label: 'C:\\secret.txt',
            open_target: null,
            sha256: null,
            size_bytes: null
          }
        }
      ]
    })

    const malformedDigest = snapshot({
      artifacts: [
        {
          artifact_id: 'artifact-a',
          presentation: {
            created_at: 1,
            kind: 'file',
            label: 'report.pdf',
            open_target: { href: 'https://example.test/report.pdf', kind: 'external_url' },
            sha256: 'not-a-digest',
            size_bytes: 1
          }
        }
      ]
    })

    expect(isProjectRuntimeSnapshot(malformedTranscript)).toBe(false)
    expect(isProjectRuntimeSnapshot(deepArtifact)).toBe(false)
    expect(isProjectRuntimeSnapshot(malformedDigest)).toBe(false)

    for (const href of [
      'https://example.test/report.pdf?access_token%3Dsecret',
      'https://example.test/report.pdf?access_token%253Dsecret',
      'https://example.test/report.pdf?%61ccess_token=secret',
      'https://example.test/report.pdf?%2561ccess_token=secret',
      'https://example.test/report.pdf?note=%7F',
      'https://example.test/report.pdf?note=%C2%85',
      'https://example.test/report.pdf?no%E2%80%8Bte=1'
    ]) {
      expect(
        isProjectRuntimeSnapshot(
          snapshot({
            artifacts: [
              {
                artifact_id: 'artifact-a',
                presentation: {
                  created_at: 1,
                  kind: 'link',
                  label: 'report.pdf',
                  open_target: { href, kind: 'external_url' },
                  sha256: null,
                  size_bytes: null
                }
              }
            ]
          })
        )
      ).toBe(false)
    }

    expect(
      isProjectRuntimeSnapshot(
        snapshot({
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'link',
                label: 'report.pdf',
                open_target: { href: 'https://example.test/report.pdf#access_token=secret', kind: 'external_url' },
                sha256: null,
                size_bytes: null
              }
            }
          ]
        })
      )
    ).toBe(false)

    for (const href of [
      'https://example.test/report.pdf#access_token%3Dsecret',
      'https://example.test/report.pdf#%61ccess_token%3Dsecret',
      'https://example.test/report.pdf#%61ccess_token=secret',
      'https://example.test/report.pdf#%2561ccess_token=secret',
      'https://example.test/report.pdf#access_token%253Dsecret',
      'https://example.test/report.pdf#access_token%25253Dsecret',
      'https://example.test/report.pdf#access_token%',
      'https://example.test/report.pdf#note%00value',
      'https://example.test/report.pdf#note%E2%80%83value',
      'https://example.test/report.pdf#note\u0085value',
      'https://example.test/report.pdf#note\u200Bvalue',
      `https://example.test/report.pdf#${'a'.repeat(4097)}`
    ]) {
      expect(
        isProjectRuntimeSnapshot(
          snapshot({
            artifacts: [
              {
                artifact_id: 'artifact-a',
                presentation: {
                  created_at: 1,
                  kind: 'link',
                  label: 'report.pdf',
                  open_target: { href, kind: 'external_url' },
                  sha256: null,
                  size_bytes: null
                }
              }
            ]
          })
        )
      ).toBe(false)
    }

    expect(
      isProjectRuntimeSnapshot(
        snapshot({
          artifacts: [
            {
              artifact_id: 'artifact-a',
              presentation: {
                created_at: 1,
                kind: 'link',
                label: 'report.pdf',
                open_target: { href: 'https://example.test/report.pdf#authentication', kind: 'external_url' },
                sha256: null,
                size_bytes: null
              }
            }
          ]
        })
      )
    ).toBe(true)

    for (const href of [
      'http://localhost/report.pdf',
      'http://preview.localhost/report.pdf',
      'http://local/report.pdf',
      'http://printer.local/report.pdf',
      'http://home.arpa/report.pdf',
      'http://router.home.arpa/report.pdf',
      'http://127。0。0。1/report.pdf',
      'http://１２７．０．０．１/report.pdf',
      'http://ⓛⓞⓒⓐⓛⓗⓞⓢⓣ/report.pdf',
      'http://127.1/report.pdf',
      'http://2130706433/report.pdf',
      'http://0x7f000001/report.pdf',
      'http://0177.0.0.1/report.pdf',
      'http://127.0.0.1/report.pdf',
      'http://10.0.0.1/report.pdf',
      'http://172.16.0.1/report.pdf',
      'http://192.168.0.1/report.pdf',
      'http://169.254.1.1/report.pdf',
      'http://0.0.0.0/report.pdf',
      'http://192.0.0.1/report.pdf',
      'http://192.0.2.1/report.pdf',
      'http://198.18.0.1/report.pdf',
      'http://198.51.100.1/report.pdf',
      'http://203.0.113.1/report.pdf',
      'http://224.0.0.1/report.pdf',
      'http://240.0.0.1/report.pdf',
      'http://[::1]/report.pdf',
      'http://[::ffff:127.0.0.1]/report.pdf',
      'http://[::ffff:10.0.0.1]/report.pdf',
      'http://[::ffff:192.0.2.1]/report.pdf',
      'http://[::ffff:224.0.0.1]/report.pdf',
      'http://[64:ff9b:1::1]/report.pdf',
      'http://[100::1]/report.pdf',
      'http://[2001::1]/report.pdf',
      'http://[2001:db8::1]/report.pdf',
      'http://[2002::1]/report.pdf',
      'http://[3fff::1]/report.pdf',
      'http://[3fff:0fff::1]/report.pdf',
      'http://[fc00::1]/report.pdf',
      'http://[fe80::1]/report.pdf',
      'http://[fec0::1]/report.pdf',
      'http://[ff02::1]/report.pdf',
      'http://[::]/report.pdf'
    ]) {
      expect(
        isProjectRuntimeSnapshot(
          snapshot({
            artifacts: [
              {
                artifact_id: 'artifact-a',
                presentation: {
                  created_at: 1,
                  kind: 'link',
                  label: 'report.pdf',
                  open_target: { href, kind: 'external_url' },
                  sha256: null,
                  size_bytes: null
                }
              }
            ]
          })
        )
      ).toBe(false)
    }

    for (const href of [
      'https://8.8.8.8/report.pdf',
      'https://[2606:4700:4700::1111]/report.pdf',
      'https://[3ffe::1]/report.pdf',
      'https://[3fff:1000::1]/report.pdf'
    ]) {
      expect(
        isProjectRuntimeSnapshot(
          snapshot({
            artifacts: [
              {
                artifact_id: 'artifact-a',
                presentation: {
                  created_at: 1,
                  kind: 'link',
                  label: 'report.pdf',
                  open_target: { href, kind: 'external_url' },
                  sha256: null,
                  size_bytes: null
                }
              }
            ]
          })
        )
      ).toBe(true)
    }
  })

  it('rejects JS-unsafe integer JSON in transcript and event payloads', () => {
    const unsafeInteger = Number.MAX_SAFE_INTEGER + 1

    expect(isProjectRuntimeSnapshot(snapshot({ transcript: [{ content: { unsafeInteger }, role: 'user' }] }))).toBe(
      false
    )
    expect(isProjectRuntimeEvent({ ...event(2), payload: { unsafeInteger } })).toBe(false)
  })

  it('accepts only the complete Task 7 runtime snapshot projection', () => {
    const complete = {
      active_run: {
        control_state: 'running',
        control_version: 3,
        turn_id: 'turn-a'
      },
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
      transcript_revision: 0,
      version: 4
    }

    expect(isProjectRuntimeSnapshot(complete)).toBe(true)
    expect(isProjectRuntimeSnapshot({ ...complete, version: true })).toBe(false)
    expect(isProjectRuntimeSnapshot({ ...complete, unexpected: 'leak' })).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        active_run: { control_state: 'running', control_version: true, turn_id: 'turn-a' }
      })
    ).toBe(false)
    expect(isProjectRuntimeSnapshot({ ...complete, active_run: { turn_id: 'turn-a' } })).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        delivery_status: { error_code: 'PROJECT_RUNTIME_REJECTED', state: 'caught_up' }
      })
    ).toBe(false)
    expect(
      isProjectRuntimeSnapshot({ ...complete, delivery_status: { error_code: 'unsafe-code', state: 'pending' } })
    ).toBe(false)
    expect(isProjectRuntimeSnapshot({ ...complete, block: { code: 'unsafe-code', kind: 'runtime' } })).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        delivery_status: { error_code: 'turn_recovery_blocked', state: 'pending' }
      })
    ).toBe(true)
    expect(isProjectRuntimeSnapshot({ ...complete, block: { code: 'TURN_RECOVERY_BLOCKED', kind: 'runtime' } })).toBe(
      false
    )
    expect(isProjectRuntimeSnapshot({ ...complete, lifecycle: 'completed' })).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        active_run: null,
        pending_approval: { approval_id: 'approval-a', kind: 'tool' }
      })
    ).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        active_run: { control_state: 'awaiting_approval', control_version: 3, turn_id: 'turn-a' }
      })
    ).toBe(false)
    expect(
      isProjectRuntimeSnapshot({
        ...complete,
        active_run: { control_state: 'awaiting_approval', control_version: 3, turn_id: 'turn-a' },
        pending_approval: { approval_id: 'approval-a', kind: 'tool' }
      })
    ).toBe(true)
  })

  it('recovers a replay race with an authoritative snapshot instead of leaving partial state', async () => {
    const recovered = snapshot({ last_sequence: 2 })
    configureProjectRuntimeRequester(async method => {
      if (method === 'project.runtime.events') {
        applyProjectEvent(event(2))

        return { after_sequence: 1, events: [event(2)], last_sequence: 2, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return recovered
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 2, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: snapshot() } })

    await syncProjectRuntime('project-a')

    expect($projectRuntimes.get()['project-a']).toEqual({ events: [], snapshot: recovered })
  })

  it('drops an old requester response after its profile scope changes', async () => {
    let resolveOldSnapshot: ((value: unknown) => void) | undefined

    const oldRequest = vi.fn(
      () =>
        new Promise<unknown>(resolve => {
          resolveOldSnapshot = resolve
        })
    )

    configureProjectRuntimeRequester(oldRequest, 'profile-old')
    const oldSync = syncProjectRuntime('project-a')
    await vi.waitFor(() => expect(oldRequest).toHaveBeenCalledTimes(1))

    const current = snapshot({ binding_id: 'binding-new', canonical_session_id: 'session-new' })

    const newRequest = vi.fn(async (method: string) => {
      if (method === 'project.runtime.snapshot') {
        return current
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-new', cursor: 1, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })

    configureProjectRuntimeRequester(newRequest, 'profile-new')
    resolveOldSnapshot?.(snapshot({ binding_id: 'binding-old', canonical_session_id: 'session-old' }))

    await expect(oldSync).rejects.toThrow('project runtime requester changed')
    expect($projectRuntimes.get()).toEqual({})
    await syncProjectRuntime('project-a')
    expect($projectRuntimes.get()['project-a'].snapshot).toEqual(current)
    expect(oldRequest).toHaveBeenCalledTimes(1)
  })

  it('invalidates a delayed response when the runtime store is reset', async () => {
    let resolveSnapshot: ((value: unknown) => void) | undefined

    const request = vi.fn(
      () =>
        new Promise<unknown>(resolve => {
          resolveSnapshot = resolve
        })
    )

    configureProjectRuntimeRequester(request, 'profile-a')
    const pending = syncProjectRuntime('project-a')
    await vi.waitFor(() => expect(request).toHaveBeenCalledTimes(1))

    resetProjectRuntimeStore()
    resolveSnapshot?.(snapshot())

    await expect(pending).rejects.toThrow('project runtime requester changed')
    expect($projectRuntimes.get()).toEqual({})
  })

  it('does not recover with a stale requester after a delayed replay page', async () => {
    let resolvePage: ((value: unknown) => void) | undefined

    const oldRequest = vi.fn((method: string) => {
      if (method === 'project.runtime.events') {
        return new Promise<unknown>(resolve => {
          resolvePage = resolve
        })
      }

      if (method === 'project.runtime.snapshot') {
        throw new Error('stale requester must not load a snapshot')
      }

      throw new Error(`unexpected ${method}`)
    })

    configureProjectRuntimeRequester(oldRequest, 'profile-old')
    $projectRuntimes.set({ 'project-a': { events: [], snapshot: snapshot() } })

    const pending = syncProjectRuntime('project-a')
    await vi.waitFor(() => expect(oldRequest).toHaveBeenCalledWith('project.runtime.events', expect.anything()))

    configureProjectRuntimeRequester(async () => snapshot(), 'profile-new')
    resolvePage?.({ after_sequence: 1, events: [], last_sequence: 1, project_id: 'project-a' })

    await expect(pending).rejects.toThrow('project runtime requester changed')
    expect(oldRequest).not.toHaveBeenCalledWith('project.runtime.snapshot', expect.anything())
  })

  it('does not let an old sync finalizer remove a newer generation lock', async () => {
    let resolveOld: ((value: unknown) => void) | undefined
    configureProjectRuntimeRequester(
      () =>
        new Promise<unknown>(resolve => {
          resolveOld = resolve
        }),
      'profile-old'
    )
    const oldSync = syncProjectRuntime('project-a')

    let resolveNew: ((value: unknown) => void) | undefined

    const newRequest = vi.fn((method: string) => {
      if (method === 'project.runtime.snapshot') {
        return new Promise<unknown>(resolve => {
          resolveNew = resolve
        })
      }

      return Promise.resolve({ binding_id: 'binding-new', cursor: 1, project_id: 'project-a' })
    })

    configureProjectRuntimeRequester(newRequest, 'profile-new')
    const newSync = syncProjectRuntime('project-a')
    resolveOld?.(snapshot())
    await expect(oldSync).rejects.toThrow('project runtime requester changed')

    expect(syncProjectRuntime('project-a')).toBe(newSync)
    resolveNew?.(snapshot({ binding_id: 'binding-new' }))
    await newSync
  })

  it('rolls back a replay when a later page is malformed and snapshot recovery fails', async () => {
    const original = { events: [], snapshot: snapshot() }
    configureProjectRuntimeRequester(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.events' && params?.after_sequence === 1) {
        return { after_sequence: 1, events: [event(2)], last_sequence: 3, project_id: 'project-a' }
      }

      if (method === 'project.runtime.events') {
        return { after_sequence: 2, events: [event(4)], last_sequence: 4, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        throw new Error('snapshot unavailable')
      }

      throw new Error('unexpected request')
    })
    $projectRuntimes.set({ 'project-a': original })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('snapshot unavailable')
    expect($projectRuntimes.get()['project-a']).toEqual(original)
  })

  it('rejects a final snapshot behind replay high-water without moving the cursor', async () => {
    const original = { events: [], snapshot: snapshot() }
    configureProjectRuntimeRequester(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.events' && params?.after_sequence === 1) {
        return { after_sequence: 1, events: [event(2)], last_sequence: 2, project_id: 'project-a' }
      }

      if (method === 'project.runtime.events') {
        return { after_sequence: 2, events: [], last_sequence: 2, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        return snapshot({ last_sequence: 1 })
      }

      throw new Error('unexpected request')
    })
    $projectRuntimes.set({ 'project-a': original })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('project runtime snapshot moved backwards')
    expect($projectRuntimes.get()['project-a']).toEqual(original)
  })

  it('does not treat a mutation receipt as visible before its reported high-water is synced', async () => {
    configureProjectRuntimeRequester(async method => {
      if (method === 'project.runtime.snapshot') {
        return snapshot({ last_sequence: 8 })
      }

      throw new Error('unexpected request')
    })

    await expect(syncProjectRuntime('project-a', 9)).rejects.toThrow('project runtime snapshot moved backwards')
    expect($projectRuntimes.get()).toEqual({})
  })

  it('does not let a lower in-flight sync satisfy a later mutation high-water', async () => {
    let releaseAcknowledgement: (() => void) | undefined

    const request = vi.fn((method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.snapshot') {
        return Promise.resolve(snapshot({ last_sequence: 8 }))
      }

      if (method === 'project.runtime.ack' && params?.cursor === 8) {
        return new Promise(resolve => {
          releaseAcknowledgement = () => resolve({ binding_id: 'binding-a', cursor: 8, project_id: 'project-a' })
        })
      }

      if (method === 'project.runtime.events') {
        return Promise.resolve({ after_sequence: 8, events: [], last_sequence: 8, project_id: 'project-a' })
      }

      throw new Error(`unexpected ${method}`)
    })

    configureProjectRuntimeRequester(request)
    const lower = syncProjectRuntime('project-a')
    await vi.waitFor(() => expect(releaseAcknowledgement).toBeTypeOf('function'))
    const highWater = syncProjectRuntime('project-a', 9)

    releaseAcknowledgement?.()

    await lower
    await expect(highWater).rejects.toThrow('project runtime snapshot moved backwards')
  })

  it('rejects recovery below an observed page high-water even when that page ends at an earlier event', async () => {
    const original = { events: [], snapshot: snapshot() }

    const request = vi.fn(async (method: string, params?: Record<string, unknown>) => {
      if (method === 'project.runtime.events' && params?.after_sequence === 1) {
        return { after_sequence: 1, events: [event(2)], last_sequence: 5, project_id: 'project-a' }
      }

      if (method === 'project.runtime.events') {
        return { malformed: true }
      }

      if (method === 'project.runtime.snapshot') {
        return snapshot({ last_sequence: 2 })
      }

      if (method === 'project.runtime.ack') {
        return { binding_id: 'binding-a', cursor: 2, project_id: 'project-a' }
      }

      throw new Error('unexpected request')
    })

    configureProjectRuntimeRequester(request)
    $projectRuntimes.set({ 'project-a': original })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('project runtime snapshot moved backwards')
    expect(request).not.toHaveBeenCalledWith('project.runtime.ack', expect.anything())
    expect($projectRuntimes.get()['project-a']).toEqual(original)
  })

  it('rejects an oversized replay page instead of applying any of it', async () => {
    const original = { events: [], snapshot: snapshot() }
    const oversized = Array.from({ length: 101 }, (_, index) => event(index + 2))
    let eventRequests = 0

    const request = vi.fn(async (method: string) => {
      if (method === 'project.runtime.events') {
        eventRequests += 1

        return { after_sequence: 1, events: oversized, last_sequence: 102, project_id: 'project-a' }
      }

      if (method === 'project.runtime.snapshot') {
        throw new Error('snapshot unavailable')
      }

      throw new Error('unexpected request')
    })

    configureProjectRuntimeRequester(request)
    $projectRuntimes.set({ 'project-a': original })

    await expect(syncProjectRuntime('project-a')).rejects.toThrow('snapshot unavailable')
    expect(eventRequests).toBe(1)
    expect($projectRuntimes.get()['project-a']).toEqual(original)
  })
})
