import { beforeEach, expect, it, vi } from 'vitest'

import type { SubmitTextOptions } from '@/app/session/hooks/use-prompt-actions/utils'
import { setPrimaryGateway } from '@/store/gateway'
import {
  captureExactLegacySessionAuthority,
  captureFrozenLegacyDraftAuthority,
  rebindExactLegacySessionAuthority,
  validateExactLegacySessionAuthority
} from '@/store/legacy-session-authority'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester } from '@/store/project-runtime'
import { $activeProjectId, $projectCatalogAuthority, $projects } from '@/store/projects'
import { setSessions } from '@/store/session'

import { submitAfterComposerMiddleware } from './submit-boundary'

const gatewayA = { request: vi.fn() } as never

beforeEach(() => {
  setPrimaryGateway(gatewayA, 'profile-a')
  $activeGatewayProfile.set('profile-a')
  setSessions(() => [])
  $activeProjectId.set(null)
  $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'profile-a' })
  $projects.set([])
  $projectRuntimes.set({})
  configureProjectRuntimeRequester(vi.fn(async () => undefined), 'profile-a')
})

it('captures the canonical immutable FrozenFreshDraftAuthority ticket shape', () => {
  const authority = captureFrozenLegacyDraftAuthority()

  expect(authority).not.toBeNull()
  expect(Object.isFrozen(authority)).toBe(true)
  expect(Object.keys(authority!).sort()).toEqual(
    [
      'activeGatewayProfile',
      'activeProjectId',
      'catalogContextGeneration',
      'catalogGeneration',
      'catalogProfile',
      'gateway',
      'gatewayGeneration',
      'runtimeRequesterGeneration',
      'runtimeRequesterScope',
      'status'
    ].sort()
  )
  expect(authority).toMatchObject({
    activeGatewayProfile: 'profile-a',
    activeProjectId: null,
    catalogContextGeneration: 1,
    catalogGeneration: 1,
    catalogProfile: 'profile-a',
    gateway: gatewayA,
    runtimeRequesterScope: 'profile-a',
    status: 'conclusively-legacy'
  })
  expect(authority?.gatewayGeneration).toEqual(expect.any(Number))
  expect(authority?.runtimeRequesterGeneration).toEqual(expect.any(Number))
})

it('blocks a conclusively legacy draft when its full producer ticket cannot be captured', async () => {
  $projectCatalogAuthority.set({ catalogGeneration: null, contextGeneration: 2, profile: 'profile-a' })
  const submit = vi.fn(async () => true)

  await expect(
    submitAfterComposerMiddleware({
      middleware: async input => ({ text: input.text }),
      submit,
      target: { runtimeSessionId: null, storedSessionId: null },
      value: 'wait for complete authority'
    })
  ).resolves.toBe(false)

  expect(submit).not.toHaveBeenCalled()
})

it('rebinds an exact legacy authority from R1 to R2 only while its durable owner stays current', () => {
  const storedSession = { id: 'stored-C', profile: 'profile-a', project_id: null } as never
  setSessions(() => [storedSession])
  $activeGatewayProfile.set('profile-a')

  const source = captureExactLegacySessionAuthority({
    requireActiveGateway: true,
    runtimeSessionId: 'runtime-R1',
    storedSession
  })

  const rebound = source ? rebindExactLegacySessionAuthority(source, 'runtime-R2') : null

  expect(source?.runtimeSessionId).toBe('runtime-R1')
  expect(rebound).not.toBeNull()
  expect(rebound?.runtimeSessionId).toBe('runtime-R2')
  expect(validateExactLegacySessionAuthority(rebound!, { runtimeSessionId: 'runtime-R2' })).toBe(true)

  setPrimaryGateway({ request: vi.fn() } as never, 'profile-a')

  expect(rebindExactLegacySessionAuthority(rebound!, 'runtime-R3')).toBeNull()
})

it('keeps the exact A target frozen while deferred middleware settles without authority drift', async () => {
  let release!: (value: { text: string }) => void
  const middleware = vi.fn(() => new Promise<{ text: string }>(resolve => (release = resolve)))
  const submit = vi.fn(async () => true)
  const rowA = { id: 'same-C', profile: 'profile-a', project_id: null } as never
  const rowB = { id: 'same-C', profile: 'profile-b', project_id: null } as never
  $activeGatewayProfile.set('profile-a')

  // Reconstructed RED before the boundary helper: ChatBar read its target only
  // after this await, so rerendering to B forwarded rowB/current refs.
  const pending = submitAfterComposerMiddleware({
    middleware,
    submit,
    target: { runtimeSessionId: 'runtime-A', storedSession: rowA, storedSessionId: 'same-C' },
    value: 'ship A'
  })

  const unsafeCurrentAfterAwait: SubmitTextOptions = {
    sessionId: 'runtime-B',
    storedSession: rowB,
    storedSessionId: 'same-C'
  }

  expect(unsafeCurrentAfterAwait.storedSession).toBe(rowB)
  release({ text: 'ship A rewritten' })

  await expect(pending).resolves.toBe(true)
  expect(submit).toHaveBeenCalledWith(
    'ship A rewritten',
    expect.objectContaining({
      sessionId: 'runtime-A',
      storedSession: rowA,
      storedSessionId: 'same-C'
    })
  )
})

it('blocks an exact legacy A submit when the active gateway changes to B during middleware', async () => {
  let release!: (value: { text: string }) => void
  const submit = vi.fn(async () => true)
  const rowA = { id: 'same-C', profile: 'profile-a', project_id: null } as never
  $activeGatewayProfile.set('profile-a')

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'same-R', storedSession: rowA, storedSessionId: 'same-C' },
    value: 'do not cross profiles'
  })

  $activeGatewayProfile.set('profile-b')
  release({ text: 'do not cross profiles' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})

it('allows exact legacy profile B through an unchanged profile A transport gateway', async () => {
  $activeGatewayProfile.set('profile-a')
  const submit = vi.fn(async () => true)
  const rowB = { id: 'stored-B', profile: 'profile-b', project_id: null } as never

  await expect(
    submitAfterComposerMiddleware({
      middleware: async input => ({ text: input.text }),
      submit,
      target: { runtimeSessionId: 'runtime-B', storedSession: rowB, storedSessionId: 'stored-B' },
      value: 'keep B exact'
    })
  ).resolves.toBe(true)

  expect(submit).toHaveBeenCalledWith(
    'keep B exact',
    expect.objectContaining({ sessionId: 'runtime-B', storedSession: rowB, storedSessionId: 'stored-B' })
  )
})

it('blocks identical managed authority after requester generation replacement during middleware', async () => {
  let release!: (value: { text: string }) => void

  const snapshot = {
    active_run: null,
    artifacts: [],
    binding_id: 'same-binding',
    block: null,
    canonical_session_id: 'same-C',
    current_phase: 'implementation',
    delivery_status: { error_code: null, state: 'caught_up' },
    last_sequence: 1,
    lifecycle: 'active',
    pending_approval: null,
    project_id: 'same-project',
    queue: [],
    transcript: [],
    transcript_revision: 1,
    version: 1
  } as never

  configureProjectRuntimeRequester(
    vi.fn(async () => undefined),
    'profile-a'
  )
  $projectRuntimes.set({ 'same-project': { events: [], snapshot } })
  $activeGatewayProfile.set('profile-a')
  const submit = vi.fn(async () => true)
  const row = { id: 'same-C', profile: 'profile-a', project_id: 'same-project' } as never

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'same-R', storedSession: row, storedSessionId: 'same-C' },
    value: 'generation A'
  })

  configureProjectRuntimeRequester(
    vi.fn(async () => undefined),
    'profile-a'
  )
  $projectRuntimes.set({ 'same-project': { events: [], snapshot } })
  release({ text: 'generation A' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})

it('keeps a managed submit flowing when only non-authority runtime content changes during middleware', async () => {
  let release!: (value: { text: string }) => void

  const snapshotBase = {
    active_run: null,
    artifacts: [],
    binding_id: 'binding-a',
    block: null,
    canonical_session_id: 'same-C',
    current_phase: 'implementation',
    delivery_status: { error_code: null, state: 'caught_up' },
    last_sequence: 1,
    lifecycle: 'active',
    pending_approval: null,
    project_id: 'same-project',
    queue: [],
    transcript: [],
    transcript_revision: 1,
    version: 1
  }

  const snapshotA = { ...snapshotBase } as never
  const snapshotChurned = { ...snapshotBase, last_sequence: 2 } as never

  configureProjectRuntimeRequester(vi.fn(async () => undefined), 'profile-a')
  $projectRuntimes.set({ 'same-project': { events: [], snapshot: snapshotA } })
  $activeGatewayProfile.set('profile-a')
  const submit = vi.fn(async () => true)
  const row = { id: 'same-C', profile: 'profile-a', project_id: 'same-project' } as never

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'same-R', storedSession: row, storedSessionId: 'same-C' },
    value: 'keep flowing'
  })

  // Alleen inhoudelijke runtime-mutatie (sequence) tijdens de await: de
  // authority-identiteit (binding/project/canonical) verandert niet, dus de
  // submit moet gewoon doorgaan — de transitieteller is bewust niet over-breed.
  $projectRuntimes.set({ 'same-project': { events: [], snapshot: snapshotChurned } })
  release({ text: 'keep flowing' })

  await expect(pending).resolves.toBe(true)
  expect(submit).toHaveBeenCalled()
})

it('keeps an initially unavailable target fail-closed when it becomes legacy during middleware', async () => {
  let release!: (value: { text: string }) => void
  $activeGatewayProfile.set('profile-a')
  $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'profile-a' })
  $projects.set([{ id: 'project-a', managed: true } as never])
  const submit = vi.fn(async () => true)
  const row = { id: 'stored-A', profile: 'profile-a', project_id: 'project-a' } as never

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'runtime-A', storedSession: row, storedSessionId: 'stored-A' },
    value: 'remain blocked'
  })

  $projects.set([{ id: 'project-a', managed: false } as never])
  release({ text: 'remain blocked' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})

it('blocks a fresh legacy draft after managed-project ABA while middleware awaits', async () => {
  let release!: (value: { text: string }) => void
  const submit = vi.fn(async () => true)

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: null, storedSessionId: null },
    value: 'fresh legacy draft'
  })

  const snapshot = {
    active_run: null,
    artifacts: [],
    binding_id: 'binding-managed',
    block: null,
    canonical_session_id: 'canonical-managed',
    current_phase: 'implementation',
    delivery_status: { error_code: null, state: 'caught_up' },
    last_sequence: 1,
    lifecycle: 'active',
    pending_approval: null,
    project_id: 'project-managed',
    queue: [],
    transcript: [],
    transcript_revision: 1,
    version: 1
  } as never

  $projects.set([{ id: 'project-managed', managed: true } as never])
  $activeProjectId.set('project-managed')
  $projectRuntimes.set({ 'project-managed': { events: [], snapshot } })
  // The visible surface is legacy again by settle time. Only the frozen draft
  // generations reveal that this is not the authority under which the user
  // pressed Enter.
  $activeProjectId.set(null)
  $projects.set([])
  $projectRuntimes.set({})
  $projectCatalogAuthority.set({ catalogGeneration: 2, contextGeneration: 2, profile: 'profile-a' })
  release({ text: 'fresh legacy draft' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})

it('invalidates exact legacy authority when the runtime requester generation changes on the same scope', () => {
  const row = { id: 'stored-A', profile: 'profile-a', project_id: null } as never
  setSessions(() => [row])

  const authority = captureExactLegacySessionAuthority({
    requireActiveGateway: true,
    runtimeSessionId: 'runtime-A',
    storedSession: row,
    storedSessionId: 'stored-A'
  })

  expect(authority).not.toBeNull()
  expect(Object.isFrozen(authority)).toBe(true)
  expect(authority).toMatchObject({
    activeGatewayProfile: 'profile-a',
    catalogContextGeneration: 1,
    catalogGeneration: 1,
    catalogProfile: 'profile-a',
    runtimeRequesterScope: 'profile-a',
    status: 'conclusively-legacy',
    storedSessionId: 'stored-A',
    targetProfile: 'profile-a'
  })
  expect(authority?.runtimeRequesterGeneration).toEqual(expect.any(Number))

  configureProjectRuntimeRequester(vi.fn(async () => undefined), 'profile-a')

  expect(validateExactLegacySessionAuthority(authority!, { runtimeSessionId: 'runtime-A' })).toBe(false)

  const recaptured = captureExactLegacySessionAuthority({
    requireActiveGateway: true,
    runtimeSessionId: 'runtime-A',
    storedSession: row,
    storedSessionId: 'stored-A'
  })

  expect(recaptured).not.toBeNull()

  $projectCatalogAuthority.set({ catalogGeneration: 2, contextGeneration: 2, profile: 'profile-a' })

  expect(validateExactLegacySessionAuthority(recaptured!, { runtimeSessionId: 'runtime-A' })).toBe(false)
})

it('blocks an exact legacy submit when the gateway object is replaced on the same profile', async () => {
  let release!: (value: { text: string }) => void
  const submit = vi.fn(async () => true)
  const row = { id: 'stored-A', profile: 'profile-a', project_id: null } as never

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'runtime-A', storedSession: row, storedSessionId: 'stored-A' },
    value: 'keep gateway A'
  })

  setPrimaryGateway({ request: vi.fn() } as never, 'profile-a')
  release({ text: 'keep gateway A' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})

it('blocks an exact managed A submit after an A→B→A surface transition during middleware (middleware ABA)', async () => {
  let release!: (value: { text: string }) => void

  const snapshotBase = {
    active_run: null,
    artifacts: [],
    binding_id: 'binding-a',
    block: null,
    canonical_session_id: 'same-C',
    current_phase: 'implementation',
    delivery_status: { error_code: null, state: 'caught_up' },
    last_sequence: 1,
    lifecycle: 'active',
    pending_approval: null,
    project_id: 'same-project',
    queue: [],
    transcript: [],
    transcript_revision: 1,
    version: 1
  }

  const snapshotA = { ...snapshotBase } as never
  const snapshotB = { ...snapshotBase, binding_id: 'binding-b' } as never

  configureProjectRuntimeRequester(vi.fn(async () => undefined), 'profile-a')
  $projectRuntimes.set({ 'same-project': { events: [], snapshot: snapshotA } })
  $activeGatewayProfile.set('profile-a')
  const submit = vi.fn(async () => true)
  const row = { id: 'same-C', profile: 'profile-a', project_id: 'same-project' } as never

  const pending = submitAfterComposerMiddleware({
    middleware: () => new Promise(resolve => (release = resolve)),
    submit,
    target: { runtimeSessionId: 'same-R', storedSession: row, storedSessionId: 'same-C' },
    value: 'do not cross ABA'
  })

  // Middleware-ABA: de managed binding wisselt A→B→A terwijl de middleware
  // pending is en keert terug naar dezelfde observable identity. De boundary
  // vergelijkt alleen de eindwaarde met de capture (gelijk ⇒ passeert); een
  // monotone surface-transitie tijdens de await moet de invocation echter
  // invalideren, ook al is de eindwaarde identiek aan de startwaarde.
  $projectRuntimes.set({ 'same-project': { events: [], snapshot: snapshotB } })
  $projectRuntimes.set({ 'same-project': { events: [], snapshot: snapshotA } })
  release({ text: 'do not cross ABA' })

  await expect(pending).resolves.toBe(false)
  expect(submit).not.toHaveBeenCalled()
})
