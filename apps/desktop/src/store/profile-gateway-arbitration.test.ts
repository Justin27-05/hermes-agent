// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import type * as HermesApi from '@/hermes'

const gatewayHarness = vi.hoisted(() => {
  const connectedProfiles: string[] = []
  const requestProfiles: string[] = []

  let releaseArtifactA = () => {}
  let artifactAConnected = Promise.resolve()

  const reset = () => {
    connectedProfiles.length = 0
    requestProfiles.length = 0
    artifactAConnected = new Promise<void>(resolve => {
      releaseArtifactA = resolve
    })
  }

  reset()

  return {
    connectedProfiles,
    releaseArtifactA: () => releaseArtifactA(),
    requestProfiles,
    reset,
    waitForArtifactA: () => artifactAConnected
  }
})

vi.mock('@/hermes', async importOriginal => {
  const original = await importOriginal<typeof HermesApi>()

  class FakeHermesGateway {
    connectionState = 'closed'
    profile = 'unassigned'
    private stateListeners = new Set<(state: string) => void>()

    async connect(wsUrl: string): Promise<void> {
      this.profile = new URL(wsUrl).hostname
      gatewayHarness.connectedProfiles.push(this.profile)

      if (this.profile === 'artifact-a') {
        await gatewayHarness.waitForArtifactA()
      }

      this.connectionState = 'open'
      this.stateListeners.forEach(listener => listener('open'))
    }

    close(): void {
      this.connectionState = 'closed'
      this.stateListeners.forEach(listener => listener('closed'))
    }

    onEvent(): () => void {
      return () => {}
    }

    onState(listener: (state: string) => void): () => void {
      this.stateListeners.add(listener)

      return () => this.stateListeners.delete(listener)
    }

    async request<T>(): Promise<T> {
      gatewayHarness.requestProfiles.push(this.profile)

      return {} as T
    }
  }

  return { ...original, HermesGateway: FakeHermesGateway }
})

vi.mock('@/lib/query-client', () => ({ invalidateProfileScopedQueries: vi.fn() }))
vi.mock('@/store/starmap', () => ({ resetStarmapGraph: vi.fn() }))

const gatewayStore = await import('./gateway')
const profileStore = await import('./profile')
const { HermesGateway } = await import('@/hermes')
const { $connection } = await import('./session')

const connectionFor = (profile: string) =>
  ({
    authMode: 'token',
    baseUrl: `https://${profile}.example`,
    mode: 'remote',
    profile,
    wsUrl: `ws://${profile}`
  }) as never

let stopGatewayObserver: (() => void) | undefined

beforeEach(async () => {
  gatewayHarness.reset()
  vi.stubGlobal('window', {
    hermesDesktop: {
      getConnection: vi.fn(async (profile = 'default') => connectionFor(profile))
    },
    location: { host: 'localhost', protocol: 'http:' }
  })

  const primary = new HermesGateway()

  Object.assign(primary, { connectionState: 'open', profile: 'default' })
  gatewayStore.setPrimaryGateway(primary, 'default')
  await gatewayStore.ensureGatewayForProfile('default')
  profileStore.$activeGatewayProfile.set('default')
  $connection.set(connectionFor('default'))
})

afterEach(async () => {
  stopGatewayObserver?.()
  stopGatewayObserver = undefined
  gatewayHarness.releaseArtifactA()
  gatewayStore.closeSecondaryGateways()
  gatewayStore.setPrimaryGateway(null, 'default')
  await gatewayStore.ensureGatewayForProfile('default')
  profileStore.$activeGatewayProfile.set('default')
  $connection.set(null)
  vi.unstubAllGlobals()
})

describe('profile and gateway activation arbitration', () => {
  it('never publishes a superseded deferred gateway before the latest profile wins', async () => {
    const publishedProfiles: string[] = []

    stopGatewayObserver = gatewayStore.$gateway.subscribe(active => {
      const profile = (active as unknown as { profile?: string } | null)?.profile

      if (profile) {
        publishedProfiles.push(profile)
        void active?.request('publication-probe')
      }
    })
    publishedProfiles.length = 0
    gatewayHarness.requestProfiles.length = 0

    const artifactA = profileStore.ensureGatewayProfile('artifact-a')

    await vi.waitFor(() => expect(gatewayHarness.connectedProfiles).toContain('artifact-a'))

    const artifactB = profileStore.ensureGatewayProfile('artifact-b')

    gatewayHarness.releaseArtifactA()
    await Promise.all([artifactA, artifactB])

    expect(publishedProfiles).toEqual(['artifact-b'])
    expect(gatewayHarness.requestProfiles).toEqual(['artifact-b'])
    expect((gatewayStore.$gateway.get() as unknown as { profile: string }).profile).toBe('artifact-b')
    expect((gatewayStore.activeGateway() as unknown as { profile: string }).profile).toBe('artifact-b')
    expect(profileStore.$activeGatewayProfile.get()).toBe('artifact-b')
  })
})
