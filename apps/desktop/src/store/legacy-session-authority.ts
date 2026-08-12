import type { HermesGateway } from '@/hermes'
import type { SessionInfo } from '@/types/hermes'

import { activeGateway } from './gateway'
import { $activeGatewayProfile, gatewayProfileAuthorityGeneration, normalizeProfileKey } from './profile'
import { projectRuntimeAuthority } from './project-runtime'
import { resolveCurrentManagedProjectSurface } from './project-surface-authority-store'
import { $activeProjectId, $projectCatalogAuthority } from './projects'
import { $sessions, sessionMatchesStoredId } from './session'

interface CaptureExactLegacySessionAuthorityOptions {
  allowCrossProfileGateway?: boolean
  requireActiveGateway?: boolean
  runtimeSessionId?: null | string
  storedSession: SessionInfo
  storedSessionId?: string
}

export interface ExactLegacySessionAuthority {
  readonly activeGatewayProfile: string
  readonly allowCrossProfileGateway: boolean
  readonly catalogContextGeneration: number
  readonly catalogGeneration: number
  readonly catalogProfile: string
  readonly gateway: HermesGateway | null
  readonly gatewayGeneration: number
  readonly requireActiveGateway: boolean
  readonly runtimeRequesterGeneration: number
  readonly runtimeRequesterScope: string
  readonly runtimeSessionId: null | string
  readonly status: 'conclusively-legacy'
  readonly storedSession: Readonly<SessionInfo>
  readonly storedSessionId: string
  readonly targetProfile: string
}

export interface FrozenFreshDraftAuthority {
  readonly activeGatewayProfile: string
  readonly activeProjectId: null | string
  readonly catalogContextGeneration: number
  readonly catalogGeneration: number
  readonly catalogProfile: string
  readonly gateway: HermesGateway
  readonly gatewayGeneration: number
  readonly runtimeRequesterGeneration: number
  readonly runtimeRequesterScope: string
  readonly status: 'conclusively-legacy'
}

const normalizedProfile = (profile: null | string | undefined): string => normalizeProfileKey(profile)

const sameDurableAuthority = (left: SessionInfo, right: Readonly<SessionInfo>): boolean =>
  left.id === right.id &&
  (left._lineage_root_id ?? null) === (right._lineage_root_id ?? null) &&
  normalizedProfile(left.profile) === normalizedProfile(right.profile) &&
  left.project_id === right.project_id

function exactCurrentRow(
  storedSessionId: string,
  targetProfile: string,
  captured: Readonly<SessionInfo>
): null | SessionInfo {
  const matches = $sessions
    .get()
    .filter(
      session =>
        normalizedProfile(session.profile) === targetProfile && sessionMatchesStoredId(session, storedSessionId)
    )

  if (matches.length > 1) {
    return null
  }

  if (matches.length === 1) {
    return sameDurableAuthority(matches[0], captured) ? matches[0] : null
  }

  // Archived rows and project-tree-only rows are not necessarily present in
  // the recents store. The immutable exact row supplied by the caller remains
  // the durable identity; live ProjectRuntime evidence is still resolved below.
  return captured as SessionInfo
}

export function captureExactLegacySessionAuthority({
  allowCrossProfileGateway = false,
  requireActiveGateway = false,
  runtimeSessionId = null,
  storedSession,
  storedSessionId = storedSession.id
}: CaptureExactLegacySessionAuthorityOptions): ExactLegacySessionAuthority | null {
  if (!sessionMatchesStoredId(storedSession, storedSessionId)) {
    return null
  }

  const targetProfile = normalizedProfile(storedSession.profile)
  const currentRow = exactCurrentRow(storedSessionId, targetProfile, storedSession)

  if (
    !currentRow ||
    resolveCurrentManagedProjectSurface(runtimeSessionId, storedSessionId, currentRow).status !== 'conclusively-legacy'
  ) {
    return null
  }

  const gateway = activeGateway()
  const catalogAuthority = $projectCatalogAuthority.get()
  const runtimeAuthority = projectRuntimeAuthority()

  if (
    catalogAuthority.catalogGeneration === null ||
    runtimeAuthority.scope === null ||
    requireActiveGateway &&
    ((!allowCrossProfileGateway && normalizedProfile($activeGatewayProfile.get()) !== targetProfile) ||
      gateway === null)
  ) {
    return null
  }

  return Object.freeze({
    activeGatewayProfile: normalizedProfile($activeGatewayProfile.get()),
    allowCrossProfileGateway,
    catalogContextGeneration: catalogAuthority.contextGeneration,
    catalogGeneration: catalogAuthority.catalogGeneration,
    catalogProfile: normalizedProfile(catalogAuthority.profile),
    gateway,
    gatewayGeneration: gatewayProfileAuthorityGeneration(),
    requireActiveGateway,
    runtimeRequesterGeneration: runtimeAuthority.requesterGeneration,
    runtimeRequesterScope: normalizedProfile(runtimeAuthority.scope),
    runtimeSessionId,
    status: 'conclusively-legacy',
    storedSession: Object.freeze({ ...currentRow }),
    storedSessionId,
    targetProfile
  })
}

export function validateExactLegacySessionAuthority(
  authority: ExactLegacySessionAuthority,
  options: { runtimeSessionId?: null | string } = {}
): boolean {
  const runtimeSessionId =
    options.runtimeSessionId === undefined ? authority.runtimeSessionId : options.runtimeSessionId
  const catalogAuthority = $projectCatalogAuthority.get()
  const runtimeAuthority = projectRuntimeAuthority()

  if (
    authority.status !== 'conclusively-legacy' ||
    runtimeSessionId !== authority.runtimeSessionId ||
    normalizedProfile($activeGatewayProfile.get()) !== authority.activeGatewayProfile ||
    catalogAuthority.catalogGeneration !== authority.catalogGeneration ||
    catalogAuthority.contextGeneration !== authority.catalogContextGeneration ||
    normalizedProfile(catalogAuthority.profile) !== authority.catalogProfile ||
    runtimeAuthority.requesterGeneration !== authority.runtimeRequesterGeneration ||
    normalizedProfile(runtimeAuthority.scope) !== authority.runtimeRequesterScope
  ) {
    return false
  }

  if (gatewayProfileAuthorityGeneration() !== authority.gatewayGeneration) {
    return false
  }

  if (
    authority.requireActiveGateway &&
    ((!authority.allowCrossProfileGateway &&
      normalizedProfile($activeGatewayProfile.get()) !== authority.targetProfile) ||
      activeGateway() !== authority.gateway)
  ) {
    return false
  }

  const currentRow = exactCurrentRow(authority.storedSessionId, authority.targetProfile, authority.storedSession)

  return (
    currentRow !== null &&
    resolveCurrentManagedProjectSurface(runtimeSessionId, authority.storedSessionId, currentRow).status ===
      'conclusively-legacy'
  )
}

/**
 * Mint the sole allowed runtime rebind for an exact durable legacy owner.
 * Both the captured R1 authority and the proposed R2 surface must still be
 * conclusively legacy under the same immutable row, profile, and gateway.
 */
export function rebindExactLegacySessionAuthority(
  authority: ExactLegacySessionAuthority,
  runtimeSessionId: string
): ExactLegacySessionAuthority | null {
  if (!runtimeSessionId || !validateExactLegacySessionAuthority(authority)) {
    return null
  }

  const rebound = Object.freeze({ ...authority, runtimeSessionId })

  return validateExactLegacySessionAuthority(rebound, { runtimeSessionId }) ? rebound : null
}

/**
 * A new-chat draft has no durable SessionInfo row to pin. Freeze every store
 * generation that can change whether `session.create` belongs to legacy chat
 * or to a managed project's canonical surface.
 */
export function captureFrozenLegacyDraftAuthority(): FrozenFreshDraftAuthority | null {
  if (resolveCurrentManagedProjectSurface(null, null).status !== 'conclusively-legacy') {
    return null
  }

  const gateway = activeGateway()

  const catalogAuthority = $projectCatalogAuthority.get()
  const runtimeAuthority = projectRuntimeAuthority()

  if (!gateway || catalogAuthority.catalogGeneration === null || runtimeAuthority.scope === null) {
    return null
  }

  return Object.freeze({
    activeGatewayProfile: normalizedProfile($activeGatewayProfile.get()),
    activeProjectId: $activeProjectId.get(),
    catalogContextGeneration: catalogAuthority.contextGeneration,
    catalogGeneration: catalogAuthority.catalogGeneration,
    catalogProfile: normalizedProfile(catalogAuthority.profile),
    gateway,
    gatewayGeneration: gatewayProfileAuthorityGeneration(),
    runtimeRequesterGeneration: runtimeAuthority.requesterGeneration,
    runtimeRequesterScope: normalizedProfile(runtimeAuthority.scope),
    status: 'conclusively-legacy'
  })
}

export function validateFrozenLegacyDraftAuthority(authority: FrozenFreshDraftAuthority): boolean {
  const catalogAuthority = $projectCatalogAuthority.get()
  const runtimeAuthority = projectRuntimeAuthority()

  return (
    authority.status === 'conclusively-legacy' &&
    activeGateway() === authority.gateway &&
    gatewayProfileAuthorityGeneration() === authority.gatewayGeneration &&
    normalizedProfile($activeGatewayProfile.get()) === authority.activeGatewayProfile &&
    $activeProjectId.get() === authority.activeProjectId &&
    catalogAuthority.catalogGeneration === authority.catalogGeneration &&
    catalogAuthority.contextGeneration === authority.catalogContextGeneration &&
    normalizedProfile(catalogAuthority.profile) === authority.catalogProfile &&
    runtimeAuthority.requesterGeneration === authority.runtimeRequesterGeneration &&
    normalizedProfile(runtimeAuthority.scope) === authority.runtimeRequesterScope &&
    resolveCurrentManagedProjectSurface(null, null).status === 'conclusively-legacy'
  )
}
