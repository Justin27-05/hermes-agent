import type { ProjectInfo, ProjectRuntimeSnapshot, SessionInfo } from '@/types/hermes'

import type { ProjectRuntimeAuthority, ProjectRuntimeState } from './project-runtime'

export interface ProjectCatalogAuthoritySnapshot {
  catalogGeneration: null | number
  contextGeneration: number
  profile: null | string
}

export type ManagedProjectSurfaceResolution =
  | { snapshot: ProjectRuntimeSnapshot; status: 'managed' }
  | { status: 'ambiguous' }
  | { projectId: null | string; status: 'unavailable' }
  | { status: 'conclusively-legacy' }

export interface ManagedProjectSurfaceInput {
  activeProfile: null | string
  activeProjectId?: null | string
  catalogAuthority: ProjectCatalogAuthoritySnapshot
  projects: readonly ProjectInfo[]
  runtimeAuthority: ProjectRuntimeAuthority
  runtimes: Record<string, ProjectRuntimeState>
  runtimeSessionId?: null | string
  sessions: readonly SessionInfo[]
  storedSessionId?: null | string
  targetProfile?: null | string
}

const normalizedProfile = (profile: null | string | undefined): string => profile?.trim() || 'default'

const authorityProfile = (input: ManagedProjectSurfaceInput): string =>
  normalizedProfile(input.targetProfile ?? input.activeProfile)

const catalogIsCurrent = (input: ManagedProjectSurfaceInput): boolean =>
  input.catalogAuthority.catalogGeneration !== null &&
  input.catalogAuthority.catalogGeneration === input.catalogAuthority.contextGeneration &&
  normalizedProfile(input.catalogAuthority.profile) === authorityProfile(input)

const runtimeIsCurrent = (input: ManagedProjectSurfaceInput): boolean =>
  input.runtimeAuthority.scope !== null && normalizedProfile(input.runtimeAuthority.scope) === authorityProfile(input)

const runtimeMatchesSession = (
  runtimes: Record<string, ProjectRuntimeState>,
  sessionId: null | string | undefined
): ProjectRuntimeSnapshot[] =>
  sessionId === null || sessionId === undefined
    ? []
    : Object.values(runtimes)
        .map(runtime => runtime.snapshot)
        .filter(snapshot => snapshot.canonical_session_id === sessionId)

const exactProfileSessionRows = (input: ManagedProjectSurfaceInput, storedSessionId: string): SessionInfo[] =>
  input.sessions.filter(
    session =>
      normalizedProfile(session.profile) === authorityProfile(input) &&
      (session.id === storedSessionId || session._lineage_root_id === storedSessionId)
  )

const projectMarkerResolution = (
  input: ManagedProjectSurfaceInput,
  projectId: null | string | undefined
): 'managed' | 'unavailable' | 'unmanaged' => {
  if (!catalogIsCurrent(input) || projectId === undefined) {
    return 'unavailable'
  }

  if (projectId === null) {
    return 'unmanaged'
  }

  const matches = input.projects.filter(project => project.id === projectId)

  if (matches.length !== 1 || matches[0].managed === undefined) {
    return 'unavailable'
  }

  return matches[0].managed ? 'managed' : 'unmanaged'
}

/**
 * Pure authority boundary for every rendered chat surface.
 *
 * Stored session ids are opaque and authoritative. A supplied stored identity
 * is never replaced by a gateway-minted live id. Catalog and runtime evidence
 * is accepted only for the active profile and current catalog generation.
 */
export function resolveManagedProjectSurface(input: ManagedProjectSurfaceInput): ManagedProjectSurfaceResolution {
  const { storedSessionId } = input

  if (storedSessionId !== null && storedSessionId !== undefined) {
    const runtimeBelongsToAnotherKnownProfile =
      input.runtimeAuthority.scope !== null &&
      normalizedProfile(input.runtimeAuthority.scope) !== authorityProfile(input)

    const storedMatches = runtimeBelongsToAnotherKnownProfile
      ? []
      : runtimeMatchesSession(input.runtimes, storedSessionId)

    const liveMatches = runtimeBelongsToAnotherKnownProfile
      ? []
      : runtimeMatchesSession(input.runtimes, input.runtimeSessionId)

    if (storedMatches.length > 1 || liveMatches.length > 1) {
      return { status: 'ambiguous' }
    }

    if (storedMatches.length === 0 && liveMatches.length === 1) {
      return runtimeIsCurrent(input) ? { status: 'ambiguous' } : { projectId: null, status: 'unavailable' }
    }

    if (storedMatches.length === 1) {
      if (!runtimeIsCurrent(input)) {
        return { projectId: storedMatches[0].project_id, status: 'unavailable' }
      }

      if (liveMatches.length === 1 && liveMatches[0].canonical_session_id !== storedMatches[0].canonical_session_id) {
        return { status: 'ambiguous' }
      }

      const rows = exactProfileSessionRows(input, storedSessionId)

      if (catalogIsCurrent(input)) {
        if (rows.length > 1) {
          return { status: 'ambiguous' }
        }

        if (rows.length === 1) {
          const rowProjectId = rows[0].project_id

          if (rowProjectId !== undefined && rowProjectId !== null && rowProjectId !== storedMatches[0].project_id) {
            return { status: 'ambiguous' }
          }
        }
      }

      return { snapshot: storedMatches[0], status: 'managed' }
    }

    const rows = exactProfileSessionRows(input, storedSessionId)

    if (rows.length > 1) {
      return { status: 'ambiguous' }
    }

    if (rows.length !== 1) {
      return { projectId: null, status: 'unavailable' }
    }

    // `null` is an explicit persisted statement that the exact stored session
    // is not project-owned. Unlike an omitted project_id, it remains safe
    // legacy evidence while another profile's catalog is still loading.
    if (rows[0].project_id === null) {
      return { status: 'conclusively-legacy' }
    }

    if (!catalogIsCurrent(input)) {
      return { projectId: null, status: 'unavailable' }
    }

    const marker = projectMarkerResolution(input, rows[0].project_id)

    return marker === 'unmanaged'
      ? { status: 'conclusively-legacy' }
      : { projectId: rows[0].project_id ?? null, status: 'unavailable' }
  }

  const liveMatches = runtimeMatchesSession(input.runtimes, input.runtimeSessionId)

  if (liveMatches.length > 1) {
    return { status: 'ambiguous' }
  }

  if (liveMatches.length === 1) {
    if (input.activeProjectId && liveMatches[0].project_id !== input.activeProjectId) {
      return { status: 'ambiguous' }
    }

    return runtimeIsCurrent(input)
      ? { snapshot: liveMatches[0], status: 'managed' }
      : { projectId: liveMatches[0].project_id, status: 'unavailable' }
  }

  if (input.activeProjectId !== null && input.activeProjectId !== undefined) {
    const marker = projectMarkerResolution(input, input.activeProjectId)
    const activeRuntime = input.runtimes[input.activeProjectId]

    if (runtimeIsCurrent(input) && activeRuntime?.snapshot.project_id === input.activeProjectId) {
      return marker === 'unmanaged' ? { status: 'ambiguous' } : { snapshot: activeRuntime.snapshot, status: 'managed' }
    }

    if (marker === 'unmanaged') {
      return { status: 'conclusively-legacy' }
    }

    return { projectId: input.activeProjectId, status: 'unavailable' }
  }

  return { status: 'conclusively-legacy' }
}
