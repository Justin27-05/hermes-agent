import { computed } from 'nanostores'

import type { SessionInfo } from '@/types/hermes'

import { $activeGatewayProfile } from './profile'
import { $projectRuntimes, projectRuntimeAuthority } from './project-runtime'
import { type ManagedProjectSurfaceResolution, resolveManagedProjectSurface } from './project-surface-authority'
import { $activeProjectId, $projectCatalogAuthority, $projects } from './projects'
import { $sessions } from './session'

export const $projectSurfaceAuthorityContext = computed(
  [$activeGatewayProfile, $activeProjectId, $projectCatalogAuthority, $projects, $projectRuntimes, $sessions],
  (activeProfile, activeProjectId, catalogAuthority, projects, runtimes, sessions) => ({
    activeProfile: activeProfile || 'default',
    activeProjectId,
    catalogAuthority,
    projects,
    runtimeAuthority: projectRuntimeAuthority(),
    runtimes,
    sessions
  })
)

export function resolveCurrentManagedProjectSurface(
  runtimeSessionId: null | string | undefined,
  storedSessionId: null | string | undefined,
  storedSession?: SessionInfo
): ManagedProjectSurfaceResolution {
  const context = $projectSurfaceAuthorityContext.get()

  const matchingRows =
    storedSessionId === null || storedSessionId === undefined
      ? []
      : context.sessions.filter(
          session => session.id === storedSessionId || session._lineage_root_id === storedSessionId
        )

  if (!storedSession && matchingRows.length > 1) {
    return { status: 'ambiguous' }
  }

  const exactStoredSession = storedSession ?? (matchingRows.length === 1 ? matchingRows[0] : undefined)

  const sessions = storedSession
    ? [
        ...context.sessions.filter(
          session => session.id !== storedSession.id && session._lineage_root_id !== storedSession.id
        ),
        storedSession
      ]
    : context.sessions

  return resolveManagedProjectSurface({
    ...context,
    runtimeSessionId,
    sessions,
    storedSessionId,
    targetProfile: exactStoredSession?.profile
  })
}
