import { atom, computed } from 'nanostores'

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

// Monotone surface-transitieteller (reviewer-A C1 — middleware ABA): verhoogt
// alleen wanneer de authority-relevante surface-context verandert (profiel,
// projectselectie, catalogus, project-/runtime-identiteit, requester-
// generatie/scope, sessie-identiteit). De composer-boundary gebruikt dit als
// monotonic surface-transition evidence: een A→B→A-transitie tijdens een
// pending middleware verhoogt de teller ook wanneer de eindwaarde identiek is
// aan de startwaarde, zodat de invocation wordt geïnvalideerd (fail-closed).
// Zuiver inhoudelijke runtime-mutaties (transcript/sequence/artifacts) tellen
// niet mee, zodat een legitieme submit niet wordt geblokkeerd door onschuldige
// churn tijdens een pending middleware.
export const $projectSurfaceTransitionGeneration = atom(0)

function surfaceAuthorityFingerprint(
  ctx: ReturnType<typeof $projectSurfaceAuthorityContext.get>
): string {
  const projects = ctx.projects
    .map(project => `${project.id}:${project.managed ?? ''}`)
    .sort()
    .join('|')

  const runtimes = Object.keys(ctx.runtimes)
    .sort()
    .map(projectId => {
      const snapshot = ctx.runtimes[projectId].snapshot

      return `${projectId}:${snapshot.project_id}:${snapshot.binding_id}:${snapshot.canonical_session_id}`
    })
    .join('|')

  const sessions = ctx.sessions
    .map(
      session =>
        `${session.id}:${session.profile ?? ''}:${session.project_id ?? ''}:${session._lineage_root_id ?? ''}`
    )
    .sort()
    .join('|')

  return [
    ctx.activeProfile,
    ctx.activeProjectId ?? '',
    ctx.catalogAuthority.catalogGeneration ?? '',
    ctx.catalogAuthority.contextGeneration,
    ctx.catalogAuthority.profile ?? '',
    ctx.runtimeAuthority.requesterGeneration,
    ctx.runtimeAuthority.scope ?? '',
    projects,
    runtimes,
    sessions
  ].join('|')
}

let lastSurfaceAuthorityFingerprint: string | undefined
$projectSurfaceAuthorityContext.listen(ctx => {
  const fingerprint = surfaceAuthorityFingerprint(ctx)

  if (fingerprint !== lastSurfaceAuthorityFingerprint) {
    lastSurfaceAuthorityFingerprint = fingerprint
    $projectSurfaceTransitionGeneration.set($projectSurfaceTransitionGeneration.get() + 1)
  }
})

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
