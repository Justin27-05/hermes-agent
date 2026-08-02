import type { SubmitTextOptions } from '@/app/session/hooks/use-prompt-actions/utils'
import { activeGateway } from '@/store/gateway'
import {
  captureExactLegacySessionAuthority,
  captureFrozenLegacyDraftAuthority,
  validateExactLegacySessionAuthority,
  validateFrozenLegacyDraftAuthority
} from '@/store/legacy-session-authority'
import { $activeGatewayProfile, normalizeProfileKey } from '@/store/profile'
import { captureProjectSubmitAuthority } from '@/store/project-composer-queue'
import { projectRuntimeAuthority } from '@/store/project-runtime'
import { resolveCurrentManagedProjectSurface } from '@/store/project-surface-authority-store'
import type { SessionInfo } from '@/types/hermes'

interface SubmitBoundaryTarget {
  runtimeSessionId: null | string | undefined
  storedSession?: SessionInfo
  storedSessionId: null | string | undefined
}

export function freezeComposerSubmitOptions(
  options: SubmitTextOptions | undefined,
  target: SubmitBoundaryTarget
): SubmitTextOptions {
  const storedSession = options?.storedSession ?? target.storedSession
  const storedSessionId = options?.storedSessionId ?? target.storedSessionId
  const runtimeSessionId = options?.sessionId ?? target.runtimeSessionId
  const surface = resolveCurrentManagedProjectSurface(runtimeSessionId, storedSessionId, storedSession)

  const legacyAuthority =
    surface.status === 'conclusively-legacy' && storedSession
      ? captureExactLegacySessionAuthority({
          allowCrossProfileGateway: true,
          requireActiveGateway: true,
          runtimeSessionId: runtimeSessionId ?? null,
          storedSession,
          storedSessionId: storedSessionId ?? storedSession.id
        })
      : null

  const legacyDraftAuthority =
    surface.status === 'conclusively-legacy' && !storedSession && !storedSessionId && !runtimeSessionId
      ? captureFrozenLegacyDraftAuthority()
      : null

  return {
    ...options,
    legacyAuthority: legacyAuthority ?? undefined,
    legacyDraftAuthority: legacyDraftAuthority ?? undefined,
    projectAuthority:
      options?.projectAuthority ??
      (surface.status === 'managed' ? captureProjectSubmitAuthority(surface.snapshot.canonical_session_id) : undefined),
    sessionId: runtimeSessionId,
    storedSession,
    storedSessionId
  }
}

export async function submitAfterComposerMiddleware(deps: {
  middleware: (input: {
    attachments: SubmitTextOptions['attachments']
    text: string
  }) => Promise<null | { attachments?: SubmitTextOptions['attachments']; text: string }>
  options?: SubmitTextOptions
  submit: (text: string, options: SubmitTextOptions) => boolean | Promise<boolean>
  target: SubmitBoundaryTarget
  value: string
}): Promise<boolean> {
  // Capture synchronously. Nothing read after middleware may retarget this
  // submit to the profile/session that happens to be current when it settles.
  const frozen = freezeComposerSubmitOptions(deps.options, deps.target)

  const capturedSurface = resolveCurrentManagedProjectSurface(
    frozen.sessionId,
    frozen.storedSessionId,
    frozen.storedSession
  )

  if (
    capturedSurface.status === 'conclusively-legacy' &&
    !frozen.legacyAuthority &&
    !frozen.legacyDraftAuthority
  ) {
    return false
  }

  const capturedActiveProfile = normalizeProfileKey($activeGatewayProfile.get())
  const capturedGateway = activeGateway()
  const capturedRuntimeAuthority = projectRuntimeAuthority()
  const draft = await deps.middleware({ attachments: deps.options?.attachments, text: deps.value })

  if (!draft) {
    return false
  }

  const currentSurface = resolveCurrentManagedProjectSurface(
    frozen.sessionId,
    frozen.storedSessionId,
    frozen.storedSession
  )

  const currentRuntimeAuthority = projectRuntimeAuthority()

  if (
    capturedSurface.status === 'ambiguous' ||
    capturedSurface.status === 'unavailable' ||
    currentSurface.status !== capturedSurface.status ||
    activeGateway() !== capturedGateway ||
    currentRuntimeAuthority.requesterGeneration !== capturedRuntimeAuthority.requesterGeneration ||
    currentRuntimeAuthority.scope !== capturedRuntimeAuthority.scope ||
    normalizeProfileKey($activeGatewayProfile.get()) !== capturedActiveProfile ||
    (capturedSurface.status === 'managed' &&
      (currentSurface.status !== 'managed' ||
        currentSurface.snapshot.binding_id !== capturedSurface.snapshot.binding_id ||
        currentSurface.snapshot.project_id !== capturedSurface.snapshot.project_id ||
        currentSurface.snapshot.canonical_session_id !== capturedSurface.snapshot.canonical_session_id)) ||
    (frozen.legacyAuthority &&
      !validateExactLegacySessionAuthority(frozen.legacyAuthority, {
        runtimeSessionId: frozen.sessionId ?? null
      })) ||
    (frozen.legacyDraftAuthority && !validateFrozenLegacyDraftAuthority(frozen.legacyDraftAuthority))
  ) {
    return false
  }

  return deps.submit(draft.text, { ...frozen, attachments: draft.attachments })
}
