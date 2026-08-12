import { resolveCurrentManagedProjectSurface } from '@/store/project-surface-authority-store'
import type { SessionInfo } from '@/types/hermes'

export function isManagedProjectComposerTarget(
  runtimeSessionId: null | string | undefined,
  storedSessionId: null | string | undefined,
  storedSession?: SessionInfo
): boolean {
  return (
    resolveCurrentManagedProjectSurface(runtimeSessionId, storedSessionId, storedSession).status !==
    'conclusively-legacy'
  )
}
