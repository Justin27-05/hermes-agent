import { computed, type ReadableAtom } from 'nanostores'

import { $activeGatewayProfile } from './profile'
import { $projectRuntimes } from './project-runtime'
import type { ManagedProjectSurfaceResolution } from './project-surface-authority'
import { resolveCurrentManagedProjectSurface } from './project-surface-authority-store'
import { $activeProjectId, $projectCatalogAuthority, $projects } from './projects'
import { $sessions } from './session'

/**
 * Reactive projection of the shared project-surface authority for user-facing
 * blocking inputs. Consumers decide which canonical input kinds they support;
 * unavailable and ambiguous authority always remain on the managed/blocked
 * side of the boundary.
 */
export function projectBlockingInputSurface(
  runtimeSessionId: null | string,
  storedSessionId: null | string
): ReadableAtom<ManagedProjectSurfaceResolution> {
  return computed(
    [$projectRuntimes, $activeGatewayProfile, $activeProjectId, $projectCatalogAuthority, $projects, $sessions],
    () => {
      const resolution = resolveCurrentManagedProjectSurface(runtimeSessionId, storedSessionId)

      return storedSessionId || resolution.status === 'managed' || resolution.status === 'ambiguous'
        ? resolution
        : ({ projectId: null, status: 'unavailable' } as const)
    }
  )
}
