import {
  createProjectMutationExecutor,
  type ProjectCommandRequester,
  type ProjectMutationIntent,
  type ProjectMutationOutcome
} from './project-command'
import { syncProjectRuntime } from './project-runtime'

export type {
  ProjectCommandResult,
  ProjectMutationIntent,
  ProjectMutationName,
  ProjectMutationOutcome
} from './project-command'

interface ProjectCommandRuntimeConfiguration {
  request: ProjectCommandRequester
  scope: string | undefined
}

const unavailableRequest: ProjectCommandRequester = () =>
  Promise.reject(new Error('project command runtime is not configured'))

const executor = createProjectMutationExecutor({
  createIdempotencyKey: () => globalThis.crypto.randomUUID(),
  request: unavailableRequest,
  sync: syncProjectRuntime
})

let activeConfiguration: ProjectCommandRuntimeConfiguration | undefined

function isConfigured(): boolean {
  return activeConfiguration !== undefined
}

export function configureProjectCommandRuntime(request: ProjectCommandRequester, scope?: string): () => void {
  const configuration = { request, scope }

  activeConfiguration = configuration
  executor.configure(request, scope)

  return () => {
    if (activeConfiguration !== configuration) {
      return
    }

    activeConfiguration = undefined
    executor.configure(unavailableRequest)
  }
}

export function executeProjectMutation(intent: ProjectMutationIntent): Promise<ProjectMutationOutcome> {
  if (!isConfigured()) {
    return Promise.reject(new Error('project command runtime is not configured'))
  }

  return executor.executeProjectMutation(intent)
}

export function retryProjectMutation(intentId: string): Promise<ProjectMutationOutcome> {
  if (!isConfigured()) {
    return Promise.reject(new Error('project command runtime is not configured'))
  }

  return executor.retry(intentId)
}

export function isProjectMutationRetryAvailable(intentId: string): boolean {
  return isConfigured() && executor.hasPendingRetry(intentId)
}
