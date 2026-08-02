import { JsonRpcGatewayError } from '@hermes/shared'
import { atom } from 'nanostores'

import { liveSessionProjectId, type SidebarProjectTree } from '@/app/chat/sidebar/projects/workspace-groups'
import type { HermesGitBaseBranch, HermesGitBranch } from '@/global'
import { getHermesConfig, type HermesGateway } from '@/hermes'
import { translateNow } from '@/i18n'
import { desktopDefaultCwd, isDesktopFsRemoteMode, selectDesktopPaths, writeDesktopFileText } from '@/lib/desktop-fs'
import { desktopGit } from '@/lib/desktop-git'
import { isMissingRpcMethod } from '@/lib/gateway-rpc'
import { persistentAtom } from '@/lib/persisted'
import { $gateway, activeGateway, ensureActiveGatewayOpen } from '@/store/gateway'
import { setSidebarAgentsGrouped } from '@/store/layout'
import { notify, notifyError } from '@/store/notifications'
import { $activeGatewayProfile, requestFreshSession } from '@/store/profile'
import {
  executeProjectMutation,
  type ProjectCommandResult,
  type ProjectMutationIntent,
  type ProjectMutationName,
  type ProjectMutationOutcome,
  retryProjectMutation
} from '@/store/project-command-runtime'
import { $projectRuntimes, projectRuntimeAuthority, type ProjectRuntimeState } from '@/store/project-runtime'
import { type ManagedProjectSurfaceResolution, resolveManagedProjectSurface } from '@/store/project-surface-authority'
import { $selectedStoredSessionId, $sessions, sessionMatchesStoredId, workspaceCwdForNewSession } from '@/store/session'
import type { ProjectInfo, ProjectsPayload } from '@/types/hermes'

// First-class, per-profile Projects (named, multi-folder workspaces). State is
// served by the live gateway's `projects.*` JSON-RPC methods, which wrap the
// per-profile projects.db store. The sidebar groups sessions by project folder
// membership; these atoms are the renderer's cached view.

export const $projects = atom<ProjectInfo[]>([])
export const $activeProjectId = atom<null | string>(null)

export interface ProjectCatalogAuthority {
  catalogGeneration: null | number
  contextGeneration: number
  profile: null | string
}

export const $projectCatalogAuthority = atom<ProjectCatalogAuthority>({
  catalogGeneration: null,
  contextGeneration: 0,
  profile: null
})

let observedProjectCatalogProfile = $activeGatewayProfile.get() || 'default'
let observedProjectCatalogGateway = $gateway.get()
let projectCatalogRequestGeneration = 0

export function projectCatalogScope(): null | string {
  return $projectCatalogAuthority.get().profile
}

export interface ProjectDialogState {
  mode: 'add-folder' | 'create' | 'rename'
  name?: string
  projectId?: string
}

export interface PendingProjectMutation {
  phase: 'executing' | 'retry_required'
}

export const $pendingProjectMutations = atom<Readonly<Record<string, PendingProjectMutation>>>({})

export function projectMutationPendingKey(name: ProjectMutationName, projectId: null | string): string {
  return name === 'project.create' ? 'project/create' : `project/${projectId ?? 'global'}/${name}`
}

// The authoritative project -> repo -> lane tree (overview), served by
// `projects.tree`. Lanes carry counts + structure; per-project session rows are
// fetched lazily on drill-in via `fetchProjectSessions`. This is the single
// source of project membership — the desktop no longer derives it.
export const $projectTree = atom<SidebarProjectTree[]>([])
export const $projectTreeLoading = atom(false)

// False when the connected backend predates the projects.* JSON-RPC surface
// (same semver label, older install). Null until the first probe.
export const $projectsRpcAvailable = atom<boolean | null>(null)

function markProjectsRpcSuccess(): void {
  $projectsRpcAvailable.set(true)
}

function markProjectsRpcFailure(err: unknown): void {
  if (isMissingRpcMethod(err)) {
    $projectsRpcAvailable.set(false)
  }
}

function projectsStaleBackendError(): Error {
  return new Error(translateNow('sidebar.projects.staleBackend'))
}

// Client-side cache eviction (Apollo-style optimistic layer): ids the user just
// deleted/archived. The backend tree is a snapshot that still lists them until
// its next refresh, so the render-time overlay strips these so the tree matches
// the live `$sessions` cache exactly — same as the flat Recents list. Pruned on
// refresh once the server snapshot has caught up.
export const $removedSessionIds = atom<Set<string>>(new Set())

export function tombstoneSessions(ids: Array<null | string | undefined>): void {
  const next = new Set($removedSessionIds.get())
  const before = next.size

  for (const id of ids) {
    const trimmed = id?.trim()

    if (trimmed) {
      next.add(trimmed)
    }
  }

  if (next.size !== before) {
    $removedSessionIds.set(next)
  }
}

export function untombstoneSessions(ids: Array<null | string | undefined>): void {
  const current = $removedSessionIds.get()

  if (!current.size) {
    return
  }

  const next = new Set(current)

  for (const id of ids) {
    const trimmed = id?.trim()

    if (trimmed) {
      next.delete(trimmed)
    }
  }

  if (next.size !== current.size) {
    $removedSessionIds.set(next)
  }
}

// Ids whose delete/archive RPC is still in flight. Their tombstones are pinned
// against the projects.tree prune below: a refresh whose snapshot predates the
// mutation completing must NOT drop the tombstone, or the row flashes back until
// the backend catches up. Keyed by id, so concurrent deletes stay independent.
export const $sessionMutationsInFlight = atom<Set<string>>(new Set())

function mutateInFlight(ids: Array<null | string | undefined>, add: boolean): void {
  const current = $sessionMutationsInFlight.get()
  const next = new Set(current)

  for (const id of ids) {
    const trimmed = id?.trim()

    if (trimmed) {
      add ? next.add(trimmed) : next.delete(trimmed)
    }
  }

  if (next.size !== current.size) {
    $sessionMutationsInFlight.set(next)
  }
}

export const beginSessionMutation = (ids: Array<null | string | undefined>): void => mutateInFlight(ids, true)
export const endSessionMutation = (ids: Array<null | string | undefined>): void => mutateInFlight(ids, false)

// True while the disk scan is in flight (drives the "finding repos" hint).
export const $reposScanning = atom(false)

// ── Project scope (the "you're inside a project" view, mirroring profile scope)─
// The sidebar's grouped view is a project switcher: ALL_PROJECTS shows the
// project overview (a list you drill into), and a concrete id means you've
// "entered" that project so only its worktrees/branches/sessions show. This is
// pure view state (localStorage), distinct from the durable active-project
// pointer in projects.db — though entering a project also makes it active so new
// chats land there, exactly as selecting a profile does.
export const ALL_PROJECTS = '__all_projects__'

const PROJECT_SCOPE_KEY = 'hermes.desktop.projectScope'

export const $projectScope = persistentAtom<string>(PROJECT_SCOPE_KEY, ALL_PROJECTS, {
  decode: raw => raw || ALL_PROJECTS,
  encode: value => value || ALL_PROJECTS
})

// Enter a project: scope the sidebar to it and make it the active project
// (best-effort — the durable pointer is nice-to-have, the view scope is the
// point). Never opens a session.
export function enterProject(id: string): void {
  $projectScope.set(id)

  // Only explicit, persisted projects (ids are `p_<hex>`) become active. Auto
  // projects (ids are filesystem paths) and the "No project" bucket have no
  // durable row to pin, so they're view-scope only.
  if (id.startsWith('p_')) {
    void setActiveProject(id).catch(() => undefined)
  }
}

export function exitProjectScope(): void {
  $projectScope.set(ALL_PROJECTS)
}

// The cwd a NEW chat should start in. The "active project" is just an atom
// ($projectScope) — so when you're inside a project, a new session (cmd-n, the
// trunk "+") starts at that project's root (its primary repo = the default-branch
// checkout) instead of inheriting whatever unrelated worktree the live cwd
// drifted into. Outside a project it falls back to the plain default (detached),
// so a bare new chat shows no branch.
export function resolveNewSessionCwd(): string {
  const scope = $projectScope.get()

  if (scope !== ALL_PROJECTS) {
    const project = $projectTree.get().find(node => node.id === scope)
    const cwd = (project?.path || project?.repos.find(repo => repo.path)?.path || '').trim()

    if (cwd) {
      return cwd
    }
  }

  return workspaceCwdForNewSession()
}

const underPath = (parent: string, child: string): boolean =>
  child === parent || child.startsWith(parent.endsWith('/') ? parent : `${parent}/`)

// The project (explicit or auto) that owns `cwd`, by longest path match across
// the live tree. Null when no project covers it (it'll surface as a fresh
// auto-project on the next tree refresh).
export function projectIdForCwd(cwd: string): null | string {
  let best: null | string = null
  let bestLen = -1

  for (const project of $projectTree.get()) {
    // Match project + repo roots AND each worktree-lane path: a linked worktree
    // (e.g. a sibling `repo-retry`) lives OUTSIDE the repo root, so root-prefix
    // matching alone would miss it — but it's still part of the project.
    const paths = [project.path, ...project.repos.flatMap(repo => [repo.path, ...repo.groups.map(group => group.path)])]

    for (const path of paths) {
      const p = (path || '').trim()

      if (p && underPath(p, cwd) && p.length > bestLen) {
        bestLen = p.length
        best = project.id
      }
    }
  }

  return best
}

// The display NAME of the explicit, named project owning `cwd` (longest path
// match), or null when the cwd sits in no named project. The status bar reads
// this to label the workspace by project instead of the bare cwd leaf. We skip
// auto-projects (a repo root promoted with no projects.db row) and the synthetic
// "No project" bucket on purpose: those have no human name, so their sessions
// keep the cwd-leaf label — matching the backend `_project_info_for_cwd`, which
// only resolves projects.db rows, so the desktop and TUI name the same session
// identically without threading a second per-session copy through session.info.
export function projectNameForCwd(cwd: string): null | string {
  const target = (cwd || '').trim()

  if (!target) {
    return null
  }

  let best: null | string = null
  let bestLen = -1

  for (const project of $projectTree.get()) {
    if (project.isAuto || project.isNoProject) {
      continue
    }

    const paths = [project.path, ...project.repos.flatMap(repo => [repo.path, ...repo.groups.map(group => group.path)])]

    for (const path of paths) {
      const p = (path || '').trim()

      if (p && underPath(p, target) && p.length > bestLen) {
        bestLen = p.length
        best = project.label
      }
    }
  }

  return best
}

// The active session's agent relocated itself (created/entered another repo or
// worktree via the terminal — backend re-anchors its cwd and emits session.info).
// Re-pull projects + tree so a freshly created/auto project and the relocated
// session row show live, then follow the view into the session's new project
// (from the overview or a now-stale project alike). Caller gates this on a real
// same-session cwd move, so a plain session switch never reaches here.
export async function followActiveSessionCwd(cwd: string): Promise<void> {
  const target = cwd.trim()

  if (!target) {
    return
  }

  await Promise.all([refreshProjects(), refreshProjectTree()])

  // Resolve only after the refresh, so a just-created/auto project is in the tree.
  const projectId = projectIdForCwd(target)

  if (projectId) {
    // The Projects tree only renders in grouped mode, so flip the sidebar into
    // it — otherwise following from the flat Sessions list would change scope
    // invisibly. Then drill into the thread's project.
    setSidebarAgentsGrouped(true)

    if (projectId !== $projectScope.get()) {
      enterProject(projectId)
    }
  }
}

// Issue a request on whichever gateway is currently active, reconnecting once
// if the socket dropped. Projects are per-profile, so they intentionally follow
// the active gateway just like the session list does.
async function gatewayRequest<T>(method: string, params: Record<string, unknown> = {}): Promise<T> {
  let gateway = activeGateway()

  if (!gateway || gateway.connectionState !== 'open') {
    gateway = await ensureActiveGatewayOpen()
  }

  if (!gateway) {
    throw new Error('Hermes gateway is not connected')
  }

  return gateway.request<T>(method, params)
}

async function gatewayRequestOn<T>(
  gateway: HermesGateway,
  method: string,
  params: Record<string, unknown> = {}
): Promise<T> {
  return gateway.request<T>(method, params)
}

interface ActiveProjectsContext {
  contextGeneration: number
  gateway: HermesGateway
  profile: string
}

async function activeProjectsContext(): Promise<ActiveProjectsContext> {
  const profile = $activeGatewayProfile.get() || 'default'
  const contextGeneration = $projectCatalogAuthority.get().contextGeneration
  let gateway = activeGateway()

  if (!gateway || gateway.connectionState !== 'open') {
    gateway = await ensureActiveGatewayOpen()
  }

  if (
    !gateway ||
    gateway !== activeGateway() ||
    profile !== ($activeGatewayProfile.get() || 'default') ||
    contextGeneration !== $projectCatalogAuthority.get().contextGeneration
  ) {
    throw new Error('Active Hermes profile changed while connecting')
  }

  return { contextGeneration, gateway, profile }
}

interface ProjectMutationContext {
  gateway: HermesGateway | null
  profile: string
}

type MutationSuccessHandler = (result: ProjectCommandResult, retried: boolean) => void

interface ProjectMutationOwner {
  context: ProjectMutationContext
  intentId: string | null
  key: string
  onSuccess: MutationSuccessHandler | undefined
  phase: PendingProjectMutation['phase']
  token: symbol
}

const pendingProjectMutationOwners = new Map<string, ProjectMutationOwner>()

function activeProfileId(): string {
  return $activeGatewayProfile.get() || 'default'
}

function captureProjectMutationContext(): ProjectMutationContext {
  return { gateway: activeGateway() ?? null, profile: activeProfileId() }
}

function isProjectMutationContextCurrent(context: ProjectMutationContext): boolean {
  return (
    context.profile === activeProfileId() && (context.gateway === null || context.gateway === (activeGateway() ?? null))
  )
}

function isProjectCatalogContextCurrent(context: ActiveProjectsContext): boolean {
  return (
    context.contextGeneration === $projectCatalogAuthority.get().contextGeneration &&
    isProjectMutationContextCurrent(context)
  )
}

function publishPendingProjectMutations(): void {
  $pendingProjectMutations.set(
    Object.fromEntries([...pendingProjectMutationOwners].map(([key, owner]) => [key, { phase: owner.phase }]))
  )
}

function releaseProjectMutation(owner: ProjectMutationOwner): void {
  if (pendingProjectMutationOwners.get(owner.key)?.token !== owner.token) {
    return
  }

  pendingProjectMutationOwners.delete(owner.key)
  publishPendingProjectMutations()
}

function clearPendingProjectMutations(): void {
  if (!pendingProjectMutationOwners.size) {
    return
  }

  pendingProjectMutationOwners.clear()
  publishPendingProjectMutations()
}

function claimProjectMutation(
  intent: ProjectMutationIntent,
  onSuccess?: MutationSuccessHandler,
  context = captureProjectMutationContext()
): ProjectMutationOwner | null {
  const key = projectMutationPendingKey(intent.name, intent.project_id)
  const existing = pendingProjectMutationOwners.get(key)

  if (existing && existing.context.profile !== activeProfileId()) {
    releaseProjectMutation(existing)
  } else if (existing) {
    return null
  }

  const owner: ProjectMutationOwner = {
    context,
    intentId: null,
    key,
    onSuccess,
    phase: 'executing',
    token: Symbol(key)
  }

  pendingProjectMutationOwners.set(key, owner)
  publishPendingProjectMutations()

  return owner
}

let pendingMutationProfile = activeProfileId()

function synchronizeProjectCatalogContext(): void {
  const profile = activeProfileId()
  const gateway = $gateway.get()

  if (profile === observedProjectCatalogProfile && gateway === observedProjectCatalogGateway) {
    return
  }

  observedProjectCatalogProfile = profile
  observedProjectCatalogGateway = gateway

  const authority = $projectCatalogAuthority.get()

  $projectCatalogAuthority.set({
    ...authority,
    contextGeneration: authority.contextGeneration + 1
  })
}

$activeGatewayProfile.subscribe(() => {
  synchronizeProjectCatalogContext()

  const profile = activeProfileId()

  if (profile !== pendingMutationProfile) {
    pendingMutationProfile = profile
    clearPendingProjectMutations()
  }
})

$gateway.subscribe(synchronizeProjectCatalogContext)

function normalizeProjectInfo(project: ProjectInfo): ProjectInfo {
  return { ...project }
}

export function resolveProjectManagementAuthority(
  projectId: string,
  projects: readonly ProjectInfo[] = $projects.get(),
  runtimes: Readonly<Record<string, ProjectRuntimeState>> = $projectRuntimes.get()
): ManagedProjectSurfaceResolution {
  return resolveManagedProjectSurface({
    activeProfile: activeProfileId(),
    activeProjectId: projectId,
    catalogAuthority: $projectCatalogAuthority.get(),
    projects,
    runtimeAuthority: projectRuntimeAuthority(),
    runtimes: { ...runtimes },
    sessions: []
  })
}

/** True means the project is protected from legacy mutation. This includes a
 * managed runtime as well as boot/profile/catalog gaps where legacy ownership
 * has not yet been proved. */
export function isEffectivelyManagedProject(
  projectId: string,
  projects: readonly ProjectInfo[] = $projects.get(),
  runtimes: Readonly<Record<string, ProjectRuntimeState>> = $projectRuntimes.get()
): boolean {
  return resolveProjectManagementAuthority(projectId, projects, runtimes).status !== 'conclusively-legacy'
}

function requireConclusiveLegacyProject(projectId: string): void {
  const resolution = resolveProjectManagementAuthority(projectId)

  if (resolution.status !== 'conclusively-legacy') {
    throw new Error(
      resolution.status === 'managed'
        ? 'managed project mutations are canonical-only'
        : 'project authority is unavailable'
    )
  }
}

function applyPayload(payload: ProjectsPayload, profile: string, contextGeneration: number): boolean {
  const authority = $projectCatalogAuthority.get()

  if (contextGeneration !== authority.contextGeneration || profile !== activeProfileId()) {
    return false
  }

  $projectCatalogAuthority.set({
    catalogGeneration: contextGeneration,
    contextGeneration: authority.contextGeneration,
    profile
  })
  $projects.set((payload.projects ?? []).map(normalizeProjectInfo))
  $activeProjectId.set(payload.active_id ?? null)

  return true
}

// Pull the full project list + active pointer. Best-effort: a failure (gateway
// not up yet) leaves the cached atoms intact so the sidebar doesn't flicker.
export async function refreshProjects(): Promise<void> {
  const requestGeneration = ++projectCatalogRequestGeneration
  let context: ActiveProjectsContext | null = null

  try {
    context = await activeProjectsContext()
    const payload = await gatewayRequestOn<ProjectsPayload>(context.gateway, 'projects.list')

    if (requestGeneration !== projectCatalogRequestGeneration || !isProjectCatalogContextCurrent(context)) {
      return
    }

    if (applyPayload(payload, context.profile, context.contextGeneration)) {
      markProjectsRpcSuccess()
    }
  } catch (err) {
    if (
      requestGeneration === projectCatalogRequestGeneration &&
      (context === null || isProjectCatalogContextCurrent(context))
    ) {
      markProjectsRpcFailure(err)
    }

    // Backend may not be ready; keep the last known list.
  }
}

async function refreshProjectsForContext(
  context: ProjectMutationContext,
  projectId: string
): Promise<ProjectInfo | null> {
  if (!context.gateway || !isProjectMutationContextCurrent(context)) {
    return null
  }

  const contextGeneration = $projectCatalogAuthority.get().contextGeneration
  const requestGeneration = ++projectCatalogRequestGeneration

  try {
    const payload = await gatewayRequestOn<ProjectsPayload>(context.gateway, 'projects.list')

    if (
      requestGeneration !== projectCatalogRequestGeneration ||
      contextGeneration !== $projectCatalogAuthority.get().contextGeneration ||
      !isProjectMutationContextCurrent(context)
    ) {
      return null
    }

    if (!applyPayload(payload, context.profile, contextGeneration)) {
      return null
    }

    markProjectsRpcSuccess()

    return $projects.get().find(project => project.id === projectId) ?? null
  } catch (error) {
    if (
      requestGeneration === projectCatalogRequestGeneration &&
      contextGeneration === $projectCatalogAuthority.get().contextGeneration &&
      isProjectMutationContextCurrent(context)
    ) {
      markProjectsRpcFailure(error)
    }

    return null
  }
}

interface ProjectTreePayload {
  projects: SidebarProjectTree[]
  active_id: null | string
  scoped_session_ids: string[]
}

let projectTreeRefreshGeneration = 0

async function refreshProjectTreeOn(gateway: HermesGateway, profile = activeProfileId()): Promise<void> {
  const generation = ++projectTreeRefreshGeneration
  const context = { gateway, profile }

  if (isProjectMutationContextCurrent(context)) {
    $projectTreeLoading.set(true)
  }

  try {
    const res = await gatewayRequestOn<ProjectTreePayload>(gateway, 'projects.tree', {
      preview_limit: 3
    })

    if (generation !== projectTreeRefreshGeneration || !isProjectMutationContextCurrent(context)) {
      return
    }

    const scoped = new Set(res.scoped_session_ids ?? [])
    $projectTree.set(res.projects ?? [])
    $activeProjectId.set(res.active_id ?? null)
    const tombstones = $removedSessionIds.get()

    if (tombstones.size) {
      // Keep a tombstone while the backend still lists the id (delete pending on
      // its side) OR while its mutation is still in flight locally — dropping it
      // early flashes the row back until the RPC lands.
      const inFlight = $sessionMutationsInFlight.get()
      const pending = new Set([...tombstones].filter(id => scoped.has(id) || inFlight.has(id)))

      if (pending.size !== tombstones.size) {
        $removedSessionIds.set(pending)
      }
    }

    markProjectsRpcSuccess()
  } catch (err) {
    if (isProjectMutationContextCurrent(context)) {
      markProjectsRpcFailure(err)
    }
  } finally {
    if (generation === projectTreeRefreshGeneration && isProjectMutationContextCurrent(context)) {
      $projectTreeLoading.set(false)
    }
  }
}

// Pull the authoritative project tree (overview structure + counts + preview
// sessions + the scoped-session-id set). Best-effort: a failure leaves the
// cached tree intact so the sidebar doesn't flicker.
export async function refreshProjectTree(): Promise<void> {
  try {
    const context = await activeProjectsContext()
    await refreshProjectTreeOn(context.gateway, context.profile)
  } catch {
    // Backend may not be ready; keep the last known tree.
  }
}

// Fully hydrated lanes (repo -> lane -> session rows) for one project, fetched
// when the user enters it. Same backend grouping as `projects.tree`, so ids and
// membership match exactly.
export async function fetchProjectSessions(projectId: string): Promise<SidebarProjectTree | null> {
  try {
    const res = await gatewayRequest<{ project: SidebarProjectTree | null }>('projects.project_sessions', {
      project_id: projectId
    })

    return res.project ?? null
  } catch {
    return null
  }
}

export interface RepoDiscoveryPolicy {
  enabled: boolean
  roots: string[]
  exclude_paths: string[]
}

export function repoDiscoveryPolicyFromConfig(config: unknown): RepoDiscoveryPolicy {
  const desktopValue = config && typeof config === 'object' ? (config as { desktop?: unknown }).desktop : undefined

  const desktop =
    desktopValue && typeof desktopValue === 'object'
      ? (desktopValue as {
          repo_scan_enabled?: unknown
          repo_scan_exclude_paths?: unknown
          repo_scan_roots?: unknown
        })
      : {}

  return {
    enabled: desktop.repo_scan_enabled !== false,
    roots: Array.isArray(desktop.repo_scan_roots)
      ? desktop.repo_scan_roots.filter((value): value is string => typeof value === 'string')
      : [],
    exclude_paths: Array.isArray(desktop.repo_scan_exclude_paths)
      ? desktop.repo_scan_exclude_paths.filter((value): value is string => typeof value === 'string')
      : []
  }
}

export function repoDiscoveryPolicySignature(policy: RepoDiscoveryPolicy): string {
  return JSON.stringify(policy)
}

interface RepoScanState {
  completedSignature?: string
  generation: number
  runningSignature?: string
}

const repoScanStates = new WeakMap<HermesGateway, RepoScanState>()
const scanningGatewayGenerations = new WeakMap<HermesGateway, number>()

function syncReposScanning(): void {
  const gateway = activeGateway()
  $reposScanning.set(Boolean(gateway && scanningGatewayGenerations.has(gateway)))
}

$gateway.subscribe(syncReposScanning)

export async function scanAndRecordRepos(force = false): Promise<void> {
  if (isDesktopFsRemoteMode()) {
    return
  }

  let context: ActiveProjectsContext

  try {
    context = await activeProjectsContext()
  } catch {
    return
  }

  const scan = desktopGit()?.scanRepos

  if (!scan) {
    return
  }

  const state = repoScanStates.get(context.gateway) ?? { generation: 0 }
  repoScanStates.set(context.gateway, state)
  let generation: number | undefined

  try {
    const policy = repoDiscoveryPolicyFromConfig(await getHermesConfig(context.profile))
    const signature = repoDiscoveryPolicySignature(policy)

    if (!force && (state.completedSignature === signature || state.runningSignature === signature)) {
      return
    }

    generation = ++state.generation
    state.runningSignature = signature

    if (!policy.enabled) {
      await gatewayRequestOn(context.gateway, 'projects.record_repos', {
        discovery_policy: policy,
        repos: []
      })
    } else {
      scanningGatewayGenerations.set(context.gateway, generation)
      syncReposScanning()

      const repos = await scan(policy.roots, {
        enabled: true,
        excludePaths: policy.exclude_paths
      })

      if (state.generation !== generation) {
        return
      }

      await gatewayRequestOn(context.gateway, 'projects.record_repos', {
        discovery_policy: policy,
        repos
      })
    }

    if (state.generation !== generation) {
      return
    }

    state.completedSignature = signature
    await refreshProjectTreeOn(context.gateway, context.profile)
  } catch {
    state.completedSignature = undefined
  } finally {
    state.runningSignature = undefined

    if (scanningGatewayGenerations.get(context.gateway) === generation) {
      scanningGatewayGenerations.delete(context.gateway)
    }

    syncReposScanning()
  }
}

export interface CreateProjectInput {
  name: string
  folders?: string[]
  primaryPath?: string
  slug?: string
  description?: string
  icon?: string
  color?: string
  boardSlug?: string
  use?: boolean
  // Free-text project idea; written to IDEA.md at the primary folder on create.
  idea?: string
}

export interface CreateProjectOptions {
  dialog?: ProjectDialogState
}

// Generate a project idea via the stateless llm.oneshot RPC (inherits the live
// session's model when one exists). Returns "" on failure so the caller can just
// leave the field untouched. The "🎲" affordance in the new-project dialog.
export async function generateProjectIdea(name: string): Promise<string> {
  try {
    const res = await gatewayRequest<{ text: string }>('llm.oneshot', {
      instructions:
        'You generate a single, concrete project idea as a short IDEA.md body: a one-line summary, ' +
        'then 3-5 bullet goals. No preamble, no code fences, under 120 words.',
      input: name.trim() ? `Project name: ${name.trim()}` : 'Surprise me with a fun project.',
      temperature: 1.0
    })

    return (res.text || '').trim()
  } catch {
    return ''
  }
}

// Write IDEA.md to a project's primary folder (best-effort). Routes through the
// remote-aware fs write, so it lands on the backend for a remote gateway and on
// disk locally — the project is created regardless of whether the file lands.
async function writeProjectIdea(folder: null | string | undefined, idea: string): Promise<void> {
  const dir = (folder || '').trim()
  const body = idea.trim()

  if (!dir || !body) {
    return
  }

  try {
    await writeDesktopFileText(`${dir.replace(/[/\\]+$/, '')}/IDEA.md`, body.endsWith('\n') ? body : `${body}\n`)
  } catch {
    // Best-effort: the project is created regardless of whether IDEA.md lands.
  }
}

// ── Optimistic cache layer ───────────────────────────────────────────────────
// The project cache (list + tree + active pointer) mutates instantly on user
// action; the write reconciles in the background and rolls the whole cache back
// on failure — the same Apollo-style layer the session list uses.

interface ProjectsSnapshot {
  projects: ProjectInfo[]
  tree: SidebarProjectTree[]
  active: null | string
}

const snapshotProjects = (): ProjectsSnapshot => ({
  projects: $projects.get(),
  tree: $projectTree.get(),
  active: $activeProjectId.get()
})

const restoreProjects = ({ projects, tree, active }: ProjectsSnapshot): void => {
  $projects.set(projects)
  $projectTree.set(tree)
  $activeProjectId.set(active)
}

// Await an already-applied optimistic write; restore the snapshot if it throws.
async function persistOrRollback(snap: ProjectsSnapshot, write: () => Promise<void>): Promise<void> {
  try {
    await write()
  } catch (err) {
    restoreProjects(snap)
    throw err
  }
}

const reconcileProjects = (): void => {
  void refreshProjects()
  void refreshProjectTree()
}

// Map a ProjectInfo (list shape) onto a minimal overview tree node so a created
// project paints instantly. The backend seeds each folder as an (empty) repo, so
// the next tree refresh fills in repos/counts; this is just the optimistic stub.
function projectInfoToTreeNode(project: ProjectInfo): SidebarProjectTree {
  return {
    id: project.id,
    label: project.name || project.id,
    path: project.primary_path ?? project.folders?.[0]?.path ?? null,
    color: project.color ?? null,
    icon: project.icon ?? null,
    isAuto: false,
    repos: [],
    sessionCount: 0,
    previewSessions: []
  }
}

async function legacyGatewayForContext(context: ProjectMutationContext): Promise<HermesGateway> {
  if (context.gateway) {
    if (!isProjectMutationContextCurrent(context)) {
      throw new Error('Active Hermes profile changed during project creation')
    }

    return context.gateway
  }

  if (!isProjectMutationContextCurrent(context)) {
    throw new Error('Active Hermes profile changed during project creation')
  }

  const connected = await activeProjectsContext()

  if (connected.profile !== context.profile) {
    throw new Error('Active Hermes profile changed during project creation')
  }

  context.gateway = connected.gateway

  return connected.gateway
}

async function createLegacyProject(
  input: CreateProjectInput,
  context: ProjectMutationContext
): Promise<ProjectInfo | null> {
  let res: { project: ProjectInfo | null }

  try {
    const gateway = await legacyGatewayForContext(context)

    res = await gatewayRequestOn<{ project: ProjectInfo | null }>(gateway, 'projects.create', {
      name: input.name,
      folders: input.folders ?? [],
      primary_path: input.primaryPath,
      slug: input.slug,
      description: input.description,
      icon: input.icon,
      color: input.color,
      board_slug: input.boardSlug,
      use: input.use ?? false
    })
  } catch (err) {
    if (isMissingRpcMethod(err)) {
      if (isProjectMutationContextCurrent(context)) {
        $projectsRpcAvailable.set(false)
      }

      throw projectsStaleBackendError()
    }

    throw err
  }

  // Not optimistic (the create awaits the RPC first, so there's nothing to roll
  // back): apply the server's row into the cached list + tree at once, so it
  // (and an entered scope) shows without waiting on the background refreshes
  // that reconcile counts/repos.
  const created = res.project === null ? null : normalizeProjectInfo(res.project)

  if (!isProjectMutationContextCurrent(context)) {
    return created
  }

  markProjectsRpcSuccess()

  if (created) {
    if (input.idea && isProjectMutationContextCurrent(context)) {
      void writeProjectIdea(created.primary_path ?? created.folders?.[0]?.path ?? input.primaryPath, input.idea)
    }

    if (isProjectMutationContextCurrent(context) && !$projects.get().some(proj => proj.id === created.id)) {
      $projects.set([...$projects.get(), created])
    }

    if (isProjectMutationContextCurrent(context) && !$projectTree.get().some(node => node.id === created.id)) {
      $projectTree.set([projectInfoToTreeNode(created), ...$projectTree.get()])
    }

    if (input.use && isProjectMutationContextCurrent(context)) {
      $activeProjectId.set(created.id)
    }

    if (isProjectMutationContextCurrent(context)) {
      setSidebarAgentsGrouped(true)
    }
  }

  if (isProjectMutationContextCurrent(context)) {
    runCanonicalProjectReconcile(context, created?.id ?? '')
  }

  return created
}

const canonicalCreatePayload = (input: CreateProjectInput): Record<string, unknown> => ({
  name: input.name,
  folders: input.folders ?? [],
  ...(input.primaryPath !== undefined && { primary_path: input.primaryPath }),
  ...(input.slug !== undefined && { slug: input.slug }),
  ...(input.description !== undefined && { description: input.description }),
  ...(input.icon !== undefined && { icon: input.icon }),
  ...(input.color !== undefined && { color: input.color }),
  ...(input.boardSlug !== undefined && { board_slug: input.boardSlug })
})

function isCanonicalMethodMissing(error: unknown): boolean {
  return error instanceof JsonRpcGatewayError && error.code === -32601
}

class HandledProjectMutationError extends Error {
  constructor() {
    super('canonical project mutation feedback was shown')
    this.name = 'HandledProjectMutationError'
  }
}

export function isHandledProjectMutationError(error: unknown): boolean {
  return error instanceof HandledProjectMutationError
}

function notifyMutationConflict(): void {
  notify({
    kind: 'warning',
    message: translateNow('sidebar.projects.mutationConflict')
  })
}

function notifyMutationRetry(owner: ProjectMutationOwner): void {
  notify({
    action: {
      label: translateNow('common.retry'),
      onClick: () => {
        void retryVisibleProjectMutation(owner)
      }
    },
    kind: 'warning',
    message: translateNow('sidebar.projects.mutationRetry')
  })
}

function settleProjectMutation(
  outcome: ProjectMutationOutcome,
  owner: ProjectMutationOwner,
  retried: boolean
): ProjectCommandResult {
  if (outcome.status === 'succeeded') {
    releaseProjectMutation(owner)

    try {
      owner.onSuccess?.(outcome.result, retried)
    } catch (error) {
      notifyError(error, translateNow('sidebar.projects.mutationFailed'))
    }

    return outcome.result
  }

  if (pendingProjectMutationOwners.get(owner.key)?.token !== owner.token) {
    throw new HandledProjectMutationError()
  }

  if (outcome.status === 'conflict') {
    releaseProjectMutation(owner)
    notifyMutationConflict()
  } else {
    owner.intentId = outcome.intent_id
    owner.phase = 'retry_required'
    publishPendingProjectMutations()
    notifyMutationRetry(owner)
  }

  throw new HandledProjectMutationError()
}

async function retryVisibleProjectMutation(owner: ProjectMutationOwner): Promise<void> {
  if (
    pendingProjectMutationOwners.get(owner.key)?.token !== owner.token ||
    owner.intentId === null ||
    owner.phase !== 'retry_required'
  ) {
    return
  }

  if (!isProjectMutationContextCurrent(owner.context)) {
    releaseProjectMutation(owner)

    return
  }

  const intentId = owner.intentId
  owner.phase = 'executing'
  publishPendingProjectMutations()

  try {
    settleProjectMutation(await retryProjectMutation(intentId), owner, true)
  } catch (error) {
    if (!isHandledProjectMutationError(error)) {
      releaseProjectMutation(owner)
      notifyError(error, translateNow('sidebar.projects.mutationFailed'))
    }
  }
}

export async function executeProjectMutationWithFeedback(
  intent: ProjectMutationIntent,
  onSuccess?: MutationSuccessHandler
): Promise<ProjectCommandResult> {
  const owner = claimProjectMutation(intent, onSuccess)

  if (!owner) {
    throw new HandledProjectMutationError()
  }

  let outcome: ProjectMutationOutcome

  try {
    outcome = await executeProjectMutation(intent)
  } catch (error) {
    releaseProjectMutation(owner)
    notifyError(error, translateNow('sidebar.projects.mutationFailed'))
    throw new HandledProjectMutationError()
  }

  return settleProjectMutation(outcome, owner, false)
}

async function setActiveProjectForContext(context: ProjectMutationContext, projectId: string): Promise<void> {
  if (!context.gateway || !isProjectMutationContextCurrent(context)) {
    return
  }

  const result = await gatewayRequestOn<{ active_id: null | string }>(context.gateway, 'projects.set_active', {
    id: projectId
  })

  if (isProjectMutationContextCurrent(context)) {
    $activeProjectId.set(result.active_id ?? null)
  }
}

async function runCanonicalCreateFollowUps(
  context: ProjectMutationContext,
  input: CreateProjectInput,
  projectId: string
): Promise<void> {
  const createdProject = refreshProjectsForContext(context, projectId)

  if (context.gateway) {
    void refreshProjectTreeOn(context.gateway, context.profile)
  }

  if (input.use) {
    void setActiveProjectForContext(context, projectId).catch(error => {
      if (isProjectMutationContextCurrent(context)) {
        notifyError(error, translateNow('sidebar.projects.activationFailed'))
      }
    })
  }

  const created = await createdProject

  if (!isProjectMutationContextCurrent(context)) {
    return
  }

  if (input.idea) {
    void writeProjectIdea(created?.primary_path ?? created?.folders?.[0]?.path ?? input.primaryPath, input.idea)
  }

  if (isProjectMutationContextCurrent(context)) {
    setSidebarAgentsGrouped(true)
  }
}

function runCanonicalProjectReconcile(context: ProjectMutationContext, projectId: string): void {
  void Promise.all([
    refreshProjectsForContext(context, projectId),
    context.gateway ? refreshProjectTreeOn(context.gateway, context.profile) : Promise.resolve()
  ])
}

export async function createProject(
  input: CreateProjectInput,
  options: CreateProjectOptions = {}
): Promise<ProjectInfo | null> {
  if ($projectsRpcAvailable.get() === false) {
    throw projectsStaleBackendError()
  }

  const intent: ProjectMutationIntent = {
    expected_version: 0,
    name: 'project.create',
    payload: canonicalCreatePayload(input),
    project_id: null
  }

  const context = captureProjectMutationContext()

  const owner = claimProjectMutation(
    intent,
    (receipt, retried) => {
      if (retried && options.dialog) {
        closeProjectDialog(options.dialog)
      }

      void runCanonicalCreateFollowUps(context, input, receipt.project_id)
    },
    context
  )

  if (!owner) {
    throw new HandledProjectMutationError()
  }

  let outcome: ProjectMutationOutcome

  try {
    outcome = await executeProjectMutation(intent)
  } catch (error) {
    if (isCanonicalMethodMissing(error) && isProjectMutationContextCurrent(context)) {
      try {
        return await createLegacyProject(input, context)
      } finally {
        releaseProjectMutation(owner)
      }
    }

    releaseProjectMutation(owner)
    notifyError(error, translateNow('sidebar.projects.mutationFailed'))
    throw new HandledProjectMutationError()
  }

  const receipt = settleProjectMutation(outcome, owner, false)

  return $projects.get().find(project => project.id === receipt.project_id) ?? null
}

export async function renameProject(
  id: string,
  name: string,
  options: { dialog?: ProjectDialogState } = {}
): Promise<void> {
  const authority = resolveProjectManagementAuthority(id)

  if (authority.status === 'managed') {
    const context = captureProjectMutationContext()

    await executeProjectMutationWithFeedback(
      {
        expected_version: authority.snapshot.version,
        name: 'project.rename',
        payload: { name },
        project_id: id
      },
      (result, retried) => {
        runCanonicalProjectReconcile(context, result.project_id)

        if (retried && options.dialog) {
          closeProjectDialog(options.dialog)
        }
      }
    )

    return
  }

  if (authority.status !== 'conclusively-legacy') {
    throw new Error('project authority is unavailable')
  }

  await updateProject(id, { name })
}

// Patch top-level project fields (name / appearance). Optimistic: the cached
// tree + list update instantly so a color/icon/name change has no round-trip
// lag; only a failed write reconciles from the server.
export async function updateProject(
  id: string,
  patch: { name?: string; color?: null | string; icon?: null | string }
): Promise<void> {
  requireConclusiveLegacyProject(id)

  const snap = snapshotProjects()

  $projectTree.set(
    snap.tree.map(node =>
      node.id === id
        ? {
            ...node,
            ...(patch.name !== undefined && { label: patch.name }),
            ...(patch.color !== undefined && { color: patch.color }),
            ...(patch.icon !== undefined && { icon: patch.icon })
          }
        : node
    )
  )
  $projects.set(snap.projects.map(proj => (proj.id === id ? { ...proj, ...patch } : proj)))

  // Backend treats null/undefined as "leave unchanged"; "" clears (stores NULL).
  // Map explicit null → "" so "no color"/"no icon" actually clear.
  await persistOrRollback(snap, () =>
    gatewayRequest('projects.update', {
      id,
      ...patch,
      ...(patch.color === null && { color: '' }),
      ...(patch.icon === null && { icon: '' })
    })
  )
}

// Appearance for an AUTO (inherited git-repo) project has no projects.db row to
// write to — its id is just the repo path. So the first color/icon change ADOPTS
// the repo as a real project (folder = repo root, name = its label) carrying the
// chosen look; from then on it patches in place like any explicit project.
// Returns true when an adoption happened, so an incremental picker can close
// (the node's id changes on adopt, and a second stale write would double-create).
export async function setProjectAppearance(
  project: Pick<SidebarProjectTree, 'color' | 'icon' | 'id' | 'isAuto' | 'label' | 'path'>,
  patch: { color?: null | string; icon?: null | string }
): Promise<boolean> {
  requireConclusiveLegacyProject(project.id)

  if (!project.isAuto) {
    await updateProject(project.id, patch)

    return false
  }

  if (!project.path) {
    return false
  }

  await createProject({
    name: project.label,
    folders: [project.path],
    primaryPath: project.path,
    // Carry any already-set look so setting one field doesn't wipe the other.
    color: (patch.color ?? project.color) || undefined,
    icon: (patch.icon ?? project.icon) || undefined
  })

  return true
}

export async function addProjectFolder(
  id: string,
  path: string,
  opts: { label?: string; isPrimary?: boolean } = {}
): Promise<void> {
  requireConclusiveLegacyProject(id)

  const snap = snapshotProjects()
  const trimmed = path.trim()

  // Optimistic: append the folder to the cached project + reflect a primary-path
  // change on its tree node, so the dialog closes onto an updated row. The folder
  // -> repo seeding (and session regrouping) is backend-computed, so the
  // background refresh fills repos in; a failure rolls the cache back.
  if (trimmed) {
    const folder = { path: trimmed, label: opts.label ?? null, is_primary: opts.isPrimary ?? false, added_at: 0 }

    $projects.set(
      snap.projects.map(proj => {
        if (proj.id !== id || proj.folders?.some(f => f.path === trimmed)) {
          return proj
        }

        const folders = opts.isPrimary
          ? [folder, ...proj.folders.map(f => ({ ...f, is_primary: false }))]
          : [...proj.folders, folder]

        return { ...proj, folders, ...(opts.isPrimary && { primary_path: trimmed }) }
      })
    )

    if (opts.isPrimary) {
      $projectTree.set(snap.tree.map(node => (node.id === id ? { ...node, path: trimmed } : node)))
    }
  }

  await persistOrRollback(snap, () =>
    gatewayRequest('projects.add_folder', { id, path, label: opts.label, is_primary: opts.isPrimary ?? false })
  )
  reconcileProjects()
}

// True when the session currently open in the main pane belongs to `projectId`.
// Used so deleting a project you have a session open from kicks you back to the
// intro draft instead of stranding you in a now-orphaned view.
function openSessionBelongsToProject(projectId: string, projects: ProjectInfo[]): boolean {
  const openId = $selectedStoredSessionId.get()

  if (!openId) {
    return false
  }

  const open = $sessions.get().find(s => sessionMatchesStoredId(s, openId))

  return Boolean(open && liveSessionProjectId(open, projects) === projectId)
}

// Optimistic: drop the project from the cached tree + list the instant it's
// clicked (the entered-scope effect exits if you deleted the project you were
// inside), reconciling from the server payload. A failed delete restores both.
export async function deleteProject(id: string): Promise<void> {
  requireConclusiveLegacyProject(id)

  const snap = snapshotProjects()
  const catalogProfile = activeProfileId()
  const catalogGeneration = $projectCatalogAuthority.get().contextGeneration
  // Capture membership BEFORE removal — the project's folders (which determine
  // ownership) are gone once it's dropped from the cache.
  const kickToIntro = openSessionBelongsToProject(id, snap.projects)

  $projects.set(snap.projects.filter(project => project.id !== id))
  $projectTree.set(snap.tree.filter(node => node.id !== id))

  if (snap.active === id) {
    $activeProjectId.set(null)
  }

  // The open session's project is gone — reset to the intro draft (the session
  // itself survives; it just falls back to Recents).
  if (kickToIntro) {
    requestFreshSession()
  }

  await persistOrRollback(snap, async () => {
    applyPayload(await gatewayRequest<ProjectsPayload>('projects.delete', { id }), catalogProfile, catalogGeneration)
  })
  void refreshProjectTree()
}

export async function setActiveProject(id: null | string): Promise<void> {
  const res = await gatewayRequest<{ active_id: null | string }>('projects.set_active', { id })
  $activeProjectId.set(res.active_id ?? null)
}

// ── Project management dialog ────────────────────────────────────────────────
// A single dialog mounted in the sidebar reads this atom, so a project node's
// menu can open create / rename / add-folder flows without prop threading
// (mirrors $profileCreateRequest).
export const $projectDialog = atom<null | ProjectDialogState>(null)

export function openProjectCreate(): void {
  if ($projectsRpcAvailable.get() === false) {
    notify({
      kind: 'warning',
      message: translateNow('sidebar.projects.staleBackend')
    })

    return
  }

  $projectDialog.set({ mode: 'create' })
}

export function openProjectRename(project: { id: string; name: string }): void {
  $projectDialog.set({ mode: 'rename', name: project.name, projectId: project.id })
}

export function openProjectAddFolder(project: { id: string; name: string }): void {
  if (isEffectivelyManagedProject(project.id)) {
    return
  }

  $projectDialog.set({ mode: 'add-folder', name: project.name, projectId: project.id })
}

export function closeProjectDialog(expected?: ProjectDialogState): void {
  if (expected && $projectDialog.get() !== expected) {
    return
  }

  $projectDialog.set(null)
}

// ── Git-driven worktrees ("Start work") ─────────────────────────────────────
// Bumped after a `git worktree add`/`remove` so the sidebar's worktree-list
// probe (useRepoWorktreeMap) refetches and the new/removed lane shows at once,
// instead of waiting for the next scope change.
export const $worktreeRefreshToken = atom(0)
const bumpWorktrees = () => $worktreeRefreshToken.set($worktreeRefreshToken.get() + 1)

// Re-run the visual `git worktree list` probe without the heavy projects.tree
// scan. Desktop-initiated add/remove already bumps the token inline; this is for
// OUT-OF-BAND changes the renderer can't see: the agent runs `git worktree
// add/remove` in the terminal during a turn, or an external terminal mutates the
// repo while the window was away. The probe is per-repo and bounded, so the
// caller (a settled turn / window refocus) can re-sync the worktree lanes
// cheaply, the same way a git GUI refreshes its tree on focus.
export function refreshWorktrees(): void {
  bumpWorktrees()
}

// Spin up a fresh worktree the lightest way (`git worktree add -b`) under the
// repo, returning where Hermes should start working. Git is the source of
// truth; the caller starts a session in the returned path.
export async function startWorkInRepo(
  repoPath: string,
  options?: { name?: string; branch?: string; base?: string; existingBranch?: string }
): Promise<null | { path: string; branch: string }> {
  const git = desktopGit()

  if (!git || !repoPath) {
    return null
  }

  const result = await git.worktreeAdd(repoPath, options)
  bumpWorktrees()

  return { branch: result.branch, path: result.path }
}

// Local branches for the composer's "convert a branch into a worktree" picker.
// Empty on a remote backend / non-repo (the Electron probe can't run).
export async function listRepoBranches(repoPath: string): Promise<HermesGitBranch[]> {
  const git = desktopGit()

  if (!git?.branchList || !repoPath) {
    return []
  }

  return git.branchList(repoPath)
}

// Local + remote-tracking branches for the base-branch picker in the
// new-worktree dialog. The remote default (origin/HEAD) is flagged so the
// UI can preselect it. Empty on a remote backend / non-repo.
export async function listBaseBranches(repoPath: string): Promise<HermesGitBaseBranch[]> {
  const git = desktopGit()

  if (!git?.baseBranchList || !repoPath) {
    return []
  }

  return git.baseBranchList(repoPath)
}

export async function switchBranchInRepo(repoPath: string, branch: string): Promise<void> {
  const git = desktopGit()

  if (!git || !repoPath || !branch.trim()) {
    return
  }

  await git.branchSwitch(repoPath, branch)
  bumpWorktrees()
}

// A composer-driven "branch off into a new worktree" hand-off. The composer
// owns the typed draft; the chat controller owns session lifecycle. The composer
// creates the worktree (startWorkInRepo), then fires this so the controller opens
// a fresh session in that worktree and prefills the draft that kicked off the
// task. A monotonic token lets a rapid second request re-fire the controller's
// effect even if the path repeats.
export interface StartWorkSessionRequest {
  draft?: string
  path: string
  token: number
}

export const $startWorkSessionRequest = atom<StartWorkSessionRequest | null>(null)

// Keyboard-driven "spin up a new worktree" intent. The composer's coding rail
// owns the name dialog (it has the active repo + branch context), so a global
// hotkey just bumps this token; the rail opens its branch-off dialog in
// response. A monotonic token re-fires even on repeat presses. No-ops off a
// repo (the rail isn't mounted), which is the right "nothing to branch" outcome.
export const $newWorktreeRequest = atom(0)

export function requestNewWorktree(): void {
  $newWorktreeRequest.set($newWorktreeRequest.get() + 1)
}

let startWorkToken = 0

export function requestStartWorkSession(path: string, draft?: string): void {
  const target = path.trim()

  if (!target) {
    return
  }

  startWorkToken += 1
  $startWorkSessionRequest.set({ draft: draft?.trim() || undefined, path: target, token: startWorkToken })
}

export async function removeWorktreePath(
  repoPath: string,
  worktreePath: string,
  options?: { force?: boolean }
): Promise<void> {
  const git = desktopGit()

  if (!git) {
    return
  }

  await git.worktreeRemove(repoPath, worktreePath, options)
  bumpWorktrees()
}

// Reveal a project/worktree path in the OS file manager (git-GUI standard).
export async function revealPath(path: null | string): Promise<void> {
  if (path) {
    await window.hermesDesktop?.revealPath?.(path)
  }
}

// Copy a path to the clipboard (git-GUI standard).
export async function copyPath(path: null | string): Promise<void> {
  if (path) {
    await window.hermesDesktop?.writeClipboard?.(path)
  }
}

// Pick a project folder via the remote-aware picker: a remote gateway browses
// the backend filesystem (seeded at its default cwd) where sessions run; local
// mode opens the native dialog. Returns the absolute path, or null if cancelled.
export async function pickProjectFolder(): Promise<null | string> {
  const [dir] = await selectDesktopPaths({
    defaultPath: (await desktopDefaultCwd())?.cwd,
    directories: true,
    multiple: false
  })

  return dir || null
}
