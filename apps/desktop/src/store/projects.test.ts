import { JsonRpcGatewayError } from '@hermes/shared'
import { atom } from 'nanostores'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import type { SidebarProjectTree } from '@/app/chat/sidebar/projects/workspace-groups'
import { $sidebarAgentsGrouped } from '@/store/layout'
import { $activeGatewayProfile } from '@/store/profile'
import { $projectRuntimes, configureProjectRuntimeRequester } from '@/store/project-runtime'

import {
  $activeProjectId,
  $pendingProjectMutations,
  $projectCatalogAuthority,
  $projectDialog,
  $projects,
  $projectScope,
  $projectsRpcAvailable,
  $projectTree,
  $removedSessionIds,
  $sessionMutationsInFlight,
  $worktreeRefreshToken,
  addProjectFolder,
  ALL_PROJECTS,
  beginSessionMutation,
  createProject,
  deleteProject,
  endSessionMutation,
  enterProject,
  executeProjectMutationWithFeedback,
  exitProjectScope,
  openProjectAddFolder,
  openProjectCreate,
  pickProjectFolder,
  projectCatalogScope,
  projectMutationPendingKey,
  projectNameForCwd,
  refreshProjects,
  refreshProjectTree,
  refreshWorktrees,
  renameProject,
  scanAndRecordRepos,
  setProjectAppearance,
  tombstoneSessions,
  updateProject
} from './projects'

vi.mock('@/i18n', () => ({
  translateNow: (key: string) => key
}))

vi.mock('@/store/notifications', () => ({
  notify: vi.fn(),
  notifyError: vi.fn()
}))

vi.mock('@/lib/desktop-fs', () => ({
  desktopDefaultCwd: vi.fn(),
  isDesktopFsRemoteMode: vi.fn(),
  selectDesktopPaths: vi.fn(),
  writeDesktopFileText: vi.fn()
}))

vi.mock('@/store/gateway', () => ({
  $gateway: atom(null),
  activeGateway: vi.fn(),
  ensureActiveGatewayOpen: vi.fn()
}))

vi.mock('@/lib/desktop-git', () => ({ desktopGit: vi.fn() }))

vi.mock('@/hermes', () => ({
  getHermesConfig: vi.fn(),
  getProfiles: vi.fn(),
  setApiRequestProfile: vi.fn(),
  STARTUP_REQUEST_TIMEOUT_MS: 1000
}))

vi.mock('@/store/project-command-runtime', () => ({
  executeProjectMutation: vi.fn(),
  retryProjectMutation: vi.fn()
}))

const fs = await import('@/lib/desktop-fs')
const desktopDefaultCwd = vi.mocked(fs.desktopDefaultCwd)
const isDesktopFsRemoteMode = vi.mocked(fs.isDesktopFsRemoteMode)
const selectDesktopPaths = vi.mocked(fs.selectDesktopPaths)

const gw = await import('@/store/gateway')
const activeGateway = vi.mocked(gw.activeGateway)
const gatewayAtom = gw.$gateway

const git = await import('@/lib/desktop-git')
const desktopGit = vi.mocked(git.desktopGit)

const hermes = await import('@/hermes')
const getHermesConfig = vi.mocked(hermes.getHermesConfig)
const notifications = await import('@/store/notifications')
const notify = vi.mocked(notifications.notify)
const notifyError = vi.mocked(notifications.notifyError)
const projectCommandRuntime = await import('@/store/project-command-runtime')
const executeProjectMutation = vi.mocked(projectCommandRuntime.executeProjectMutation)
const retryProjectMutation = vi.mocked(projectCommandRuntime.retryProjectMutation)

describe('project scope', () => {
  beforeEach(() => {
    window.localStorage.clear()
    $projectScope.set(ALL_PROJECTS)
  })

  it('defaults to ALL_PROJECTS', () => {
    expect($projectScope.get()).toBe(ALL_PROJECTS)
  })

  it('enterProject scopes the sidebar to the project id', () => {
    // setActiveProject fires best-effort (no gateway in test → it rejects and is
    // swallowed); the synchronous scope change is what matters here.
    enterProject('p_123')
    expect($projectScope.get()).toBe('p_123')
  })

  it('exitProjectScope returns to the overview', () => {
    enterProject('p_123')
    exitProjectScope()
    expect($projectScope.get()).toBe(ALL_PROJECTS)
  })

  it('entering the synthetic No-project bucket still scopes (no active pin)', () => {
    enterProject('__no_project__')
    expect($projectScope.get()).toBe('__no_project__')
  })

  it('persists the scope to localStorage', () => {
    enterProject('p_abc')
    expect(window.localStorage.getItem('hermes.desktop.projectScope')).toBe('p_abc')
  })
})

describe('projectNameForCwd', () => {
  const treeNode = (
    over: Partial<SidebarProjectTree> & Pick<SidebarProjectTree, 'id' | 'label'>
  ): SidebarProjectTree => ({
    path: null,
    repos: [],
    sessionCount: 0,
    ...over
  })

  beforeEach(() => {
    $projectTree.set([])
  })

  it('names the explicit project owning the cwd (longest path match)', () => {
    $projectTree.set([
      treeNode({ id: 'p_web', label: 'Website', path: '/repos/website' }),
      treeNode({ id: 'p_api', label: 'API', path: '/repos/api' })
    ])

    expect(projectNameForCwd('/repos/website/src/app')).toBe('Website')
  })

  it('matches nested repo and worktree paths, not just the project root', () => {
    $projectTree.set([
      treeNode({
        id: 'p_mono',
        label: 'Monorepo',
        path: '/repos/mono',
        repos: [
          {
            id: 'r1',
            label: 'mono',
            path: '/repos/mono',
            sessionCount: 0,
            groups: [{ id: 'g1', label: 'feature', path: '/elsewhere/mono-feature', sessions: [] }]
          }
        ]
      })
    ])

    // A linked worktree lives OUTSIDE the project root but still belongs to it.
    expect(projectNameForCwd('/elsewhere/mono-feature/src')).toBe('Monorepo')
  })

  it('ignores auto-projects and the No-project bucket (no named identity)', () => {
    $projectTree.set([
      treeNode({ id: '/repos/loose', label: 'loose', path: '/repos/loose', isAuto: true }),
      treeNode({ id: '__no_project__', label: 'No project', path: null, isNoProject: true })
    ])

    expect(projectNameForCwd('/repos/loose/src')).toBeNull()
  })

  it('returns null for a cwd in no project and for a blank cwd', () => {
    $projectTree.set([treeNode({ id: 'p_web', label: 'Website', path: '/repos/website' })])

    expect(projectNameForCwd('/somewhere/else')).toBeNull()
    expect(projectNameForCwd('')).toBeNull()
  })
})

describe('worktree refresh', () => {
  it('refreshWorktrees bumps the probe token so useRepoWorktreeMap refetches', () => {
    const before = $worktreeRefreshToken.get()
    refreshWorktrees()
    expect($worktreeRefreshToken.get()).toBe(before + 1)
  })
})

describe('pickProjectFolder', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('uses the remote-aware directory picker locally', async () => {
    isDesktopFsRemoteMode.mockReturnValue(false)
    selectDesktopPaths.mockResolvedValue(['/local/repo'])

    await expect(pickProjectFolder()).resolves.toBe('/local/repo')
    expect(selectDesktopPaths).toHaveBeenCalledWith({ defaultPath: undefined, directories: true, multiple: false })
  })

  it('seeds the picker with the backend cwd on a remote gateway', async () => {
    isDesktopFsRemoteMode.mockReturnValue(true)
    desktopDefaultCwd.mockResolvedValue({ branch: 'main', cwd: '/backend/work' })
    selectDesktopPaths.mockResolvedValue(['/backend/work/repo'])

    await expect(pickProjectFolder()).resolves.toBe('/backend/work/repo')
    expect(selectDesktopPaths).toHaveBeenCalledWith({
      defaultPath: '/backend/work',
      directories: true,
      multiple: false
    })
  })

  it('returns null when the picker is cancelled (empty selection)', async () => {
    isDesktopFsRemoteMode.mockReturnValue(false)
    selectDesktopPaths.mockResolvedValue([])

    await expect(pickProjectFolder()).resolves.toBeNull()
  })
})

describe('createProject', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $sidebarAgentsGrouped.set(false)
    $activeProjectId.set(null)
    $projectsRpcAvailable.set(null)
    $projects.set([])
    $projectTree.set([])
    $activeGatewayProfile.set('default')
    executeProjectMutation.mockRejectedValue(new JsonRpcGatewayError({ code: -32601 }))
  })

  it('falls back to legacy create only when the canonical command method is missing', async () => {
    const created = { folders: [], id: 'p_new', managed: false, name: 'Demo', primary_path: '/srv/demo' }

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.create') {
        return { project: created }
      }

      // Reconcile (fire-and-forget) re-reads list + tree; echo the project back
      // so the optimistic state survives instead of being wiped to empty.
      return { active_id: 'p_new', projects: [created], scoped_session_ids: [] }
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    const result = await createProject({ folders: ['/srv/demo'], name: 'Demo', use: true })

    expect(result).toEqual(created)
    expect(executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 0,
      name: 'project.create',
      payload: { folders: ['/srv/demo'], name: 'Demo' },
      project_id: null
    })
    expect(request).toHaveBeenCalledWith('projects.create', expect.objectContaining({ name: 'Demo' }))
    expect($sidebarAgentsGrouped.get()).toBe(true)
    expect($activeProjectId.get()).toBe('p_new')
  })

  it('keeps one semantic owner across canonical method-missing and the entire deferred legacy create', async () => {
    let finishLegacy!: (value: unknown) => void

    const legacyResponse = new Promise(resolve => {
      finishLegacy = resolve
    })

    const request = vi.fn((method: string) =>
      method === 'projects.create'
        ? legacyResponse
        : Promise.resolve({ active_id: null, projects: [], scoped_session_ids: [] })
    )

    const replacementGateway = {
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ project: null })
    }

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)
    executeProjectMutation.mockRejectedValue(new JsonRpcGatewayError({ code: -32601 }))

    const first = createProject({ folders: ['/srv/legacy'], name: 'Legacy' })
    await vi.waitFor(() => {
      expect(request).toHaveBeenCalledWith('projects.create', expect.objectContaining({ name: 'Legacy' }))
    })
    expect($pendingProjectMutations.get()[projectMutationPendingKey('project.create', null)]).toEqual({
      phase: 'executing'
    })

    activeGateway.mockReturnValue(replacementGateway as never)
    await expect(createProject({ folders: ['/srv/duplicate'], name: 'Duplicate' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
    expect(request.mock.calls.filter(([method]) => method === 'projects.create')).toHaveLength(1)
    expect(replacementGateway.request).not.toHaveBeenCalled()

    finishLegacy({
      project: {
        folders: [],
        id: 'p_legacy',
        managed: false,
        name: 'Legacy',
        primary_path: '/srv/legacy'
      }
    })
    await expect(first).resolves.toMatchObject({ id: 'p_legacy' })
    expect($pendingProjectMutations.get()).toEqual({})
  })

  it('does not apply a delayed legacy create response after its profile and gateway are replaced', async () => {
    let finishLegacy!: (value: unknown) => void

    const legacyResponse = new Promise(resolve => {
      finishLegacy = resolve
    })

    const originalGateway = {
      connectionState: 'open',
      request: vi.fn((method: string) =>
        method === 'projects.create'
          ? legacyResponse
          : Promise.resolve({ active_id: null, projects: [], scoped_session_ids: [] })
      )
    }

    const replacementGateway = { connectionState: 'open', request: vi.fn() }

    const replacementProject = {
      archived: false,
      board_slug: null,
      color: null,
      created_at: 0,
      description: null,
      folders: [],
      id: 'p_replacement',
      icon: null,
      managed: true,
      name: 'Replacement',
      primary_path: '/srv/replacement',
      slug: 'replacement'
    }

    const replacementTree = {
      id: 'p_replacement',
      label: 'Replacement',
      path: '/srv/replacement',
      repos: [],
      sessionCount: 0
    }

    activeGateway.mockReturnValue(originalGateway as never)
    executeProjectMutation.mockRejectedValue(new JsonRpcGatewayError({ code: -32601 }))

    const task = createProject({ folders: ['/srv/original'], name: 'Original', use: true })
    await vi.waitFor(() => {
      expect(originalGateway.request).toHaveBeenCalledWith(
        'projects.create',
        expect.objectContaining({ name: 'Original' })
      )
    })

    $activeGatewayProfile.set('replacement')
    activeGateway.mockReturnValue(replacementGateway as never)
    $projects.set([replacementProject])
    $projectTree.set([replacementTree])
    $activeProjectId.set('p_replacement')
    finishLegacy({
      project: {
        folders: [],
        id: 'p_original',
        managed: false,
        name: 'Original',
        primary_path: '/srv/original'
      }
    })

    await expect(task).resolves.toMatchObject({ id: 'p_original' })
    expect($projects.get()).toEqual([replacementProject])
    expect($projectTree.get()).toEqual([replacementTree])
    expect($activeProjectId.get()).toBe('p_replacement')
    expect(replacementGateway.request).not.toHaveBeenCalled()
    expect($pendingProjectMutations.get()).toEqual({})
  })

  it('normalizes an older create response before it reaches cached project state', async () => {
    const legacyCreated = { folders: [], id: 'legacy-created', name: 'Legacy', primary_path: null }

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.create') {
        return { project: legacyCreated }
      }

      return { active_id: null, projects: [legacyCreated], scoped_session_ids: [] }
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    await expect(createProject({ folders: [], name: 'Legacy' })).resolves.toEqual(legacyCreated)
    expect($projects.get()).toEqual([legacyCreated])
  })

  it('marks the backend stale and surfaces a friendly error when projects.create is missing', async () => {
    activeGateway.mockReturnValue({
      connectionState: 'open',
      request: vi.fn().mockRejectedValue(new Error('unknown method: projects.create'))
    } as never)

    await expect(createProject({ folders: ['/srv/demo'], name: 'Demo' })).rejects.toThrow(
      'sidebar.projects.staleBackend'
    )
    expect($projectsRpcAvailable.get()).toBe(false)
  })

  it('creates a managed project through the canonical command runtime without calling legacy create', async () => {
    const created = {
      folders: [],
      id: 'p_managed',
      managed: true,
      name: 'Managed',
      primary_path: '/srv/managed'
    }

    executeProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_managed' },
      status: 'succeeded'
    } as never)

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return { active_id: null, projects: [created] }
      }

      if (method === 'projects.tree') {
        return { active_id: null, projects: [], scoped_session_ids: [] }
      }

      throw new Error(`unexpected method: ${method}`)
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    await expect(
      createProject({
        boardSlug: 'managed-board',
        color: '#123456',
        description: 'Canonical project',
        folders: ['/srv/managed'],
        icon: 'rocket',
        name: 'Managed',
        primaryPath: '/srv/managed',
        slug: 'managed'
      })
    ).resolves.toBeNull()

    await vi.waitFor(() => {
      expect($projects.get()).toEqual([created])
    })

    expect(executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 0,
      name: 'project.create',
      payload: {
        board_slug: 'managed-board',
        color: '#123456',
        description: 'Canonical project',
        folders: ['/srv/managed'],
        icon: 'rocket',
        name: 'Managed',
        primary_path: '/srv/managed',
        slug: 'managed'
      },
      project_id: null
    })
    expect(request).not.toHaveBeenCalledWith('projects.create', expect.anything())
  })

  it('does not fall back to legacy create after a canonical timeout or domain rejection', async () => {
    const request = vi.fn()

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    for (const error of [
      new Error('request timed out after 30s: project.command'),
      new Error('unknown method: project.command'),
      new JsonRpcGatewayError({ code: 5065, data: { code: 'PROJECT_COMMAND_REJECTED' } })
    ]) {
      executeProjectMutation.mockRejectedValueOnce(error)
      await expect(createProject({ name: 'Managed' })).rejects.toMatchObject({
        name: 'HandledProjectMutationError'
      })
    }

    expect(request).not.toHaveBeenCalled()
    expect(notifyError).toHaveBeenCalledTimes(3)
  })

  it('does not redirect a delayed method-missing fallback into a replacement profile', async () => {
    let rejectCommand!: (error: unknown) => void

    const command = new Promise((_resolve, reject) => {
      rejectCommand = reject
    })

    const originalGateway = { connectionState: 'open', request: vi.fn() }

    const replacementGateway = {
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ project: null })
    }

    activeGateway.mockReturnValue(originalGateway as never)
    executeProjectMutation.mockReturnValue(command as never)
    const task = createProject({ folders: ['/srv/original'], name: 'Original' })
    $activeGatewayProfile.set('replacement')
    activeGateway.mockReturnValue(replacementGateway as never)
    rejectCommand(new JsonRpcGatewayError({ code: -32601 }))

    await expect(task).rejects.toMatchObject({ name: 'HandledProjectMutationError' })
    expect(replacementGateway.request).not.toHaveBeenCalled()
  })

  it('makes retry-required create visible and retries the frozen intent without a fresh execute', async () => {
    const created = {
      folders: [],
      id: 'p_retry',
      managed: true,
      name: 'Retry project',
      primary_path: '/srv/retry'
    }

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return { active_id: null, projects: [created] }
      }

      if (method === 'projects.tree') {
        return { active_id: null, projects: [], scoped_session_ids: [] }
      }

      throw new Error(`unexpected method: ${method}`)
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)
    executeProjectMutation.mockResolvedValue({ intent_id: 'frozen-create', status: 'retry_required' })
    retryProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_retry' },
      status: 'succeeded'
    } as never)

    await expect(createProject({ folders: ['/srv/retry'], name: 'Retry project' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })

    const retryNotice = notify.mock.calls.map(([input]) => input).find(input => input.action?.label === 'common.retry')

    expect(retryNotice).toBeTruthy()
    expect($pendingProjectMutations.get()[projectMutationPendingKey('project.create', null)]).toEqual({
      phase: 'retry_required'
    })
    await expect(createProject({ folders: ['/srv/duplicate'], name: 'Duplicate' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
    retryNotice!.action!.onClick()

    await vi.waitFor(() => {
      expect(retryProjectMutation).toHaveBeenCalledWith('frozen-create')
      expect($pendingProjectMutations.get()).toEqual({})
    })
    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
  })

  it('preserves the semantic lock across a second retry-required result and clears it on conflict', async () => {
    activeGateway.mockReturnValue({ connectionState: 'open', request: vi.fn() } as never)
    executeProjectMutation.mockResolvedValue({ intent_id: 'frozen-create', status: 'retry_required' })
    retryProjectMutation
      .mockResolvedValueOnce({ intent_id: 'frozen-create', status: 'retry_required' })
      .mockResolvedValueOnce({ status: 'conflict' })

    await expect(createProject({ folders: ['/srv/retry'], name: 'Retry' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    const firstRetry = notify.mock.calls.at(-1)![0].action!
    firstRetry.onClick()
    firstRetry.onClick()

    await vi.waitFor(() => {
      expect(retryProjectMutation).toHaveBeenCalledTimes(1)
      expect($pendingProjectMutations.get()[projectMutationPendingKey('project.create', null)]).toEqual({
        phase: 'retry_required'
      })
    })
    await expect(createProject({ folders: ['/srv/duplicate'], name: 'Duplicate' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    expect(executeProjectMutation).toHaveBeenCalledTimes(1)

    notify.mock.calls.at(-1)![0].action!.onClick()
    await vi.waitFor(() => {
      expect(retryProjectMutation).toHaveBeenCalledTimes(2)
      expect($pendingProjectMutations.get()).toEqual({})
    })
  })

  it('clears a frozen create owner after retry throws or its profile is invalidated', async () => {
    activeGateway.mockReturnValue({ connectionState: 'open', request: vi.fn() } as never)
    executeProjectMutation.mockResolvedValue({ intent_id: 'frozen-create', status: 'retry_required' })
    retryProjectMutation.mockRejectedValue(new Error('project command requester changed'))

    await expect(createProject({ folders: ['/srv/retry'], name: 'Retry' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    notify.mock.calls.at(-1)![0].action!.onClick()

    await vi.waitFor(() => {
      expect($pendingProjectMutations.get()).toEqual({})
    })

    executeProjectMutation.mockResolvedValueOnce({ intent_id: 'profile-create', status: 'retry_required' })
    await expect(createProject({ folders: ['/srv/profile'], name: 'Profile' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    $activeGatewayProfile.set('replacement')

    expect($pendingProjectMutations.get()).toEqual({})
  })

  it('treats the canonical receipt as terminal success when refresh and activation fail', async () => {
    executeProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_created' },
      status: 'succeeded'
    } as never)
    const request = vi.fn().mockRejectedValue(new Error('follow-up failed'))
    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    await expect(createProject({ folders: ['/srv/created'], name: 'Created', use: true })).resolves.toBeNull()

    await vi.waitFor(() => {
      expect(request).toHaveBeenCalledWith('projects.set_active', { id: 'p_created' })
      expect(notifyError).toHaveBeenCalledWith(
        expect.objectContaining({ message: 'follow-up failed' }),
        'sidebar.projects.activationFailed'
      )
    })
    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
  })

  it('never applies canonical create follow-ups to a replacement profile or closes its newer dialog', async () => {
    let resolveActivation!: (value: unknown) => void
    let resolveList!: (value: unknown) => void
    let resolveTree!: (value: unknown) => void

    const activation = new Promise(resolve => {
      resolveActivation = resolve
    })

    const list = new Promise(resolve => {
      resolveList = resolve
    })

    const tree = new Promise(resolve => {
      resolveTree = resolve
    })

    const originalGateway = {
      connectionState: 'open',
      request: vi.fn((method: string) =>
        method === 'projects.list' ? list : method === 'projects.tree' ? tree : activation
      )
    }

    const replacementGateway = { connectionState: 'open', request: vi.fn() }
    const originalDialog = { mode: 'create' as const }
    const replacementDialog = { mode: 'create' as const }

    activeGateway.mockReturnValue(originalGateway as never)
    $projectDialog.set(originalDialog)
    executeProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_original' },
      status: 'succeeded'
    } as never)

    await expect(
      createProject({ folders: ['/srv/original'], name: 'Original', use: true }, { dialog: originalDialog })
    ).resolves.toBeNull()
    expect(originalGateway.request).toHaveBeenCalledWith('projects.set_active', { id: 'p_original' })
    $projectDialog.set(replacementDialog)
    $activeGatewayProfile.set('replacement')
    activeGateway.mockReturnValue(replacementGateway as never)
    resolveList({
      active_id: 'p_original',
      projects: [{ folders: [], id: 'p_original', managed: true, name: 'Original', primary_path: null }]
    })
    resolveTree({
      active_id: 'p_original',
      projects: [{ id: 'p_original', label: 'Original', path: null, repos: [], sessionCount: 0 }],
      scoped_session_ids: []
    })
    resolveActivation({ active_id: 'p_original' })

    await vi.waitFor(() => {
      expect(originalGateway.request).toHaveBeenCalledTimes(3)
    })
    expect($projects.get()).toEqual([])
    expect($projectTree.get()).toEqual([])
    expect($activeProjectId.get()).toBeNull()
    expect($projectDialog.get()).toBe(replacementDialog)
    expect(replacementGateway.request).not.toHaveBeenCalled()
  })

  it('lets delayed Retry close only the exact dialog that originated the frozen create', async () => {
    const originalDialog = { mode: 'create' as const }
    const replacementDialog = { mode: 'create' as const }
    activeGateway.mockReturnValue({
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ active_id: null, projects: [], scoped_session_ids: [] })
    } as never)
    executeProjectMutation.mockResolvedValue({ intent_id: 'delayed-create', status: 'retry_required' })
    retryProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_delayed' },
      status: 'succeeded'
    } as never)
    $projectDialog.set(originalDialog)

    await expect(
      createProject({ folders: ['/srv/delayed'], name: 'Delayed' }, { dialog: originalDialog })
    ).rejects.toMatchObject({ name: 'HandledProjectMutationError' })
    $projectDialog.set(replacementDialog)
    notify.mock.calls.at(-1)![0].action!.onClick()

    await vi.waitFor(() => {
      expect(retryProjectMutation).toHaveBeenCalledWith('delayed-create')
      expect($pendingProjectMutations.get()).toEqual({})
    })
    expect($projectDialog.get()).toBe(replacementDialog)
  })

  it('surfaces canonical create rejections once without falling back or losing the dialog', async () => {
    const rejection = new Error('gateway connection closed')

    executeProjectMutation.mockRejectedValue(rejection)

    await expect(createProject({ folders: ['/srv/rejected'], name: 'Rejected' })).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })

    expect(notifyError).toHaveBeenCalledWith(rejection, 'sidebar.projects.mutationFailed')
    expect(notify).not.toHaveBeenCalled()
  })
})

describe('renameProject', () => {
  const runtime = {
    events: [],
    snapshot: {
      active_run: null,
      artifacts: [],
      binding_id: 'binding-1',
      block: null,
      canonical_session_id: 'session-1',
      current_phase: 'implementation',
      delivery_status: { error_code: null, state: 'caught_up' },
      last_sequence: 3,
      lifecycle: 'active',
      pending_approval: null,
      project_id: 'p_managed',
      queue: [],
      transcript: [],
      transcript_revision: 0,
      version: 7
    }
  } as const

  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    configureProjectRuntimeRequester(vi.fn(), 'default')
    const { contextGeneration } = $projectCatalogAuthority.get()
    $projectCatalogAuthority.set({
      catalogGeneration: contextGeneration,
      contextGeneration,
      profile: 'default'
    })
    $projects.set([
      {
        archived: false,
        board_slug: null,
        color: null,
        created_at: 0,
        description: null,
        folders: [],
        icon: null,
        id: 'p_managed',
        managed: true,
        name: 'Old name',
        primary_path: null,
        slug: 'managed'
      }
    ])
    $projectTree.set([
      {
        id: 'p_managed',
        label: 'Old name',
        path: null,
        repos: [],
        sessionCount: 0
      }
    ])
    $projectRuntimes.set({ p_managed: runtime as never })
  })

  it('renames managed projects through the canonical runtime without an optimistic cache write', async () => {
    let finish!: (value: unknown) => void

    const pending = new Promise(resolve => {
      finish = resolve
    })

    executeProjectMutation.mockReturnValue(pending as never)

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return {
          active_id: null,
          projects: [{ ...$projects.get()[0], name: 'New name' }]
        }
      }

      if (method === 'projects.tree') {
        return {
          active_id: null,
          projects: [{ ...$projectTree.get()[0], label: 'New name' }],
          scoped_session_ids: []
        }
      }

      throw new Error(`unexpected method: ${method}`)
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    const task = renameProject('p_managed', 'New name')

    expect($projects.get()[0]?.name).toBe('Old name')
    expect($projectTree.get()[0]?.label).toBe('Old name')
    expect(executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'project.rename',
      payload: { name: 'New name' },
      project_id: 'p_managed'
    })

    finish({
      result: { last_event_sequence: 4, project_id: 'p_managed' },
      status: 'succeeded'
    })
    await task

    expect($projects.get()[0]?.name).toBe('Old name')
    await vi.waitFor(() => {
      expect($projects.get()[0]?.name).toBe('New name')
    })
    expect(request).not.toHaveBeenCalledWith('projects.update', expect.anything())
  })

  it('blocks a second managed rename while its frozen intent owns the semantic action', async () => {
    activeGateway.mockReturnValue({ connectionState: 'open', request: vi.fn() } as never)
    executeProjectMutation.mockResolvedValue({ intent_id: 'frozen-rename', status: 'retry_required' })
    retryProjectMutation.mockResolvedValue({ status: 'conflict' })

    await expect(renameProject('p_managed', 'First name')).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    await expect(renameProject('p_managed', 'Second name')).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })

    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
    expect($pendingProjectMutations.get()[projectMutationPendingKey('project.rename', 'p_managed')]).toEqual({
      phase: 'retry_required'
    })
    notify.mock.calls.at(-1)![0].action!.onClick()
    await vi.waitFor(() => {
      expect($pendingProjectMutations.get()).toEqual({})
    })
  })

  it('keeps legacy rename behavior for an explicitly unmanaged project', async () => {
    $projects.set([{ ...$projects.get()[0]!, managed: false }])
    $projectRuntimes.set({})
    const request = vi.fn().mockResolvedValue({})

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    await renameProject('p_managed', 'Legacy name')

    expect(executeProjectMutation).not.toHaveBeenCalled()
    expect(request).toHaveBeenCalledWith('projects.update', { id: 'p_managed', name: 'Legacy name' })
  })

  it('treats a same-project canonical runtime as managed when the catalog marker is absent', async () => {
    $projects.set([{ ...$projects.get()[0]!, managed: undefined }])
    executeProjectMutation.mockResolvedValue({
      result: { last_event_sequence: 4, project_id: 'p_managed' },
      status: 'succeeded'
    } as never)

    const request = vi.fn(async (method: string) => {
      if (method === 'projects.list') {
        return {
          active_id: null,
          projects: [{ ...$projects.get()[0], managed: true, name: 'Canonical name' }]
        }
      }

      return { active_id: null, projects: [], scoped_session_ids: [] }
    })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)

    await renameProject('p_managed', 'Canonical name')

    expect(executeProjectMutation).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'project.rename',
      payload: { name: 'Canonical name' },
      project_id: 'p_managed'
    })
    expect(request).not.toHaveBeenCalledWith('projects.update', expect.anything())
  })

  it('blocks every legacy project mutation for a project proven managed by its runtime', async () => {
    $projects.set([{ ...$projects.get()[0]!, managed: undefined }])
    const beforeProjects = $projects.get()
    const beforeTree = $projectTree.get()
    const request = vi.fn()

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)
    $projectDialog.set(null)

    openProjectAddFolder({ id: 'p_managed', name: 'Managed' })

    await expect(updateProject('p_managed', { name: 'Forbidden' })).rejects.toThrow('managed project')
    await expect(
      setProjectAppearance(
        {
          color: null,
          icon: null,
          id: 'p_managed',
          isAuto: false,
          label: 'Managed',
          path: null
        },
        { color: '#123456' }
      )
    ).rejects.toThrow('managed project')
    await expect(addProjectFolder('p_managed', '/srv/extra')).rejects.toThrow('managed project')
    await expect(deleteProject('p_managed')).rejects.toThrow('managed project')

    expect($projectDialog.get()).toBeNull()
    expect($projects.get()).toBe(beforeProjects)
    expect($projectTree.get()).toBe(beforeTree)
    expect(request).not.toHaveBeenCalled()
  })
})

describe('canonical menu mutation ownership', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    activeGateway.mockReturnValue({ connectionState: 'open', request: vi.fn() } as never)
  })

  it('blocks a second direct menu action while Retry owns the first frozen intent', async () => {
    const intent = {
      expected_version: 7,
      name: 'run.stop' as const,
      payload: { expected_control_version: 4, turn_id: 'turn-7' },
      project_id: 'p_managed'
    }

    executeProjectMutation.mockResolvedValue({ intent_id: 'frozen-stop', status: 'retry_required' })
    retryProjectMutation.mockResolvedValue({ status: 'conflict' })

    await expect(executeProjectMutationWithFeedback(intent)).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })
    await expect(executeProjectMutationWithFeedback(intent)).rejects.toMatchObject({
      name: 'HandledProjectMutationError'
    })

    expect(executeProjectMutation).toHaveBeenCalledTimes(1)
    notify.mock.calls.at(-1)![0].action!.onClick()
    await vi.waitFor(() => {
      expect($pendingProjectMutations.get()).toEqual({})
    })
  })
})

describe('projects RPC capability', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $projectsRpcAvailable.set(null)
  })

  it('marks the backend stale when projects.list is missing', async () => {
    activeGateway.mockReturnValue({
      connectionState: 'open',
      request: vi.fn().mockRejectedValue(new Error('unknown method: projects.list'))
    } as never)

    await refreshProjects()

    expect($projectsRpcAvailable.get()).toBe(false)
  })

  it('blocks opening the create dialog once the backend is known stale', () => {
    $projectsRpcAvailable.set(false)

    openProjectCreate()

    expect(notify).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'warning', message: 'sidebar.projects.staleBackend' })
    )
  })
})

describe('projects managed marker', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    $projects.set([])
    $projectRuntimes.set({})
  })

  it('preserves an absent older-backend managed marker as unknown', async () => {
    activeGateway.mockReturnValue({
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ active_id: null, projects: [{ id: 'legacy-project' }] })
    } as never)

    await refreshProjects()

    expect($projects.get()).toEqual([{ id: 'legacy-project' }])
  })

  it('blocks every direct legacy mutation when the current catalog marker is unknown', async () => {
    const request = vi.fn().mockResolvedValue({ active_id: null, projects: [{ id: 'unknown-project' }] })

    activeGateway.mockReturnValue({ connectionState: 'open', request } as never)
    await refreshProjects()
    request.mockClear()
    const beforeProjects = $projects.get()
    const beforeTree = $projectTree.get()

    await expect(updateProject('unknown-project', { name: 'Forbidden' })).rejects.toThrow(
      'project authority is unavailable'
    )
    await expect(
      setProjectAppearance(
        {
          color: null,
          icon: null,
          id: 'unknown-project',
          isAuto: false,
          label: 'Unknown',
          path: null
        },
        { color: '#123456' }
      )
    ).rejects.toThrow('project authority is unavailable')
    await expect(addProjectFolder('unknown-project', '/srv/extra')).rejects.toThrow(
      'project authority is unavailable'
    )
    await expect(deleteProject('unknown-project')).rejects.toThrow('project authority is unavailable')

    expect($projects.get()).toBe(beforeProjects)
    expect($projectTree.get()).toBe(beforeTree)
    expect(request).not.toHaveBeenCalled()
  })

  it('does not reuse profile A explicit-legacy authority after profile B takes over the same project id', async () => {
    const requestA = vi
      .fn()
      .mockResolvedValue({ active_id: null, projects: [{ id: 'shared-project', managed: false }] })
    const requestB = vi.fn().mockResolvedValue({})

    activeGateway.mockReturnValue({ connectionState: 'open', request: requestA } as never)
    await refreshProjects()
    expect(projectCatalogScope()).toBe('default')
    expect($projects.get()).toEqual([{ id: 'shared-project', managed: false }])

    $activeGatewayProfile.set('profile-b')
    activeGateway.mockReturnValue({ connectionState: 'open', request: requestB } as never)
    const beforeProjects = $projects.get()
    const beforeTree = $projectTree.get()
    $projectDialog.set(null)

    openProjectAddFolder({ id: 'shared-project', name: 'Shared' })
    await expect(updateProject('shared-project', { name: 'Forbidden' })).rejects.toThrow(
      'project authority is unavailable'
    )
    await expect(
      setProjectAppearance(
        {
          color: null,
          icon: null,
          id: 'shared-project',
          isAuto: false,
          label: 'Shared',
          path: null
        },
        { icon: 'rocket' }
      )
    ).rejects.toThrow('project authority is unavailable')
    await expect(addProjectFolder('shared-project', '/srv/extra')).rejects.toThrow(
      'project authority is unavailable'
    )
    await expect(deleteProject('shared-project')).rejects.toThrow('project authority is unavailable')

    expect($projectDialog.get()).toBeNull()
    expect($projects.get()).toBe(beforeProjects)
    expect($projectTree.get()).toBe(beforeTree)
    expect(requestB).not.toHaveBeenCalled()
  })

  it('tags a successful project catalog with the profile that supplied it', async () => {
    $activeGatewayProfile.set('work')
    activeGateway.mockReturnValue({
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ active_id: null, projects: [] })
    } as never)

    await refreshProjects()

    expect(projectCatalogScope()).toBe('work')
  })

  it('does not publish a delayed A catalog after the gateway context transitions A to B to A', async () => {
    let resolveStaleA: ((value: unknown) => void) | undefined

    const staleResponse = new Promise(resolve => {
      resolveStaleA = resolve
    })

    const gatewayA = { connectionState: 'open', request: vi.fn(() => staleResponse) }
    const gatewayB = { connectionState: 'open', request: vi.fn() }
    let currentGateway = gatewayA

    activeGateway.mockImplementation(() => currentGateway as never)
    $activeGatewayProfile.set('default')
    gatewayAtom.set(gatewayA as never)

    const pendingA = refreshProjects()

    await vi.waitFor(() => expect(gatewayA.request).toHaveBeenCalledWith('projects.list', {}))

    currentGateway = gatewayB
    $activeGatewayProfile.set('work')
    gatewayAtom.set(gatewayB as never)
    currentGateway = gatewayA
    $activeGatewayProfile.set('default')
    gatewayAtom.set(gatewayA as never)

    resolveStaleA?.({ active_id: null, projects: [{ id: 'stale-a' }] })
    await pendingA

    expect($projects.get()).toEqual([])
  })
})

describe('repository discovery policy', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $activeGatewayProfile.set('default')
    isDesktopFsRemoteMode.mockReturnValue(false)
  })

  function gatewayWith(request: ReturnType<typeof vi.fn>) {
    const gateway = { connectionState: 'open', request }
    activeGateway.mockReturnValue(gateway as never)
    gatewayAtom.set(gateway as never)

    return gateway
  }

  it('records disabled policy without invoking the filesystem scanner', async () => {
    const request = vi.fn(async (method: string) =>
      method === 'projects.tree'
        ? { active_id: null, projects: [], scoped_session_ids: [] }
        : { accepted: false, repos: [] }
    )

    gatewayWith(request)
    const scanRepos = vi.fn()
    desktopGit.mockReturnValue({ scanRepos } as never)
    getHermesConfig.mockResolvedValue({
      desktop: {
        repo_scan_enabled: false,
        repo_scan_exclude_paths: [],
        repo_scan_roots: []
      }
    })

    await scanAndRecordRepos()

    expect(scanRepos).not.toHaveBeenCalled()
    expect(request).toHaveBeenCalledWith('projects.record_repos', {
      discovery_policy: { enabled: false, exclude_paths: [], roots: [] },
      repos: []
    })
  })

  it('passes custom roots and exclusions to Electron and records on the origin gateway', async () => {
    const request = vi.fn(async (method: string) =>
      method === 'projects.tree'
        ? { active_id: null, projects: [], scoped_session_ids: [] }
        : { accepted: true, repos: [] }
    )

    gatewayWith(request)
    const scanRepos = vi.fn().mockResolvedValue([{ label: 'repo', root: '/work/repo' }])
    desktopGit.mockReturnValue({ scanRepos } as never)
    getHermesConfig.mockResolvedValue({
      desktop: {
        repo_scan_enabled: true,
        repo_scan_exclude_paths: ['/work/vendor'],
        repo_scan_roots: ['/work']
      }
    })

    await scanAndRecordRepos()

    expect(getHermesConfig).toHaveBeenCalledWith('default')
    expect(scanRepos).toHaveBeenCalledWith(['/work'], {
      enabled: true,
      excludePaths: ['/work/vendor']
    })
    expect(request).toHaveBeenCalledWith('projects.record_repos', {
      discovery_policy: {
        enabled: true,
        exclude_paths: ['/work/vendor'],
        roots: ['/work']
      },
      repos: [{ label: 'repo', root: '/work/repo' }]
    })
  })

  it('does not scan the local filesystem for remote connections', async () => {
    isDesktopFsRemoteMode.mockReturnValue(true)
    const scanRepos = vi.fn()
    desktopGit.mockReturnValue({ scanRepos } as never)

    await scanAndRecordRepos(true)

    expect(scanRepos).not.toHaveBeenCalled()
    expect(getHermesConfig).not.toHaveBeenCalled()
  })
})

describe('project tree profile isolation', () => {
  it('does not publish a late response from the previous profile', async () => {
    let resolveA: ((value: unknown) => void) | undefined

    const responseA = new Promise(resolve => {
      resolveA = resolve
    })

    const gatewayA = { connectionState: 'open', request: vi.fn(() => responseA) }

    const gatewayB = {
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({
        active_id: null,
        projects: [{ id: 'profile-b', label: 'Profile B', path: null, repos: [], sessionCount: 0 }],
        scoped_session_ids: []
      })
    }

    let current = gatewayA
    activeGateway.mockImplementation(() => current as never)
    gatewayAtom.set(gatewayA as never)

    const pendingA = refreshProjectTree()
    current = gatewayB
    $activeGatewayProfile.set('profile-b')
    gatewayAtom.set(gatewayB as never)
    await refreshProjectTree()
    resolveA?.({
      active_id: null,
      projects: [{ id: 'profile-a', label: 'Profile A', path: null, repos: [], sessionCount: 0 }],
      scoped_session_ids: []
    })
    await pendingA

    expect($projectTree.get().map(project => project.id)).toEqual(['profile-b'])
  })
})

describe('tombstone pruning', () => {
  const openGatewayReturning = (scopedIds: string[]) => {
    const gateway = {
      connectionState: 'open',
      request: vi.fn().mockResolvedValue({ active_id: null, projects: [], scoped_session_ids: scopedIds })
    }

    activeGateway.mockImplementation(() => gateway as never)
    gatewayAtom.set(gateway as never)

    return gateway
  }

  beforeEach(() => {
    $removedSessionIds.set(new Set())
    $sessionMutationsInFlight.set(new Set())
  })

  it('keeps an in-flight delete tombstone even when the backend snapshot omits it', async () => {
    // Optimistic delete: hide the row, mark the RPC as in flight.
    tombstoneSessions(['sess-1'])
    beginSessionMutation(['sess-1'])

    // A projects.tree refresh races the pending delete: the id is already gone
    // from scope, but the RPC hasn't landed — the tombstone must survive so the
    // row doesn't flash back.
    openGatewayReturning([])
    await refreshProjectTree()

    expect($removedSessionIds.get().has('sess-1')).toBe(true)
  })

  it('prunes the tombstone once the mutation settles and scope no longer lists it', async () => {
    tombstoneSessions(['sess-1'])
    beginSessionMutation(['sess-1'])
    openGatewayReturning([])
    await refreshProjectTree()

    // Delete RPC settled; the next refresh with the id absent from scope drops it.
    endSessionMutation(['sess-1'])
    await refreshProjectTree()

    expect($removedSessionIds.get().has('sess-1')).toBe(false)
  })
})
