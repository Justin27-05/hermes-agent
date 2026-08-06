import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

import type { ProjectActiveRun, ProjectRuntimeSnapshot } from '@/types/hermes'

import { ProjectMenu } from './project-menu'
import type { SidebarProjectTree } from './workspace-groups'

const stores = vi.hoisted(() => {
  const testAtom = <T,>(initial: T) => {
    let value = initial
    const listeners = new Set<(next: T) => void>()

    return {
      get: () => value,
      listen: (listener: (next: T) => void) => {
        listeners.add(listener)

        return () => listeners.delete(listener)
      },
      set: (next: T) => {
        value = next

        for (const listener of listeners) {
          listener(value)
        }
      },
      subscribe: (listener: (next: T) => void) => {
        listeners.add(listener)
        listener(value)

        return () => listeners.delete(listener)
      }
    }
  }

  return {
    pending: testAtom<Record<string, { phase: 'executing' | 'retry_required' }>>({}),
    projects: testAtom<unknown[]>([]),
    runtimes: testAtom<Record<string, unknown>>({})
  }
})

const projectActions = vi.hoisted(() => ({
  copyPath: vi.fn(),
  deleteProject: vi.fn(),
  executeProjectMutation: vi.fn(),
  executeProjectMutationWithFeedback: vi.fn(),
  openProjectAddFolder: vi.fn(),
  openProjectRename: vi.fn(),
  revealPath: vi.fn(),
  setActiveProject: vi.fn(),
  setProjectAppearance: vi.fn().mockResolvedValue(false)
}))

afterEach(cleanup)

// jsdom doesn't implement ResizeObserver; Radix's PopoverContent/Arrow use it
// (via @radix-ui/react-use-size) to measure the arrow once the popover is
// actually mounted. The kebab-only test above never opens a Popover, so it
// doesn't need this — only the appearance-popover test below does.
beforeAll(() => {
  vi.stubGlobal(
    'ResizeObserver',
    class {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
  )
})

vi.mock('@/i18n', () => ({
  useI18n: () => ({
    t: {
      common: { cancel: 'Cancel', confirm: 'Confirm', done: 'Done', loading: 'Loading…' },
      sidebar: {
        projects: {
          copyPath: 'Copy path',
          deleteConfirm: 'This cannot be undone.',
          menu: 'Project actions',
          menuAddFolder: 'Add folder',
          menuAcceptCompletion: 'Accept completion',
          menuAppearance: 'Appearance',
          menuContinueWork: 'Continue work',
          menuDelete: 'Delete',
          menuReopen: 'Reopen',
          menuRename: 'Rename',
          menuResume: 'Resume',
          menuSetActive: 'Set active',
          menuStop: 'Stop',
          noColor: 'No color',
          removeFromSidebar: 'Remove from sidebar',
          reveal: 'Reveal in file manager',
          runtimeApproval: 'Approval',
          runtimeApprovalNone: 'none',
          runtimeApprovalPending: 'pending',
          runtimeBlock: 'Blocked',
          runtimeBlockNone: 'none',
          runtimeDelivery: 'Delivery',
          runtimeLifecycle: 'Lifecycle',
          runtimePhase: 'Phase',
          runtimeQueue: 'Queue',
          runtimeRetryPending: 'Retry pending',
          runtimeStatus: 'Project runtime status'
        }
      }
    }
  })
}))

vi.mock('@/store/layout', () => ({
  $panesFlipped: {
    get: () => false,
    listen: () => () => {},
    subscribe: (fn: (v: boolean) => void) => {
      fn(false)

      return () => {}
    }
  },
  dismissAutoProject: vi.fn()
}))

vi.mock('@/store/projects', () => ({
  $pendingProjectMutations: stores.pending,
  $projects: stores.projects,
  copyPath: projectActions.copyPath,
  deleteProject: projectActions.deleteProject,
  executeProjectMutationWithFeedback: projectActions.executeProjectMutationWithFeedback,
  isEffectivelyManagedProject: (
    projectId: string,
    projects: Array<{ id: string; managed?: boolean }>,
    runtimes: Record<string, { snapshot?: { project_id?: string } }>
  ) =>
    projects.find(item => item.id === projectId)?.managed === true ||
    runtimes[projectId]?.snapshot?.project_id === projectId,
  openProjectAddFolder: projectActions.openProjectAddFolder,
  openProjectRename: projectActions.openProjectRename,
  projectMutationPendingKey: (name: string, projectId: null | string) =>
    name === 'project.create' ? 'project/create' : `project/${projectId}/${name}`,
  revealPath: projectActions.revealPath,
  setActiveProject: projectActions.setActiveProject,
  setProjectAppearance: projectActions.setProjectAppearance
}))

vi.mock('@/store/project-runtime', () => ({
  $projectRuntimes: stores.runtimes
}))

vi.mock('@/store/project-command-runtime', () => ({
  executeProjectMutation: projectActions.executeProjectMutation
}))

const project = {
  color: null,
  icon: null,
  id: 'p1',
  isAuto: false,
  label: 'Test D',
  path: '/repo'
} as unknown as SidebarProjectTree

const tipTrigger = (el: HTMLElement) => el.closest('[data-slot="tooltip-trigger"]')

const openTriggerMenu = (trigger: HTMLElement) => {
  // Radix's dropdown trigger opens on pointerdown (a synthetic 'click' fireEvent
  // alone won't do it), so fire the full mouse sequence a real click produces —
  // same technique as session-actions-menu.test.tsx (#67500).
  fireEvent.pointerDown(trigger, { button: 0, pointerType: 'mouse' })
  fireEvent.pointerUp(trigger, { button: 0, pointerType: 'mouse' })
  fireEvent.click(trigger)
}

const managedProject = {
  archived: false,
  board_slug: null,
  color: null,
  created_at: 0,
  description: null,
  folders: [],
  icon: null,
  id: 'p1',
  managed: true,
  name: 'Test D',
  primary_path: '/repo',
  slug: 'test-d'
}

const runtimeState = (
  lifecycle: 'active' | 'awaiting_acceptance' | 'completed',
  activeRun: null | ProjectActiveRun = null
): { events: never[]; snapshot: ProjectRuntimeSnapshot } => ({
  events: [],
  snapshot: {
    active_run: activeRun,
    artifacts: [],
    binding_id: 'binding-1',
    block: null,
    canonical_session_id: 'session-1',
    current_phase: 'implementation',
    delivery_status: { error_code: null, state: 'caught_up' },
    last_sequence: 11,
    lifecycle,
    pending_approval:
      activeRun?.control_state === 'awaiting_approval' ? { approval_id: 'approval-1', kind: 'tool' } : null,
    project_id: 'p1',
    queue: [],
    transcript: [],
    transcript_revision: 0,
    version: 7
  }
})

describe('ProjectMenu', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    stores.projects.set([])
    stores.runtimes.set({})
    stores.pending.set({})
    projectActions.executeProjectMutation.mockResolvedValue({ status: 'conflict' })
    projectActions.executeProjectMutationWithFeedback.mockResolvedValue({
      last_event_sequence: 12,
      project_id: 'p1'
    })
  })

  it('wraps the kebab trigger in a Tip', () => {
    render(<ProjectMenu isActive={false} project={project} />)

    const button = screen.getByRole('button', { name: 'Project actions' })
    expect(tipTrigger(button)).toBeTruthy()
  })

  // #67500 (Gille, second pass): when anchorRef is absent, the trigger used to
  // be `<PopoverAnchor asChild>{trigger}</PopoverAnchor>` where `trigger` was
  // ALREADY wrapped in <Tip> — so PopoverAnchor's asChild cloned Tip itself
  // (Tip doesn't forward extra props to its children), and the popover's
  // real-DOM anchor ref never reached the button. Composing Tip OUTSIDE
  // PopoverAnchor (Tip > PopoverAnchor > DropdownMenuTrigger > button) fixes
  // that ref delivery.
  //
  // What this test can't verify: jsdom has no layout engine, so the actual
  // POSITIONING the anchor ref enables isn't observable here — same
  // limitation already noted above for the icon grid. What it does verify:
  // the 3-deep asChild chain doesn't regress into the same silent-drop
  // failure as the original bug (#67500, first pass) — the trigger stays a
  // real, clickable element that opens the menu and reaches the Appearance
  // popover end-to-end, for the anchorRef-absent path specifically (the
  // anchorRef-present path never touches PopoverAnchor and is covered by the
  // kebab test above).
  it('opens the appearance popover through the kebab trigger when anchorRef is absent', async () => {
    render(<ProjectMenu isActive={false} project={project} />)

    const trigger = screen.getByRole('button', { name: 'Project actions' })

    openTriggerMenu(trigger)

    const appearanceItem = await screen.findByRole('menuitem', { name: 'Appearance' })

    fireEvent.click(appearanceItem)

    // The color-swatch "No color" clear option only renders once the
    // appearance Popover is actually open — proving the click reached the
    // real button through the full Tip > PopoverAnchor > DropdownMenuTrigger
    // chain rather than getting silently dropped on an intermediate wrapper.
    expect(await screen.findByRole('button', { name: 'No color' })).toBeTruthy()
  }, 15000)

  it('never offers hard Delete for an exactly managed project while legacy projects retain it', async () => {
    stores.projects.set([{ ...managedProject, managed: undefined }])
    stores.runtimes.set({ p1: runtimeState('active') })
    const { unmount } = render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))

    expect(screen.queryByRole('menuitem', { name: /Delete/ })).toBeNull()
    expect(screen.queryByRole('menuitem', { name: 'Appearance' })).toBeNull()
    expect(screen.queryByRole('menuitem', { name: 'Add folder' })).toBeNull()

    unmount()
    stores.projects.set([{ ...managedProject, managed: false }])
    stores.runtimes.set({})
    render(<ProjectMenu isActive={false} project={project} />)
    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))

    expect(await screen.findByRole('menuitem', { name: /Delete/ })).toBeTruthy()
  })

  it('shows the exact canonical phase, lifecycle, queue, approval, delivery, and block status', async () => {
    const state = runtimeState('active', {
      control_state: 'awaiting_approval',
      control_version: 4,
      turn_id: 'turn-7'
    })

    Object.assign(state.snapshot, {
      block: { code: 'surface_sync_blocked', kind: 'operation' },
      current_phase: 'review',
      delivery_status: { error_code: 'discord_permission', state: 'blocked' },
      queue: [
        { sequence: 12, status: 'queued', turn_id: 'turn-8' },
        { sequence: 13, status: 'queued', turn_id: 'turn-9' }
      ]
    } satisfies Partial<ProjectRuntimeSnapshot>)
    stores.projects.set([managedProject])
    stores.runtimes.set({ p1: state })
    render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))

    const status = await screen.findByRole('status', { name: 'Project runtime status' })
    expect(status.textContent).toContain('Phase: review')
    expect(status.textContent).toContain('Lifecycle: active')
    expect(status.textContent).toContain('Queue: 2')
    expect(status.textContent).toContain('Approval: pending (tool)')
    expect(status.textContent).toContain('Delivery: blocked (discord_permission)')
    expect(status.textContent).toContain('Blocked: operation (surface_sync_blocked)')
  })

  it('offers canonical accept-completion and continue-work actions only from awaiting acceptance', async () => {
    stores.projects.set([managedProject])
    stores.runtimes.set({ p1: runtimeState('awaiting_acceptance') })
    const { unmount } = render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Accept completion' }))

    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'project.accept_completion',
      payload: {},
      project_id: 'p1'
    })

    unmount()
    render(<ProjectMenu isActive={false} project={project} />)
    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Continue work' }))

    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenLastCalledWith({
      expected_version: 7,
      name: 'project.reopen',
      payload: {},
      project_id: 'p1'
    })
  })

  it('offers canonical Reopen only for a completed managed project', async () => {
    stores.projects.set([managedProject])
    stores.runtimes.set({ p1: runtimeState('completed') })
    render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Reopen' }))

    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'project.reopen',
      payload: {},
      project_id: 'p1'
    })
    expect(screen.queryByRole('menuitem', { name: 'Accept completion' })).toBeNull()
  })

  it('derives Stop from the canonical active run and sends both CAS versions', async () => {
    const state = runtimeState('active', { control_state: 'running', control_version: 4, turn_id: 'turn-7' })

    stores.projects.set([managedProject])
    stores.runtimes.set({ p1: state })
    render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Stop' }))

    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'run.stop',
      payload: { expected_control_version: 4, turn_id: 'turn-7' },
      project_id: 'p1'
    })
    expect(stores.runtimes.get()).toEqual({ p1: state })
  })

  it('keeps a frozen menu action visibly disabled instead of minting a second intent', async () => {
    stores.projects.set([managedProject])
    stores.runtimes.set({
      p1: runtimeState('active', { control_state: 'running', control_version: 4, turn_id: 'turn-7' })
    })
    render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Stop' }))
    stores.pending.set({ 'project/p1/run.stop': { phase: 'retry_required' } })
    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))

    const status = await screen.findByRole('status', { name: 'Project runtime status' })
    const stop = await screen.findByRole('menuitem', { name: 'Stop' })
    expect(status.textContent).toContain('Retry pending: run.stop')
    expect(stop.getAttribute('aria-disabled')).toBe('true')
    fireEvent.click(stop)
    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenCalledTimes(1)
  })

  it('derives Resume only from a durably stopped canonical run', async () => {
    stores.projects.set([managedProject])
    stores.runtimes.set({
      p1: runtimeState('active', { control_state: 'stopped', control_version: 5, turn_id: 'turn-7' })
    })
    render(<ProjectMenu isActive={false} project={project} />)

    openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))
    expect(screen.queryByRole('menuitem', { name: 'Stop' })).toBeNull()
    fireEvent.click(await screen.findByRole('menuitem', { name: 'Resume' }))

    expect(projectActions.executeProjectMutationWithFeedback).toHaveBeenCalledWith({
      expected_version: 7,
      name: 'run.resume',
      payload: { expected_control_version: 5, turn_id: 'turn-7' },
      project_id: 'p1'
    })
  })

  it.each(['stop_requested', 'resume_requested'] as const)(
    'does not invent a second control action while the canonical run is %s',
    async controlState => {
      stores.projects.set([managedProject])
      stores.runtimes.set({
        p1: runtimeState('active', { control_state: controlState, control_version: 5, turn_id: 'turn-7' })
      })
      render(<ProjectMenu isActive={false} project={project} />)

      openTriggerMenu(screen.getByRole('button', { name: 'Project actions' }))

      expect(screen.queryByRole('menuitem', { name: 'Stop' })).toBeNull()
      expect(screen.queryByRole('menuitem', { name: 'Resume' })).toBeNull()
    }
  )
})
