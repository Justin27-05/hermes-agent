import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import type * as Nanostores from 'nanostores'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { ProjectDialog } from './project-dialog'

afterEach(cleanup)

vi.mock('@/i18n', () => ({
  useI18n: () => ({
    t: {
      common: { cancel: 'Cancel', save: 'Save' },
      sidebar: {
        projects: {
          addFolder: 'Add folder',
          create: 'Create',
          createDesc: 'Create a new project',
          createFailed: 'Failed to create project',
          createTitle: 'New project',
          foldersLabel: 'Folders',
          ideaGenerate: 'Generate',
          ideaGenerating: 'Generating…',
          ideaLabel: 'Idea',
          ideaPlaceholder: 'What are you building?',
          ideaShuffle: 'Shuffle ideas',
          mutationPending: 'Retry pending',
          namePlaceholder: 'Project name',
          noFolders: 'No folders yet',
          primaryBadge: 'Primary',
          removeFolder: 'Remove folder'
        }
      }
    }
  })
}))

// $projectDialog is a real nanostore atom in the app; recreate it here so
// useStore behaves identically without pulling in the rest of the projects
// store (backend calls, project list, etc.) which is irrelevant to the Tip fix.
// vi.mock factories are hoisted above the rest of the file, so the atom must
// be created inside vi.hoisted to exist by the time the factory runs.
const { $pendingProjectMutations, $projectDialog } = vi.hoisted(() => {
  const { atom } = require('nanostores') as typeof Nanostores

  return {
    $pendingProjectMutations: atom<Record<string, { phase: 'executing' | 'retry_required' }>>({}),
    $projectDialog: atom<{ mode: 'create' | 'rename' | 'add-folder'; name?: string; projectId?: string } | null>({
      mode: 'create'
    })
  }
})

const projectActions = vi.hoisted(() => ({
  addProjectFolder: vi.fn(),
  closeProjectDialog: vi.fn(),
  createProject: vi.fn(),
  generateProjectIdea: vi.fn(),
  pickProjectFolder: vi.fn(async () => '/Users/test/my-folder'),
  renameProject: vi.fn()
}))

vi.mock('@/store/projects', () => ({
  $pendingProjectMutations,
  $projectDialog,
  ...projectActions,
  isHandledProjectMutationError: (error: unknown) => Boolean(error && typeof error === 'object' && 'handled' in error),
  projectMutationPendingKey: (name: string, projectId: null | string) =>
    name === 'project.create' ? 'project/create' : `project/${projectId}/${name}`
}))

const notifications = vi.hoisted(() => ({ notifyError: vi.fn() }))
vi.mock('@/store/notifications', () => ({
  notifyError: notifications.notifyError
}))

vi.mock('@/lib/project-idea-templates', () => ({
  randomIdeaTemplates: () => [{ emoji: '🚀', idea: 'A rocket tracker', label: 'Rocket tracker' }]
}))

const tipTrigger = (el: HTMLElement) => el.closest('[data-slot="tooltip-trigger"]')

describe('ProjectDialog', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    $projectDialog.set({ mode: 'create' })
    $pendingProjectMutations.set({})
    projectActions.closeProjectDialog.mockImplementation(expected => {
      if (!expected || $projectDialog.get() === expected) {
        $projectDialog.set(null)
      }
    })
  })

  it('wraps the "shuffle idea" button in a Tip', () => {
    render(<ProjectDialog />)

    const button = screen.getByRole('button', { name: 'Shuffle ideas' })
    expect(tipTrigger(button)).toBeTruthy()
  })

  it('wraps the "remove folder" button in a Tip once a folder is added', async () => {
    render(<ProjectDialog />)

    fireEvent.click(screen.getByRole('button', { name: 'Add folder' }))

    const button = await screen.findByRole('button', { name: 'Remove folder' })
    expect(tipTrigger(button)).toBeTruthy()
  })

  it('keeps the dialog open without a duplicate toast when the store already surfaced canonical retry', async () => {
    projectActions.createProject.mockRejectedValue({ handled: true })
    render(<ProjectDialog />)

    fireEvent.change(screen.getByPlaceholderText('Project name'), { target: { value: 'Managed' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add folder' }))
    await screen.findByText('/Users/test/my-folder')
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))

    await waitFor(() => {
      expect(projectActions.createProject).toHaveBeenCalled()
    })
    expect(projectActions.closeProjectDialog).not.toHaveBeenCalled()
    expect(notifications.notifyError).not.toHaveBeenCalled()
  })

  it('keeps a frozen create visibly disabled and blocks keyboard resubmission', async () => {
    render(<ProjectDialog />)

    fireEvent.change(screen.getByPlaceholderText('Project name'), { target: { value: 'Managed' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add folder' }))
    await screen.findByText('/Users/test/my-folder')
    $pendingProjectMutations.set({ 'project/create': { phase: 'retry_required' } })

    expect((await screen.findByRole('status')).textContent).toContain('Retry pending')
    expect(screen.getByRole('button', { name: 'Create' }).hasAttribute('disabled')).toBe(true)
    fireEvent.keyDown(screen.getByPlaceholderText('Project name'), { key: 'Enter' })
    expect(projectActions.createProject).not.toHaveBeenCalled()
  })

  it('closes only the exact dialog submission after a delayed canonical success', async () => {
    let finish!: () => void

    const deferred = new Promise<void>(resolve => {
      finish = resolve
    })

    projectActions.createProject.mockReturnValue(deferred)
    const originalDialog = $projectDialog.get()!
    render(<ProjectDialog />)

    fireEvent.change(screen.getByPlaceholderText('Project name'), { target: { value: 'Managed' } })
    fireEvent.click(screen.getByRole('button', { name: 'Add folder' }))
    await screen.findByText('/Users/test/my-folder')
    fireEvent.click(screen.getByRole('button', { name: 'Create' }))
    const replacementDialog = { mode: 'create' as const }
    $projectDialog.set(replacementDialog)
    finish()

    await waitFor(() => {
      expect(projectActions.closeProjectDialog).toHaveBeenCalledWith(originalDialog)
    })
    expect($projectDialog.get()).toBe(replacementDialog)
  })
})
