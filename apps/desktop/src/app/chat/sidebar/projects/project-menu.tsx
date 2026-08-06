import { useStore } from '@nanostores/react'
import type * as React from 'react'
import { useState } from 'react'

import { Codicon } from '@/components/ui/codicon'
import { ColorSwatches } from '@/components/ui/color-swatches'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu'
import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover'
import { Tip } from '@/components/ui/tooltip'
import { useI18n } from '@/i18n'
import { PROFILE_SWATCHES } from '@/lib/profile-color'
import { cn } from '@/lib/utils'
import { $panesFlipped, dismissAutoProject } from '@/store/layout'
import type { ProjectMutationName } from '@/store/project-command-runtime'
import { $projectRuntimes } from '@/store/project-runtime'
import {
  $pendingProjectMutations,
  $projects,
  copyPath,
  deleteProject,
  executeProjectMutationWithFeedback,
  isEffectivelyManagedProject,
  openProjectAddFolder,
  openProjectRename,
  projectMutationPendingKey,
  revealPath,
  setActiveProject,
  setProjectAppearance
} from '@/store/projects'

import type { SidebarProjectTree } from './workspace-groups'

// Curated codicons for the project glyph (tinted by the chosen color).
const ICONS = [
  'folder-library',
  'repo',
  'rocket',
  'beaker',
  'flame',
  'star-full',
  'heart',
  'zap',
  'target',
  'lightbulb',
  'tools',
  'device-desktop',
  'device-mobile',
  'terminal',
  'dashboard',
  'globe',
  'broadcast',
  'cloud',
  'database',
  'package',
  'book',
  'organization',
  'bug',
  'shield',
  'key',
  'gift',
  'telescope',
  'home'
]

// Per-project actions, modeled on git GUIs (GitHub Desktop / GitKraken): reveal
// in the file manager, copy path, and "Remove from sidebar" (never deletes files
// — auto projects are dismissed, explicit ones drop their entry). Explicit
// projects additionally get rename / add folder / set active. Hidden until the
// row is hovered (group/workspace), matching the + affordance.
export function ProjectMenu({
  project,
  isActive,
  scoped = false,
  onExitScope,
  anchorRef
}: {
  project: SidebarProjectTree
  isActive: boolean
  // True when rendered in the entered-project header, so removal can leave the
  // now-defunct scope.
  scoped?: boolean
  onExitScope?: () => void
  // Anchor the appearance popover to the whole row instead of the kebab, so it
  // opens flush against the sidebar's content-facing edge — otherwise a
  // right-side sidebar drags the picker across the entire panel (the kebab
  // lives at the row's outer edge). Falls back to the kebab when absent.
  anchorRef?: React.RefObject<HTMLElement | null>
}) {
  const { t } = useI18n()
  const p = t.sidebar.projects
  const target = { id: project.id, name: project.label }
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false)
  const [appearanceOpen, setAppearanceOpen] = useState(false)
  // Open toward the content area: right when the sidebar is on the left, left
  // when the panes are flipped (sidebar on the right).
  const panesFlipped = useStore($panesFlipped)
  const projects = useStore($projects)
  const projectRuntimes = useStore($projectRuntimes)
  const pendingProjectMutations = useStore($pendingProjectMutations)
  const managed = isEffectivelyManagedProject(project.id, projects, projectRuntimes)
  const runtime = managed ? projectRuntimes[project.id]?.snapshot : undefined

  const pendingAction = (
    [
      'project.rename',
      'project.accept_completion',
      'project.reopen',
      'run.stop',
      'run.resume'
    ] satisfies ProjectMutationName[]
  ).find(name => pendingProjectMutations[projectMutationPendingKey(name, project.id)])

  const isActionPending = (name: ProjectMutationName): boolean =>
    Boolean(pendingProjectMutations[projectMutationPendingKey(name, project.id)])

  const runtimeApproval = runtime?.pending_approval
    ? `${p.runtimeApprovalPending} (${runtime.pending_approval.kind})`
    : p.runtimeApprovalNone

  const runtimeDelivery = runtime
    ? `${runtime.delivery_status.state}${
        runtime.delivery_status.error_code ? ` (${runtime.delivery_status.error_code})` : ''
      }`
    : ''

  const runtimeBlock = runtime?.block ? `${runtime.block.kind} (${runtime.block.code})` : p.runtimeBlockNone

  const removeAuto = () => {
    dismissAutoProject(project.id)

    if (scoped) {
      onExitScope?.()
    }
  }

  const confirmDelete = async () => {
    await deleteProject(project.id)

    if (scoped) {
      onExitScope?.()
    }
  }

  const runManagedCommand = (name: ProjectMutationName, payload: Record<string, unknown> = {}) => {
    if (!runtime || isActionPending(name)) {
      return
    }

    void executeProjectMutationWithFeedback({
      expected_version: runtime.version,
      name,
      payload,
      project_id: project.id
    }).catch(() => undefined)
  }

  // Appearance writes route through the adopt-aware helper: an auto project is
  // materialized on its first change (its id then changes), so close the picker
  // on adopt to stop a second write double-creating from a now-stale node.
  const applyAppearance = async (patch: { color?: null | string; icon?: null | string }) => {
    if (await setProjectAppearance(project, patch)) {
      setAppearanceOpen(false)
    }
  }

  // Set color / pick an icon — shown for explicit projects and for auto ones
  // (where selecting adopts the repo as a real project so the look sticks).
  const appearanceItem = (
    <DropdownMenuItem onSelect={() => setAppearanceOpen(true)}>
      <Codicon name="symbol-color" size="0.875rem" />
      <span>{p.menuAppearance}</span>
    </DropdownMenuItem>
  )

  // The bare trigger button (no Tip, no anchor) — composed with whichever of
  // Tip / PopoverAnchor apply below, always OUTSIDE the asChild chain that
  // ends at this button, never wrapping it directly. asChild clones only its
  // immediate child, so any of these wrappers placed inside another
  // asChild-consuming component (instead of around it) would have its
  // injected props silently swallowed by that inner component instead of
  // reaching the real DOM button (see #67500).
  const triggerButton = (
    <DropdownMenuTrigger asChild>
      <button
        aria-label={p.menu}
        className={cn(
          'grid size-4 shrink-0 place-items-center rounded-sm bg-transparent text-(--ui-text-quaternary) opacity-0 transition-opacity hover:bg-(--ui-control-hover-background) hover:text-foreground data-[state=open]:opacity-100',
          // In the project header reveal on the whole header hover; in overview
          // rows reveal on the row hover.
          scoped ? 'group-hover/section:opacity-100' : 'group-hover/workspace:opacity-100'
        )}
        onClick={event => event.stopPropagation()}
        type="button"
      >
        <Codicon name="kebab-vertical" size="0.75rem" />
      </button>
    </DropdownMenuTrigger>
  )

  // Tip always wraps the outermost element of whatever we render — either the
  // trigger directly (anchorRef present: the popover anchors to the whole row
  // via a separate virtualRef, so PopoverAnchor isn't involved here), or the
  // PopoverAnchor-wrapped trigger (anchorRef absent: the popover anchors to
  // this button itself). Either way, Tip > PopoverAnchor > DropdownMenuTrigger
  // > button, so asChild composes props/ref all the way down to the real DOM
  // node instead of stopping at an intermediate wrapper.
  const trigger = (
    <Tip label={p.menu}>{anchorRef ? triggerButton : <PopoverAnchor asChild>{triggerButton}</PopoverAnchor>}</Tip>
  )

  return (
    <Popover onOpenChange={setAppearanceOpen} open={appearanceOpen}>
      {/* Position the appearance popover against the row (when a ref is wired);
          the kebab is only the dropdown trigger then. */}
      {anchorRef ? <PopoverAnchor virtualRef={anchorRef as React.RefObject<HTMLElement>} /> : null}
      <DropdownMenu>
        {trigger}
        {/* Closing the menu refocuses the trigger (also the popover anchor),
            which the appearance popover would read as focus-outside and die on.
            Suppress that refocus so it survives. */}
        <DropdownMenuContent
          align="end"
          className={cn('w-48', runtime && 'w-64')}
          onCloseAutoFocus={event => event.preventDefault()}
          sideOffset={6}
        >
          {runtime ? (
            <>
              <div
                aria-label={p.runtimeStatus}
                className="grid gap-0.5 px-2 py-1 text-xs text-(--ui-text-secondary)"
                role="status"
              >
                <span>{`${p.runtimePhase}: ${runtime.current_phase}`}</span>
                <span>{`${p.runtimeLifecycle}: ${runtime.lifecycle}`}</span>
                <span>{`${p.runtimeQueue}: ${runtime.queue.length}`}</span>
                <span>{`${p.runtimeApproval}: ${runtimeApproval}`}</span>
                <span className="break-all">{`${p.runtimeDelivery}: ${runtimeDelivery}`}</span>
                <span className="break-all">{`${p.runtimeBlock}: ${runtimeBlock}`}</span>
                {pendingAction ? <span>{`${p.runtimeRetryPending}: ${pendingAction}`}</span> : null}
              </div>
              <DropdownMenuSeparator />
            </>
          ) : null}
          {project.isAuto ? (
            // Inherited (auto) repos can still be themed — the change adopts the
            // repo as a real project. Rename / add-folder / set-active stay out
            // until then (they need the materialized record).
            project.path && (
              <>
                {appearanceItem}
                <DropdownMenuSeparator />
              </>
            )
          ) : (
            <>
              <DropdownMenuItem
                disabled={(managed && !runtime) || isActionPending('project.rename')}
                onSelect={() => openProjectRename(target)}
              >
                <Codicon name="edit" size="0.875rem" />
                <span>{p.menuRename}</span>
              </DropdownMenuItem>
              {!managed ? (
                <>
                  {appearanceItem}
                  <DropdownMenuItem onSelect={() => openProjectAddFolder(target)}>
                    <Codicon name="new-folder" size="0.875rem" />
                    <span>{p.menuAddFolder}</span>
                  </DropdownMenuItem>
                </>
              ) : null}
              <DropdownMenuItem disabled={isActive} onSelect={() => void setActiveProject(project.id)}>
                <Codicon name="target" size="0.875rem" />
                <span>{p.menuSetActive}</span>
              </DropdownMenuItem>
              {managed && runtime?.lifecycle === 'awaiting_acceptance' ? (
                <>
                  <DropdownMenuItem
                    disabled={isActionPending('project.accept_completion')}
                    onSelect={() => runManagedCommand('project.accept_completion')}
                  >
                    <Codicon name="pass-filled" size="0.875rem" />
                    <span>{p.menuAcceptCompletion}</span>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    disabled={isActionPending('project.reopen')}
                    onSelect={() => runManagedCommand('project.reopen')}
                  >
                    <Codicon name="debug-continue" size="0.875rem" />
                    <span>{p.menuContinueWork}</span>
                  </DropdownMenuItem>
                </>
              ) : null}
              {managed && runtime?.lifecycle === 'completed' ? (
                <DropdownMenuItem
                  disabled={isActionPending('project.reopen')}
                  onSelect={() => runManagedCommand('project.reopen')}
                >
                  <Codicon name="debug-restart" size="0.875rem" />
                  <span>{p.menuReopen}</span>
                </DropdownMenuItem>
              ) : null}
              {managed &&
              runtime?.lifecycle === 'active' &&
              (runtime.active_run?.control_state === 'running' ||
                runtime.active_run?.control_state === 'awaiting_approval') ? (
                <DropdownMenuItem
                  disabled={isActionPending('run.stop')}
                  onSelect={() =>
                    runManagedCommand('run.stop', {
                      expected_control_version: runtime.active_run!.control_version,
                      turn_id: runtime.active_run!.turn_id
                    })
                  }
                >
                  <Codicon name="debug-stop" size="0.875rem" />
                  <span>{p.menuStop}</span>
                </DropdownMenuItem>
              ) : null}
              {managed && runtime?.lifecycle === 'active' && runtime.active_run?.control_state === 'stopped' ? (
                <DropdownMenuItem
                  disabled={isActionPending('run.resume')}
                  onSelect={() =>
                    runManagedCommand('run.resume', {
                      expected_control_version: runtime.active_run!.control_version,
                      turn_id: runtime.active_run!.turn_id
                    })
                  }
                >
                  <Codicon name="debug-continue" size="0.875rem" />
                  <span>{p.menuResume}</span>
                </DropdownMenuItem>
              ) : null}
              <DropdownMenuSeparator />
            </>
          )}
          <DropdownMenuItem disabled={!project.path} onSelect={() => void revealPath(project.path)}>
            <Codicon name="folder-opened" size="0.875rem" />
            <span>{p.reveal}</span>
          </DropdownMenuItem>
          <DropdownMenuItem disabled={!project.path} onSelect={() => void copyPath(project.path)}>
            <Codicon name="copy" size="0.875rem" />
            <span>{p.copyPath}</span>
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          {project.isAuto ? (
            <DropdownMenuItem onSelect={removeAuto} variant="destructive">
              <Codicon name="trash" size="0.875rem" />
              <span>{p.removeFromSidebar}</span>
            </DropdownMenuItem>
          ) : managed ? null : (
            <DropdownMenuItem onSelect={() => setConfirmDeleteOpen(true)} variant="destructive">
              <Codicon name="trash" size="0.875rem" />
              <span>{`${p.menuDelete}…`}</span>
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
      <PopoverContent
        align="start"
        className="w-auto p-2"
        onClick={event => event.stopPropagation()}
        side={panesFlipped ? 'left' : 'right'}
        sideOffset={6}
      >
        <ColorSwatches
          clearIcon="circle-slash"
          clearLabel={p.noColor}
          onChange={color => void applyAppearance({ color })}
          swatches={PROFILE_SWATCHES}
          value={project.color ?? null}
        />
        {/* Same 6 columns + gap as the swatch grid so the popover keeps the
            profile picker's width (icons flex to fill, not fixed-width). */}
        <div className="mt-2 grid grid-cols-6 gap-1.5">
          {ICONS.map(name => (
            <Tip key={name} label={name}>
              <button
                aria-label={name}
                className={cn(
                  'grid aspect-square place-items-center rounded-md text-(--ui-text-tertiary) transition hover:bg-(--ui-control-hover-background)',
                  project.icon === name && 'bg-(--ui-control-active-background) text-foreground'
                )}
                onClick={() => void applyAppearance({ icon: project.icon === name ? null : name })}
                style={project.icon === name && project.color ? { color: project.color } : undefined}
                type="button"
              >
                <Codicon name={name} size="0.8125rem" />
              </button>
            </Tip>
          ))}
        </div>
      </PopoverContent>
      {!project.isAuto && !managed ? (
        <ConfirmDialog
          confirmLabel={p.menuDelete}
          description={p.deleteConfirm}
          destructive
          onClose={() => setConfirmDeleteOpen(false)}
          onConfirm={confirmDelete}
          open={confirmDeleteOpen}
          title={`${p.menuDelete} "${project.label}"?`}
        />
      ) : null}
    </Popover>
  )
}
