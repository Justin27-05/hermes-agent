import { useStore } from '@nanostores/react'
import type * as React from 'react'
import { useState } from 'react'

import {
  type ActionItemSpec,
  ActionsContextMenu,
  DROPDOWN_KIT,
  type MenuKit,
  renderActionItem
} from '@/components/ui/actions-menu'
import { Codicon } from '@/components/ui/codicon'
import { ConfirmDialog } from '@/components/ui/confirm-dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from '@/components/ui/dropdown-menu'
import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover'
import { useI18n } from '@/i18n'
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

import { ProjectAppearancePicker } from './project-appearance'
import type { SidebarProjectTree } from './workspace-groups'

// Shared per-project state + handlers, so the kebab dropdown and the row's
// right-click menu drive the exact same actions. Modeled on git GUIs (GitHub
// Desktop / GitKraken): reveal in the file manager, copy path, and "Remove from
// sidebar" (never deletes files — auto projects are dismissed, explicit ones
// drop their entry). Explicit projects additionally get rename / add folder /
// set active.
function useProjectActions({
  project,
  isActive,
  scoped,
  onExitScope
}: {
  project: SidebarProjectTree
  isActive: boolean
  scoped: boolean
  onExitScope?: () => void
}) {
  const { t } = useI18n()
  const p = t.sidebar.projects
  const target = { id: project.id, name: project.label }
  const [confirmDeleteOpen, setConfirmDeleteOpen] = useState(false)

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

  // Rename / add folder / set active — explicit projects only (auto ones lack a
  // materialized record). Appearance is handled per-surface (popover vs submenu)
  // by the caller since its picker chrome differs.
  const identityItems: ActionItemSpec[] = project.isAuto
    ? []
    : [
        { icon: 'edit', key: 'rename', label: p.menuRename, onSelect: () => openProjectRename(target) },
        {
          icon: 'new-folder',
          key: 'add-folder',
          label: p.menuAddFolder,
          onSelect: () => openProjectAddFolder(target)
        },
        {
          disabled: isActive,
          icon: 'target',
          key: 'set-active',
          label: p.menuSetActive,
          onSelect: () => void setActiveProject(project.id)
        }
      ]

  const pathItems: ActionItemSpec[] = [
    {
      disabled: !project.path,
      icon: 'folder-opened',
      key: 'reveal',
      label: p.reveal,
      onSelect: () => void revealPath(project.path)
    },
    {
      disabled: !project.path,
      icon: 'copy',
      key: 'copy',
      label: p.copyPath,
      onSelect: () => void copyPath(project.path)
    }
  ]

  const dangerItem: ActionItemSpec = project.isAuto
    ? { icon: 'trash', key: 'remove', label: p.removeFromSidebar, onSelect: removeAuto, variant: 'destructive' }
    : {
        icon: 'trash',
        key: 'delete',
        label: `${p.menuDelete}…`,
        onSelect: () => setConfirmDeleteOpen(true),
        variant: 'destructive'
      }

  const confirmDialog = (
    <ConfirmDialog
      confirmLabel={p.menuDelete}
      description={p.deleteConfirm}
      destructive
      onClose={() => setConfirmDeleteOpen(false)}
      onConfirm={confirmDelete}
      open={confirmDeleteOpen}
      title={`${p.menuDelete} "${project.label}"?`}
    />
  )

  return { confirmDialog, dangerItem, identityItems, pathItems }
}

// Per-project actions. The kebab keeps its row-anchored Appearance popover; the
// right-click menu (ProjectContextMenu) renders the same actions with Appearance
// as a submenu. Hidden until the row is hovered, matching the + affordance.
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

  const { confirmDialog, dangerItem, identityItems, pathItems } = useProjectActions({
    isActive,
    onExitScope,
    project,
    scoped
  })

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

  // When anchorRef is absent, PopoverAnchor wraps the trigger so the
  // appearance popover positions against this button. Keep asChild chains free
  // of non-forwarding wrappers (#67500).
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

  const trigger = anchorRef ? triggerButton : <PopoverAnchor asChild>{triggerButton}</PopoverAnchor>

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
          {pathItems.map(item => renderActionItem(DROPDOWN_KIT, item))}
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
        <ProjectAppearancePicker
          color={project.color ?? null}
          icon={project.icon ?? null}
          noColorLabel={p.noColor}
          onColor={color => void applyAppearance({ color })}
          onIcon={icon => void applyAppearance({ icon })}
        />
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

interface ProjectContextMenuProps {
  project: SidebarProjectTree
  isActive: boolean
  scoped?: boolean
  onExitScope?: () => void
  children: React.ReactNode
}

// Wrap a project row so right-clicking it opens the same actions as its kebab.
// The kebab's row-anchored Appearance popover can't nest in a context menu, so
// here Appearance is a submenu with the same swatch + icon picker.
export function ProjectContextMenu({
  project,
  isActive,
  scoped = false,
  onExitScope,
  children
}: ProjectContextMenuProps) {
  const { t } = useI18n()
  const p = t.sidebar.projects

  const { confirmDialog, dangerItem, identityItems, pathItems } = useProjectActions({
    isActive,
    onExitScope,
    project,
    scoped
  })

  const canTheme = !project.isAuto || Boolean(project.path)

  const applyAppearance = (patch: { color?: null | string; icon?: null | string }) => {
    void setProjectAppearance(project, patch)
  }

  const items = (kit: MenuKit) => (
    <>
      {identityItems.map(item => renderActionItem(kit, item))}
      {canTheme && (
        <kit.Sub>
          <kit.SubTrigger>
            <Codicon name="symbol-color" size="0.875rem" />
            <span>{p.menuAppearance}</span>
          </kit.SubTrigger>
          <kit.SubContent className="w-auto p-2">
            <ProjectAppearancePicker
              color={project.color ?? null}
              icon={project.icon ?? null}
              noColorLabel={p.noColor}
              onColor={color => applyAppearance({ color })}
              onIcon={icon => applyAppearance({ icon })}
            />
          </kit.SubContent>
        </kit.Sub>
      )}
      {(identityItems.length > 0 || canTheme) && <kit.Separator />}
      {pathItems.map(item => renderActionItem(kit, item))}
      <kit.Separator />
      {renderActionItem(kit, dangerItem)}
    </>
  )

  return (
    <>
      <ActionsContextMenu ariaLabel={p.menu} contentClassName="w-48" items={items}>
        {children}
      </ActionsContextMenu>
      {confirmDialog}
    </>
  )
}
