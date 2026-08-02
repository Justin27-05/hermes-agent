/**
 * Prompt actions for a SESSION TILE — the same verbs the primary chat wires
 * (submit incl. slash, cancel, steer, edit, reload, restore, branch-hide
 * sync), targeted at the tile's session instead of the active one. State
 * writes go through the delegate's `updateSession` (the wiring cache), so
 * the cache, the primary view, and every tile mirror stay one truth; view
 * concerns (busy pill, transcript) reach the tile via its `$sessionStates`
 * slice — never the global `$busy`/`$messages`.
 */

import type { AppendMessage, ThreadMessage } from '@assistant-ui/react'
import { useCallback, useMemo, useRef } from 'react'

import { useGatewayRequest } from '@/app/gateway/hooks/use-gateway-request'
import type { ClientSessionState } from '@/app/types'
import { PROMPT_SUBMIT_REQUEST_TIMEOUT_MS } from '@/hermes'
import { useI18n } from '@/i18n'
import { textPart } from '@/lib/chat-messages'
import { SLASH_COMMAND_RE } from '@/lib/chat-runtime'
import { sanitizeComposerInput } from '@/lib/composer-input-sanitize'
import { triggerHaptic } from '@/lib/haptics'
import { clearClarifyRequest } from '@/store/clarify'
import type { ComposerAttachment } from '@/store/composer'
import { resetSessionBackground } from '@/store/composer-status'
import { notifyError } from '@/store/notifications'
import { clearPreviewArtifacts } from '@/store/preview-status'
import {
  markManagedComposerNotice,
  resolveCapturedProjectSubmitAuthority,
  submitManagedProjectPrompt
} from '@/store/project-composer-queue'
import { resolveCurrentManagedProjectSurface } from '@/store/project-surface-authority-store'
import { executeProjectMutationWithFeedback } from '@/store/projects'
import { clearAllPrompts } from '@/store/prompts'
import { $connection, $sessions, sessionMatchesStoredId } from '@/store/session'
import { $sessionStates, sessionTileDelegate } from '@/store/session-states'
import { broadcastSessionsChanged } from '@/store/session-sync'
import { clearSessionSubagents } from '@/store/subagents'
import { clearSessionTodos } from '@/store/todos'
import type { SessionInfo } from '@/types/hermes'

import { uploadComposerAttachment } from '../session/hooks/use-prompt-actions'
import {
  applyBranchVisibility,
  applyReloadOptimistic,
  applyRewindOptimistic,
  finalizeInterruptedMessages,
  planEdit,
  planReload,
  planRestore,
  runRewindSubmit,
  truncateSubmitParams
} from '../session/hooks/use-prompt-actions/rewind'
import { useSubmitPrompt } from '../session/hooks/use-prompt-actions/submit'
import { type GatewayRequest, type SubmitTextOptions } from '../session/hooks/use-prompt-actions/utils'
import { upsertOptimisticSession } from '../session/hooks/use-session-actions/utils'

import type { ComposerScope } from './composer/scope'

/**
 * List a tile's session in the sidebar/tab strip on its first send.
 *
 * A ⌘T tab's session is created UNLISTED (see `openNewSessionTile`), so it has
 * no `$sessions` row until its first turn persists and a refresh surfaces it —
 * for that whole first exchange the tab and the sidebar read "New session".
 * ⌘N has no such gap: its session is created per-send and seeded with the
 * user's text as the row preview. Seeding the same way here names the session
 * within the first message; the server's auto-title supersedes it once the turn
 * completes.
 *
 * No-ops on empty text and on a session that is already listed, so re-sends
 * never clobber a real title with a raw message preview.
 */
export function listTileSessionRow(deps: {
  cwd?: string
  model?: string
  preview: string
  runtimeId: string
  sessions: readonly SessionInfo[]
  storedSessionId: string
}): boolean {
  const preview = deps.preview.trim()

  if (!preview || deps.sessions.some(session => sessionMatchesStoredId(session, deps.storedSessionId))) {
    return false
  }

  upsertOptimisticSession(
    { info: { cwd: deps.cwd, model: deps.model }, session_id: deps.runtimeId, stored_session_id: deps.storedSessionId },
    deps.storedSessionId,
    null,
    preview
  )
  broadcastSessionsChanged()

  return true
}

interface SessionTileActionsArgs {
  runtimeId: string
  scope: ComposerScope
  storedSession?: SessionInfo
  storedSessionId: string
}

export function useSessionTileActions({ runtimeId, scope, storedSession, storedSessionId }: SessionTileActionsArgs) {
  const { t } = useI18n()
  const copy = t.desktop
  const managedProjectCopy = t.statusStack.managedProject
  const { requestGateway } = useGatewayRequest()

  const runtimeIdRef = useRef(runtimeId)
  runtimeIdRef.current = runtimeId
  const storedIdRef = useRef(storedSessionId)
  storedIdRef.current = storedSessionId

  const resolveProjectSurface = useCallback(
    (runtimeSessionId: null | string | undefined) =>
      resolveCurrentManagedProjectSurface(runtimeSessionId, storedIdRef.current, storedSession),
    [storedSession]
  )

  // Tile busy tracks the SESSION state, never the global $busy — and it must
  // read LIVE. A render-time snapshot goes stale (this hook's host doesn't
  // re-render on busy edges), and a stale `true` silently blocks every
  // subsequent submit ("tile only sends one message"). The setter is a no-op:
  // session state owns busy; submit's optimistic writes flow through
  // updateSession.
  const busyRef = useMemo(
    () =>
      ({
        get current() {
          return $sessionStates.get()[runtimeIdRef.current]?.busy ?? false
        },
        set current(_value: boolean) {
          // Owned by session state.
        }
      }) as { current: boolean },
    []
  )

  const update = useCallback(
    (updater: (state: ClientSessionState) => ClientSessionState) =>
      sessionTileDelegate()?.updateSession(runtimeIdRef.current, updater),
    []
  )

  const readState = useCallback(() => $sessionStates.get()[runtimeIdRef.current], [])
  const readMessages = useCallback(() => readState()?.messages ?? [], [readState])

  // A ⌘T tab's session is unlisted until its first turn persists — seed the
  // row from the user's first message so the tab and sidebar name it right
  // away (see listTileSessionRow).
  const listTileSession = useCallback((preview: string) => {
    const runtimeId = runtimeIdRef.current
    const state = $sessionStates.get()[runtimeId]

    listTileSessionRow({
      cwd: state?.cwd,
      model: state?.model,
      preview,
      runtimeId,
      sessions: $sessions.get(),
      storedSessionId: storedIdRef.current
    })
  }, [])

  // Tile-side attachment staging: same upload rules as the primary submit
  // (skip synced/pathless, byte-upload files+images), against the tile scope.
  const syncAttachmentsForSubmit = useCallback(
    async (
      sessionId: string,
      attachments: ComposerAttachment[],
      options: { requestGateway?: GatewayRequest; updateComposerAttachments?: boolean } = {}
    ): Promise<ComposerAttachment[]> => {
      const attachmentRequestGateway = options.requestGateway ?? requestGateway
      const remote = $connection.get()?.mode === 'remote'
      const synced: ComposerAttachment[] = []

      for (const attachment of attachments) {
        if (!attachment.path || attachment.attachedSessionId === sessionId) {
          synced.push(attachment)

          continue
        }

        if (attachment.kind === 'image' || attachment.kind === 'file') {
          const next = await uploadComposerAttachment(attachment, {
            remote,
            requestGateway: attachmentRequestGateway,
            sessionId
          })

          if (options.updateComposerAttachments ?? true) {
            scope.attachments.update(next)
          }

          synced.push(next)

          continue
        }

        synced.push(attachment)
      }

      return synced
    },
    [requestGateway, scope.attachments]
  )

  // The REAL submit pipeline with tile seams: session always exists, and the
  // scope's writers replace the global view/attachment writes.
  const submitPromptText = useSubmitPrompt({
    activeSessionIdRef: runtimeIdRef,
    busyRef,
    copy,
    createBackendSessionForSend: async () => runtimeIdRef.current,
    getRoutedStoredSessionId: () => storedIdRef.current,
    getRuntimeIdForStoredSession: storedId => (storedId === storedIdRef.current ? runtimeIdRef.current : null),
    // A tile IS its session — no route to abandon, so the create-abort guard's
    // token is a stable constant (the guard never trips for a tile).
    getRouteToken: () => runtimeId,
    requestGateway,
    // Tile ids are always bound before this hook mounts, so routed recovery is
    // unreachable here; keep the shared submit contract explicit.
    resumeStoredSession: () => undefined,
    selectedStoredSessionIdRef: storedIdRef,
    syncAttachmentsForSubmit,
    updateSessionState: (sessionId, updater) => sessionTileDelegate()!.updateSession(sessionId, updater),
    scope: {
      clearAttachments: scope.attachments.clear,
      readAttachments: () => scope.attachments.$attachments.get(),
      // Busy/messages flow through updateSession -> the tile's state slice;
      // the primary view atoms must never see a tile turn.
      setAwaitingResponse: () => undefined,
      setBusy: () => undefined,
      setMessages: () => undefined
    }
  })

  const submitText = useCallback(
    async (rawText: string, options?: SubmitTextOptions) => {
      const visibleText = sanitizeComposerInput(rawText).trim()
      const attachments = options?.attachments ?? scope.attachments.$attachments.get()
      const targetSessionId = options?.sessionId ?? runtimeIdRef.current
      let managedRuntime

      if (options?.projectAuthority) {
        const capturedResolution = resolveCapturedProjectSubmitAuthority(options.projectAuthority)
        const surfaceResolution = resolveProjectSurface(targetSessionId)

        const capturedManagedMatchesSurface =
          capturedResolution.status === 'managed' &&
          surfaceResolution.status === 'managed' &&
          capturedResolution.snapshot.binding_id === surfaceResolution.snapshot.binding_id &&
          capturedResolution.snapshot.project_id === surfaceResolution.snapshot.project_id &&
          capturedResolution.snapshot.canonical_session_id === surfaceResolution.snapshot.canonical_session_id

        const capturedLegacyMatchesSurface =
          capturedResolution.status === 'legacy' && surfaceResolution.status === 'conclusively-legacy'

        if (!capturedManagedMatchesSurface && !capturedLegacyMatchesSurface) {
          notifyError(new Error(managedProjectCopy.voiceScopeChanged), managedProjectCopy.voiceScopeChanged)

          return false
        }

        managedRuntime = capturedManagedMatchesSurface ? capturedResolution.snapshot : undefined
      } else {
        const managedResolution = resolveProjectSurface(targetSessionId)

        if (managedResolution.status === 'ambiguous' || managedResolution.status === 'unavailable') {
          const message =
            managedResolution.status === 'ambiguous'
              ? managedProjectCopy.ambiguousSession
              : managedProjectCopy.runtimeUnavailable

          notifyError(new Error(message), message)

          return false
        }

        managedRuntime = managedResolution.status === 'managed' ? managedResolution.snapshot : undefined
      }

      listTileSession(visibleText)

      if (managedRuntime) {
        return await submitManagedProjectPrompt({
          attachmentsPresent: attachments.length > 0,
          copy: managedProjectCopy,
          fromQueue: Boolean(options?.fromQueue),
          onOptimistic: optimistic =>
            sessionTileDelegate()?.updateSession(targetSessionId, state => ({
              ...state,
              messages: state.messages.some(message => message.id === optimistic.local_id)
                ? state.messages
                : [
                    ...state.messages,
                    { id: optimistic.local_id, parts: [textPart(visibleText)], pending: true, role: 'user' }
                  ]
            })),
          snapshot: managedRuntime,
          text: visibleText
        })
      }

      if (!attachments.length && SLASH_COMMAND_RE.test(visibleText)) {
        if (!storedSession) {
          notifyError(new Error(managedProjectCopy.runtimeUnavailable), managedProjectCopy.runtimeUnavailable)

          return false
        }

        triggerHaptic('selection')
        await sessionTileDelegate()?.executeSlash(visibleText, targetSessionId, storedSession)

        return true
      }

      return await submitPromptText(rawText, options)
    },
    [
      listTileSession,
      managedProjectCopy,
      resolveProjectSurface,
      scope.attachments.$attachments,
      storedSession,
      submitPromptText
    ]
  )

  const blockManagedHistoryMutation = useCallback(
    (sessionId: string): boolean => {
      const resolution = resolveProjectSurface(sessionId)

      if (resolution.status === 'conclusively-legacy') {
        return false
      }

      if (resolution.status === 'managed') {
        markManagedComposerNotice(resolution.snapshot, {
          message: managedProjectCopy.historyUnsupported,
          status: 'blocked',
          text: ''
        })
      } else {
        notifyError(new Error(managedProjectCopy.historyUnsupported), managedProjectCopy.historyUnsupported)
      }

      return true
    },
    [managedProjectCopy.historyUnsupported, resolveProjectSurface]
  )

  const cancelRun = useCallback(async () => {
    const sessionId = runtimeIdRef.current
    const managedResolution = resolveProjectSurface(sessionId)

    if (managedResolution.status === 'ambiguous' || managedResolution.status === 'unavailable') {
      const message =
        managedResolution.status === 'ambiguous'
          ? managedProjectCopy.ambiguousSession
          : managedProjectCopy.runtimeUnavailable

      notifyError(new Error(message), message)

      return
    }

    if (managedResolution.status === 'managed') {
      const { active_run: activeRun } = managedResolution.snapshot

      if (!activeRun || activeRun.control_state !== 'running') {
        markManagedComposerNotice(managedResolution.snapshot, {
          message: managedProjectCopy.stopUnavailable,
          status: 'blocked',
          text: ''
        })

        return
      }

      await executeProjectMutationWithFeedback({
        expected_version: managedResolution.snapshot.version,
        name: 'run.stop',
        payload: {
          expected_control_version: activeRun.control_version,
          turn_id: activeRun.turn_id
        },
        project_id: managedResolution.snapshot.project_id
      }).catch(() => undefined)

      return
    }

    update(state => ({
      ...state,
      messages: finalizeInterruptedMessages(state.messages, state.streamId),
      busy: false,
      awaitingResponse: false,
      streamId: null,
      pendingBranchGroup: null,
      needsInput: false,
      interrupted: true
    }))

    clearSessionTodos(sessionId)
    clearSessionSubagents(sessionId)
    resetSessionBackground(sessionId)
    clearAllPrompts(sessionId)
    clearClarifyRequest(undefined, sessionId)

    try {
      await requestGateway('session.interrupt', { session_id: sessionId })
    } catch (err) {
      notifyError(err, copy.stopFailed)
    }
  }, [
    copy.stopFailed,
    managedProjectCopy.ambiguousSession,
    managedProjectCopy.runtimeUnavailable,
    managedProjectCopy.stopUnavailable,
    requestGateway,
    resolveProjectSurface,
    update
  ])

  const steerPrompt = useCallback(
    async (rawText: string): Promise<boolean> => {
      const text = rawText.trim()
      const sessionId = runtimeIdRef.current

      if (!text || !sessionId) {
        return false
      }

      if (resolveProjectSurface(sessionId).status !== 'conclusively-legacy') {
        return submitText(rawText, { sessionId })
      }

      const messageId = `user-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

      const mutate = (updater: (state: ClientSessionState) => ClientSessionState) =>
        sessionTileDelegate()?.updateSession(sessionId, updater)

      // Match the primary composer: insert the correction before the active
      // reply before awaiting the redirect RPC, whose completion can race us.
      mutate(state => {
        const message = {
          id: messageId,
          role: 'user' as const,
          parts: [textPart(text)]
        }

        const streamIndex = state.streamId ? state.messages.findIndex(candidate => candidate.id === state.streamId) : -1

        const lastAssistantIndex = state.messages.map(candidate => candidate.role).lastIndexOf('assistant')
        const insertionIndex = streamIndex >= 0 ? streamIndex : lastAssistantIndex

        const messages =
          insertionIndex >= 0
            ? [...state.messages.slice(0, insertionIndex), message, ...state.messages.slice(insertionIndex)]
            : [...state.messages, message]

        return { ...state, messages }
      })

      const discardOptimisticMessage = () =>
        mutate(state => ({
          ...state,
          messages: state.messages.filter(message => message.id !== messageId)
        }))

      const moveOptimisticMessageToEnd = () =>
        mutate(state => {
          const message = state.messages.find(candidate => candidate.id === messageId)

          return message
            ? { ...state, messages: [...state.messages.filter(candidate => candidate.id !== messageId), message] }
            : state
        })

      try {
        const result = await requestGateway<{ status?: string }>('session.redirect', {
          session_id: sessionId,
          text
        })

        if (result?.status === 'redirected') {
          triggerHaptic('submit')

          return true
        }

        if (result?.status === 'queued') {
          moveOptimisticMessageToEnd()
          triggerHaptic('submit')

          return true
        }
      } catch {
        discardOptimisticMessage()
        // Swallow — the caller queues the text so nothing is lost.

        return false
      }

      discardOptimisticMessage()

      return false
    },
    [requestGateway, resolveProjectSurface, submitText]
  )

  // Rewind primitive (interrupt-first for live turns, busy-retry) — shared with
  // the primary chat so the two can't diverge.
  const submitRewind = useCallback(
    (text: string, truncateOrdinal: number | undefined, interruptFirst: boolean) =>
      runRewindSubmit(requestGateway, runtimeIdRef.current, text, truncateOrdinal, interruptFirst),
    [requestGateway]
  )

  const reloadFromMessage = useCallback(
    async (parentId: string | null) => {
      if (blockManagedHistoryMutation(runtimeIdRef.current)) {
        return
      }

      const state = readState()

      if (!state || state.busy) {
        return
      }

      const plan = planReload(state.messages, parentId)

      if (!plan) {
        return
      }

      update(current => applyReloadOptimistic(current, plan))

      try {
        await requestGateway(
          'prompt.submit',
          {
            session_id: runtimeIdRef.current,
            text: plan.text,
            ...truncateSubmitParams(plan.truncateOrdinal)
          },
          PROMPT_SUBMIT_REQUEST_TIMEOUT_MS
        )
      } catch (err) {
        update(current => ({ ...current, busy: false, awaitingResponse: false }))
        notifyError(err, copy.regenerateFailed)
      }
    },
    [blockManagedHistoryMutation, copy.regenerateFailed, readState, requestGateway, update]
  )

  const restoreToMessage = useCallback(
    async (messageId: string, target?: { text?: string; userOrdinal?: number | null }) => {
      const sessionId = runtimeIdRef.current

      if (blockManagedHistoryMutation(sessionId)) {
        return
      }

      const messages = readMessages()
      const plan = planRestore(messages, messageId, target)

      clearSessionTodos(sessionId)
      resetSessionBackground(sessionId)
      clearPreviewArtifacts(sessionId)

      const wasBusy = readState()?.busy ?? false

      update(state => applyRewindOptimistic(state, plan.sourceIndex))

      try {
        await submitRewind(plan.text, plan.truncateOrdinal, wasBusy)
      } catch (err) {
        update(state => ({ ...state, busy: false, awaitingResponse: false, messages }))
        throw err
      }
    },
    [blockManagedHistoryMutation, readMessages, readState, submitRewind, update]
  )

  const editMessage = useCallback(
    async (edited: AppendMessage) => {
      if (blockManagedHistoryMutation(runtimeIdRef.current)) {
        return
      }

      const messages = readMessages()
      const plan = planEdit(messages, edited)

      if (!plan) {
        return
      }

      const sessionId = runtimeIdRef.current

      clearSessionTodos(sessionId)
      resetSessionBackground(sessionId)
      clearPreviewArtifacts(sessionId)

      const wasBusy = readState()?.busy ?? false

      update(state => applyRewindOptimistic(state, plan.sourceIndex, plan.editedMessage))

      try {
        await submitRewind(plan.text, plan.truncateOrdinal, wasBusy)
      } catch (err) {
        update(state => ({ ...state, busy: false, awaitingResponse: false, messages }))
        notifyError(err, copy.editFailed)
      }
    },
    [blockManagedHistoryMutation, copy.editFailed, readMessages, readState, submitRewind, update]
  )

  // Branch-visibility sync (assistant-ui hides non-active branches).
  const handleThreadMessagesChange = useCallback(
    (nextMessages: readonly ThreadMessage[]) => update(state => applyBranchVisibility(state, nextMessages)),
    [update]
  )

  const dismissError = useCallback(
    (messageId: string) => {
      update(state => ({ ...state, messages: state.messages.filter(m => m.id !== messageId) }))
    },
    [update]
  )

  return useMemo(
    () => ({
      cancelRun,
      dismissError,
      editMessage,
      handleThreadMessagesChange,
      reloadFromMessage,
      restoreToMessage,
      steerPrompt,
      submitText
    }),
    [
      cancelRun,
      dismissError,
      editMessage,
      handleThreadMessagesChange,
      reloadFromMessage,
      restoreToMessage,
      steerPrompt,
      submitText
    ]
  )
}
