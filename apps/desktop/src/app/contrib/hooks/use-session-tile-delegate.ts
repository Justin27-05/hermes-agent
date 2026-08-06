import { useEffect, useRef } from 'react'

import { getSessionMessages, PROMPT_SUBMIT_REQUEST_TIMEOUT_MS } from '@/hermes'
import { translateNow } from '@/i18n'
import { toChatMessages } from '@/lib/chat-messages'
import {
  captureExactLegacySessionAuthority,
  validateExactLegacySessionAuthority
} from '@/store/legacy-session-authority'
import { notifyError } from '@/store/notifications'
import { projectComposerMessages } from '@/store/project-composer-queue'
import { resolveCurrentManagedProjectSurface } from '@/store/project-surface-authority-store'
import { publishSessionState, setSessionTileDelegate } from '@/store/session-states'
import type { SessionInfo, SessionResumeResponse } from '@/types/hermes'

import type { usePromptActions } from '../../session/hooks/use-prompt-actions'
import type { useSessionStateCache } from '../../session/hooks/use-session-state-cache'
import type { GatewayRequester } from '../types'

type SessionStateCache = ReturnType<typeof useSessionStateCache>

interface SessionTileDelegateParams {
  archiveSession: (storedSessionId: string) => Promise<unknown>
  branchStoredSession: (storedSessionId: string, sessionProfile?: string | null) => Promise<unknown>
  executeSlashCommand: ReturnType<typeof usePromptActions>['executeSlashCommand']
  removeSession: (storedSessionId: string) => Promise<unknown>
  requestGateway: GatewayRequester
  runtimeIdByStoredSessionIdRef: SessionStateCache['runtimeIdByStoredSessionIdRef']
  sessionStateByRuntimeIdRef: SessionStateCache['sessionStateByRuntimeIdRef']
  updateSessionState: SessionStateCache['updateSessionState']
}

/**
 * Publishes the session-tile delegate: resume / submit / interrupt / slash for
 * tiled sessions WITHOUT touching the primary view ($activeSessionId /
 * $messages stay the main thread's). Resume reuses a live runtime binding when
 * one exists (incl. the main thread's own session); a cold tile binds +
 * hydrates the cache, which publishSessionState mirrors to the tile.
 */
export function useSessionTileDelegate({
  archiveSession,
  branchStoredSession,
  executeSlashCommand,
  removeSession,
  requestGateway,
  runtimeIdByStoredSessionIdRef,
  sessionStateByRuntimeIdRef,
  updateSessionState
}: SessionTileDelegateParams): void {
  const runtimeProfileRef = useRef(new Map<string, string>())

  useEffect(() => {
    const allowLegacyStoredSessionMutation = (
      storedSession: SessionInfo,
      runtimeSessionId: null | string = null
    ) => {
      const authority = captureExactLegacySessionAuthority({
        runtimeSessionId,
        storedSession,
        storedSessionId: storedSession.id
      })

      if (authority) {
        return authority
      }

      const resolution = resolveCurrentManagedProjectSurface(runtimeSessionId, storedSession.id, storedSession)
      const message =
        resolution.status === 'ambiguous'
          ? translateNow('statusStack.managedProject.ambiguousSession')
          : resolution.status === 'unavailable'
            ? translateNow('statusStack.managedProject.runtimeUnavailable')
            : translateNow('statusStack.managedProject.historyUnsupported')

      notifyError(new Error(message), message)

      return null
    }

    setSessionTileDelegate({
      archiveSession: async storedSession => {
        if (!allowLegacyStoredSessionMutation(storedSession)) {
          return
        }

        await archiveSession(storedSession.id)
      },
      branchSession: async storedSession => {
        if (!allowLegacyStoredSessionMutation(storedSession)) {
          return
        }

        await branchStoredSession(storedSession.id, storedSession.profile)
      },
      deleteSession: async storedSession => {
        if (!allowLegacyStoredSessionMutation(storedSession)) {
          return
        }

        await removeSession(storedSession.id)
      },
      executeSlash: async (rawCommand, runtimeId, storedSession) => {
        const authority = allowLegacyStoredSessionMutation(storedSession, runtimeId)

        if (!authority || !validateExactLegacySessionAuthority(authority, { runtimeSessionId: runtimeId })) {
          return
        }

        await executeSlashCommand(rawCommand, { sessionId: runtimeId })
      },
      interruptSession: async (runtimeId, storedSession) => {
        const authority = allowLegacyStoredSessionMutation(storedSession, runtimeId)

        if (!authority || !validateExactLegacySessionAuthority(authority, { runtimeSessionId: runtimeId })) {
          return
        }

        await requestGateway('session.interrupt', { session_id: runtimeId })
      },
      resumeTile: async storedSession => {
        const storedSessionId = storedSession.id
        const existing = runtimeIdByStoredSessionIdRef.current.get(storedSessionId)
        const managedResolution = resolveCurrentManagedProjectSurface(existing, storedSessionId, storedSession)

        if (managedResolution.status === 'ambiguous') {
          throw new Error(translateNow('statusStack.managedProject.ambiguousSession'))
        }

        if (managedResolution.status === 'unavailable') {
          throw new Error(translateNow('statusStack.managedProject.runtimeUnavailable'))
        }

        if (managedResolution.status === 'managed') {
          updateSessionState(
            storedSessionId,
            state => ({
              ...state,
              busy: managedResolution.snapshot.active_run?.control_state === 'running',
              messages: projectComposerMessages(managedResolution.snapshot)
            }),
            storedSessionId
          )

          return storedSessionId
        }

        const authority = allowLegacyStoredSessionMutation(storedSession, existing ?? null)

        if (!authority) {
          throw new Error(translateNow('statusStack.managedProject.runtimeUnavailable'))
        }

        const cached = existing ? sessionStateByRuntimeIdRef.current.get(existing) : undefined

        if (
          existing &&
          cached?.storedSessionId === storedSessionId &&
          runtimeProfileRef.current.get(existing) === authority.targetProfile &&
          validateExactLegacySessionAuthority(authority, { runtimeSessionId: existing })
        ) {
          publishSessionState(existing, cached)

          return existing
        }

        // Resolve the owning profile before binding a runtime. A tile can open a
        // session from any profile, not just the active one; resuming (or
        // reading messages) without a profile lets the gateway fall back to the
        // launch-profile DB and fork the conversation into the wrong profile —
        // the same cross-profile bleed the recovery resumes had (#67603).
        const profile = authority.targetProfile

        const [prefetch, resumed] = await Promise.all([
          getSessionMessages(storedSessionId, profile).catch(() => null),
          requestGateway<SessionResumeResponse>('session.resume', {
            session_id: storedSessionId,
            cols: 96,
            ...(profile ? { profile } : {})
          })
        ])

        const runtimeId = resumed?.session_id

        if (!runtimeId) {
          throw new Error('resume returned no session id')
        }

        if (!validateExactLegacySessionAuthority(authority, { runtimeSessionId: existing ?? null })) {
          throw new Error(translateNow('statusStack.managedProject.runtimeUnavailable'))
        }

        runtimeProfileRef.current.set(runtimeId, profile)
        updateSessionState(
          runtimeId,
          state => ({
            ...state,
            busy: Boolean(resumed?.info?.running),
            messages:
              state.messages.length > 0 ? state.messages : toChatMessages(prefetch?.messages ?? resumed?.messages ?? [])
          }),
          storedSessionId
        )

        return runtimeId
      },
      submitToSession: async (runtimeId, text, storedSession) => {
        const authority = allowLegacyStoredSessionMutation(storedSession, runtimeId)

        if (!authority || !validateExactLegacySessionAuthority(authority, { runtimeSessionId: runtimeId })) {
          return
        }

        await requestGateway('prompt.submit', { session_id: runtimeId, text }, PROMPT_SUBMIT_REQUEST_TIMEOUT_MS)
      },
      updateSession: (runtimeId, updater) => updateSessionState(runtimeId, updater)
    })
  }, [
    archiveSession,
    branchStoredSession,
    executeSlashCommand,
    removeSession,
    requestGateway,
    runtimeIdByStoredSessionIdRef,
    sessionStateByRuntimeIdRef,
    updateSessionState
  ])
}
