import { useCallback, useEffect, useRef, useState } from 'react'

import { useI18n } from '@/i18n'
import { chatMessageText, collectUnspokenTurnSpeech } from '@/lib/chat-messages'
import { triggerHaptic } from '@/lib/haptics'
import { resetBrowseState } from '@/store/composer-input-history'
import {
  captureExactLegacySessionAuthority,
  captureFrozenLegacyDraftAuthority,
  validateExactLegacySessionAuthority,
  validateFrozenLegacyDraftAuthority
} from '@/store/legacy-session-authority'
import { notifyError } from '@/store/notifications'
import {
  captureProjectSubmitAuthority,
  quarantineProjectVoicePrompt,
  resolveCapturedProjectSubmitAuthority
} from '@/store/project-composer-queue'
import { resolveCurrentManagedProjectSurface } from '@/store/project-surface-authority-store'
import { $messages } from '@/store/session'
import { $autoSpeakReplies, setAutoSpeakReplies } from '@/store/voice-prefs'
import type { SessionInfo } from '@/types/hermes'

import type { SubmitTextOptions } from '../../../session/hooks/use-prompt-actions/utils'
import type { ComposerTarget } from '../focus'
import { onComposerVoiceToggleRequest } from '../focus'
import type { ChatBarProps } from '../types'

import { useAutoSpeakReplies } from './use-auto-speak-replies'
import { useVoiceConversation } from './use-voice-conversation'
import { useVoiceRecorder } from './use-voice-recorder'

interface UseComposerVoiceArgs {
  busy: boolean
  clearDraft: () => void
  disabled: boolean
  focusInput: () => void
  insertText: (text: string) => void
  maxRecordingSeconds: number
  onSubmit: ChatBarProps['onSubmit']
  onTranscribeAudio: ChatBarProps['onTranscribeAudio']
  sessionId: string | null | undefined
  storedSessionId?: string | null
  storedSession?: SessionInfo
  /** This composer's focus-bus key — voice toggles targeting another
   *  composer (or the active one, when not us) are ignored. */
  target: ComposerTarget
}

/**
 * The composer's voice engine: push-to-talk dictation (transcript → draft), the
 * full voice-conversation loop, and auto-speak of replies. Self-contained — it
 * consumes the draft/submit primitives passed in but nothing depends back on it,
 * so it lifts cleanly out of ChatBar.
 */
export function useComposerVoice({
  busy,
  clearDraft,
  disabled,
  focusInput,
  insertText,
  maxRecordingSeconds,
  onSubmit,
  onTranscribeAudio,
  sessionId,
  storedSession,
  storedSessionId,
  target
}: UseComposerVoiceArgs) {
  const { t } = useI18n()

  const [voiceConversationActive, setVoiceConversationActive] = useState(false)
  const lastSpokenIdRef = useRef<string | null>(null)

  const { dictate, voiceActivityState, voiceStatus } = useVoiceRecorder({
    focusInput,
    maxRecordingSeconds,
    onTranscript: insertText,
    onTranscribeAudio
  })

  /** Auto-speak selector: the latest unspoken reply only — a backlog collapses to the newest. */
  const pendingResponse = () => {
    const messages = $messages.get()
    const last = messages.findLast(m => m.role === 'assistant' && !m.hidden)

    if (!last || last.id === lastSpokenIdRef.current) {
      return null
    }

    const text = chatMessageText(last).trim()

    if (!text) {
      return null
    }

    return {
      id: last.id,
      pending: Boolean(last.pending),
      text
    }
  }

  /**
   * Voice-conversation selector: every unspoken assistant bubble of the turn,
   * in order — narration interims AND the final answer, not just whichever
   * bubble happens to be last. See `collectUnspokenTurnSpeech`.
   */
  const pendingTurnResponse = () => collectUnspokenTurnSpeech($messages.get(), lastSpokenIdRef.current)

  const consumePendingResponse = () => {
    const messages = $messages.get()
    const last = messages.findLast(m => m.role === 'assistant' && !m.hidden)

    if (last) {
      lastSpokenIdRef.current = last.id
    }
  }

  const captureSubmitOptions = useCallback((): SubmitTextOptions => {
    const canonicalSessionId = storedSessionId ?? sessionId ?? null
    const surface = resolveCurrentManagedProjectSurface(sessionId, canonicalSessionId, storedSession)
    const authoritySessionId = surface.status === 'managed' ? surface.snapshot.canonical_session_id : canonicalSessionId
    const captured = captureProjectSubmitAuthority(authoritySessionId)

    return {
      legacyAuthority:
        surface.status === 'conclusively-legacy' && storedSession
          ? (captureExactLegacySessionAuthority({
              allowCrossProfileGateway: true,
              requireActiveGateway: true,
              runtimeSessionId: sessionId ?? null,
              storedSession,
              storedSessionId: storedSessionId ?? storedSession.id
            }) ?? undefined)
          : undefined,
      legacyDraftAuthority:
        surface.status === 'conclusively-legacy' && !sessionId && !storedSessionId && !storedSession
          ? (captureFrozenLegacyDraftAuthority() ?? undefined)
          : undefined,
      projectAuthority:
        surface.status === 'ambiguous' || surface.status === 'unavailable'
          ? { ...captured, status: 'ambiguous' }
          : captured,
      sessionId: sessionId ?? authoritySessionId,
      storedSessionId: authoritySessionId
    }
  }, [sessionId, storedSession, storedSessionId])

  const submitVoiceTurn = async (text: string, options: SubmitTextOptions) => {
    if (
      (options.legacyAuthority &&
        !validateExactLegacySessionAuthority(options.legacyAuthority, {
          runtimeSessionId: options.sessionId ?? null
        })) ||
      (options.legacyDraftAuthority && !validateFrozenLegacyDraftAuthority(options.legacyDraftAuthority))
    ) {
      insertText(text)
      notifyError(
        new Error(t.statusStack.managedProject.voiceScopeChanged),
        t.statusStack.managedProject.voiceScopeChanged
      )

      return
    }

    const capturedAuthority = options.projectAuthority

    const capturedResolution = capturedAuthority
      ? resolveCapturedProjectSubmitAuthority(capturedAuthority)
      : { status: 'stale' as const }

    if (!capturedAuthority || capturedResolution.status === 'stale') {
      if (capturedAuthority) {
        quarantineProjectVoicePrompt(capturedAuthority, text)
      }

      notifyError(
        new Error(t.statusStack.managedProject.voiceScopeChanged),
        t.statusStack.managedProject.voiceScopeChanged
      )

      return
    }

    if (busy && capturedResolution.status !== 'managed') {
      return
    }

    triggerHaptic('submit')
    resetBrowseState(options.sessionId)
    clearDraft()

    try {
      const accepted = await onSubmit(text, options)

      if (accepted === false) {
        if (resolveCapturedProjectSubmitAuthority(capturedAuthority).status === 'stale') {
          quarantineProjectVoicePrompt(capturedAuthority, text)
          notifyError(
            new Error(t.statusStack.managedProject.voiceScopeChanged),
            t.statusStack.managedProject.voiceScopeChanged
          )
        } else {
          insertText(text)
        }
      }
    } catch (error) {
      if (resolveCapturedProjectSubmitAuthority(capturedAuthority).status === 'stale') {
        quarantineProjectVoicePrompt(capturedAuthority, text)
      } else {
        insertText(text)
      }

      notifyError(error, t.composer.queueStuckTitle)
    }
  }

  const conversation = useVoiceConversation({
    busy,
    captureSubmitOptions,
    consumePendingResponse,
    enabled: voiceConversationActive,
    onFatalError: () => setVoiceConversationActive(false),
    onSubmit: submitVoiceTurn,
    onTranscribeAudio,
    pendingResponse: pendingTurnResponse
  })

  // The `composer.voice` hotkey (Ctrl+B) toggles the conversation. Starting
  // with STT unconfigured lets the conversation surface its own "configure
  // speech-to-text" notice rather than silently no-opping.
  const toggleVoiceConversation = useCallback(() => {
    if (disabled) {
      return
    }

    if (voiceConversationActive) {
      setVoiceConversationActive(false)
      void conversation.end()
    } else {
      setVoiceConversationActive(true)
    }
  }, [conversation, disabled, voiceConversationActive])

  useEffect(
    () => onComposerVoiceToggleRequest(toggled => toggled === target && toggleVoiceConversation()),
    [target, toggleVoiceConversation]
  )

  // Explicit start/end for the on-screen conversation controls (the hotkey uses
  // the gated toggle above).
  const startConversation = useCallback(() => setVoiceConversationActive(true), [])

  const endConversation = useCallback(() => {
    setVoiceConversationActive(false)
    void conversation.end()
  }, [conversation])

  const handleToggleAutoSpeak = useCallback(() => {
    void setAutoSpeakReplies(!$autoSpeakReplies.get()).catch(error =>
      notifyError(error, t.settings.config.autosaveFailed)
    )
  }, [t])

  useAutoSpeakReplies({
    conversationActive: voiceConversationActive,
    failureLabel: t.assistant.thread.readAloudFailed,
    markSpoken: consumePendingResponse,
    pendingReply: pendingResponse,
    sessionId
  })

  return {
    conversation,
    dictate,
    endConversation,
    handleToggleAutoSpeak,
    startConversation,
    voiceActivityState,
    voiceConversationActive,
    voiceStatus
  }
}
