import { act, cleanup, renderHook } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { $queuedPromptsBySession } from '@/store/composer-queue'
import { setPrimaryGateway } from '@/store/gateway'
import { $managedVoiceRecoveries } from '@/store/project-composer-queue'
import { $projectRuntimes, configureProjectRuntimeRequester, resetProjectRuntimeStore } from '@/store/project-runtime'
import { $activeProjectId, $projectCatalogAuthority, $projects } from '@/store/projects'
import type { SessionInfo } from '@/types/hermes'

import type { SubmitTextOptions } from '../../../session/hooks/use-prompt-actions/utils'

import { useComposerVoice } from './use-composer-voice'

const voiceConversation = vi.hoisted(() => ({
  args: null as null | {
    captureSubmitOptions: () => SubmitTextOptions
    onSubmit: (text: string, options: SubmitTextOptions) => Promise<void>
  },
  hook: vi.fn(
    (args: {
      captureSubmitOptions: () => SubmitTextOptions
      onSubmit: (text: string, options: SubmitTextOptions) => Promise<void>
    }) => {
      voiceConversation.args = args

      return { end: vi.fn(async () => undefined) }
    }
  )
}))

vi.mock('./use-voice-conversation', () => ({ useVoiceConversation: voiceConversation.hook }))
vi.mock('./use-voice-recorder', () => ({
  useVoiceRecorder: () => ({ dictate: vi.fn(), voiceActivityState: null, voiceStatus: 'idle' })
}))
vi.mock('./use-auto-speak-replies', () => ({ useAutoSpeakReplies: vi.fn() }))

const setManagedRuntime = (canonicalSessionId: string, overrides: { bindingId?: string; projectId?: string } = {}) => {
  const projectId = overrides.projectId ?? 'project-a'
  $projectRuntimes.set({
    [projectId]: {
      events: [],
      snapshot: {
        active_run: { control_state: 'running', control_version: 1, turn_id: 'turn-a' },
        artifacts: [],
        binding_id: overrides.bindingId ?? 'binding-a',
        block: null,
        canonical_session_id: canonicalSessionId,
        current_phase: 'implementation',
        delivery_status: { error_code: null, state: 'caught_up' },
        last_sequence: 1,
        lifecycle: 'active',
        pending_approval: null,
        project_id: projectId,
        queue: [],
        transcript: [],
        transcript_revision: 0,
        version: 2
      }
    }
  })
}

const renderVoice = (
  options: {
    busy?: boolean
    draft?: boolean
    managed?: boolean
    sessionId?: string
    storedSession?: SessionInfo
    storedSessionId?: string
    submitResult?: boolean
  } = {}
) => {
  if (options.managed) {
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    setManagedRuntime(options.storedSessionId ?? options.sessionId ?? 'runtime-session')
  }

  const insertText = vi.fn()
  const onSubmit = vi.fn(async () => options.submitResult ?? true)

  const hook = renderHook(
    ({ sessionId }: { sessionId: null | string }) =>
      useComposerVoice({
        busy: options.busy ?? true,
        clearDraft: vi.fn(),
        disabled: false,
        focusInput: vi.fn(),
        insertText,
        maxRecordingSeconds: 120,
        onSubmit,
        onTranscribeAudio: vi.fn(async () => ''),
        sessionId,
        storedSession: options.storedSession,
        storedSessionId: options.storedSessionId,
        target: 'main'
      }),
    { initialProps: { sessionId: options.draft ? null : (options.sessionId ?? 'runtime-session') } }
  )

  return { hook, insertText, onSubmit }
}

describe('useComposerVoice managed busy routing', () => {
  afterEach(() => {
    cleanup()
    resetProjectRuntimeStore()
    $activeProjectId.set(null)
    $projects.set([])
    $projectCatalogAuthority.set({ catalogGeneration: null, contextGeneration: 0, profile: null })
    configureProjectRuntimeRequester(undefined)
    setPrimaryGateway(null, 'default')
    $queuedPromptsBySession.set({})
    $managedVoiceRecoveries.set({})
    voiceConversation.args = null
    vi.restoreAllMocks()
  })

  it('routes a busy managed voice turn through the canonical submit callback', async () => {
    const { onSubmit } = renderVoice({ managed: true })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    await act(async () => voiceConversation.args!.onSubmit('voice follow-up', capturedOptions))

    expect(onSubmit).toHaveBeenCalledWith('voice follow-up', capturedOptions)
  })

  it('captures managed authority under stored C while preserving live R only as transport identity', () => {
    renderVoice({ managed: true, sessionId: 'runtime-R', storedSessionId: 'stored-C' })

    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    expect(capturedOptions.projectAuthority).toMatchObject({ sessionId: 'stored-C', status: 'managed' })
    expect(capturedOptions.sessionId).toBe('runtime-R')
    expect(capturedOptions.storedSessionId).toBe('stored-C')
  })

  it('captures the active managed canonical C for a draft without stored or live session ids', () => {
    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'default'
    )
    setManagedRuntime('managed-draft-C')
    $activeProjectId.set('project-a')

    renderVoice({ draft: true })

    expect(voiceConversation.args!.captureSubmitOptions()).toMatchObject({
      projectAuthority: { sessionId: 'managed-draft-C', status: 'managed' },
      sessionId: 'managed-draft-C',
      storedSessionId: 'managed-draft-C'
    })
  })

  it('restores a fresh legacy voice turn when its draft authority is replaced during transcription', async () => {
    setPrimaryGateway({ request: vi.fn() } as never, 'default')
    configureProjectRuntimeRequester(vi.fn(async () => undefined), 'default')
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })
    $projects.set([])
    $activeProjectId.set(null)

    const { insertText, onSubmit } = renderVoice({ busy: false, draft: true })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    expect(capturedOptions.legacyDraftAuthority).toBeDefined()

    setManagedRuntime('managed-draft-C')
    $projects.set([{ id: 'project-a', managed: true } as never])
    $activeProjectId.set('project-a')

    // Return to the same visible legacy draft before transcription settles.
    // The immutable generations must still reveal that its producer vanished.
    $activeProjectId.set(null)
    $projects.set([])
    $projectRuntimes.set({})
    $projectCatalogAuthority.set({ catalogGeneration: 1, contextGeneration: 1, profile: 'default' })

    await act(async () => voiceConversation.args!.onSubmit('voice draft to preserve', capturedOptions))

    expect(onSubmit).not.toHaveBeenCalled()
    expect(insertText).toHaveBeenCalledWith('voice draft to preserve')
    expect($managedVoiceRecoveries.get()).toEqual({})
  })

  it('restores an exact legacy voice turn when its gateway is replaced during transcription', async () => {
    const storedSession = {
      id: 'stored-voice',
      profile: 'default',
      project_id: null
    } as SessionInfo

    setPrimaryGateway({ request: vi.fn() } as never, 'default')
    configureProjectRuntimeRequester(vi.fn(async () => undefined), 'default')
    $projectCatalogAuthority.set({ catalogGeneration: 0, contextGeneration: 0, profile: 'default' })

    const { insertText, onSubmit } = renderVoice({
      busy: false,
      sessionId: 'runtime-voice',
      storedSession,
      storedSessionId: storedSession.id
    })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    expect(capturedOptions.legacyAuthority).toBeDefined()

    setPrimaryGateway({ request: vi.fn() } as never, 'default')

    await act(async () => voiceConversation.args!.onSubmit('voice turn to preserve', capturedOptions))

    expect(onSubmit).not.toHaveBeenCalled()
    expect(insertText).toHaveBeenCalledWith('voice turn to preserve')
    expect($managedVoiceRecoveries.get()).toEqual({})
  })

  it('keeps unmanaged busy voice behavior unchanged', async () => {
    const { onSubmit } = renderVoice()
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    await act(async () => voiceConversation.args!.onSubmit('legacy busy voice', capturedOptions))

    expect(onSubmit).not.toHaveBeenCalled()
  })

  it('restores a rejected managed voice turn into the visible draft', async () => {
    const { insertText } = renderVoice({ managed: true, submitResult: false })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    await act(async () => voiceConversation.args!.onSubmit('please retry me', capturedOptions))

    expect(insertText).toHaveBeenCalledWith('please retry me')
  })

  it('submits a transcribed turn to the session captured before the active surface changes', async () => {
    const { hook, onSubmit } = renderVoice({ managed: true, sessionId: 'runtime-session-a' })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    hook.rerender({ sessionId: 'runtime-session-b' })
    await act(async () => voiceConversation.args!.onSubmit('captured before switch', capturedOptions))

    expect(onSubmit).toHaveBeenCalledWith('captured before switch', capturedOptions)
  })

  it('quarantines an A voice turn when the same project/session is replaced by profile generation B', async () => {
    const requesterA = vi.fn(async () => undefined)
    configureProjectRuntimeRequester(requesterA, ' profile-a ')
    setManagedRuntime('runtime-session')
    const { insertText, onSubmit } = renderVoice({ managed: false, sessionId: 'runtime-session' })
    const capturedOptions = voiceConversation.args!.captureSubmitOptions()

    configureProjectRuntimeRequester(
      vi.fn(async () => undefined),
      'profile-b'
    )
    setManagedRuntime('runtime-session', { bindingId: 'binding-b' })

    await act(async () => voiceConversation.args!.onSubmit('must remain under A', capturedOptions))

    expect(onSubmit).not.toHaveBeenCalled()
    expect(insertText).not.toHaveBeenCalled()
    expect($queuedPromptsBySession.get()).toEqual({})
    expect(Object.values($managedVoiceRecoveries.get()).flat()).toEqual([
      expect.objectContaining({ attachments: [], text: 'must remain under A' })
    ])
  })
})
