import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { MicRecorderOptions } from './use-mic-recorder'
import { useVoiceConversation } from './use-voice-conversation'

const mic = vi.hoisted(() => ({
  options: null as MicRecorderOptions | null,
  start: vi.fn(async (options?: MicRecorderOptions) => {
    mic.options = options ?? null
  }),
  stop: vi.fn(async () => ({
    audio: new Blob(['voice'], { type: 'audio/webm' }),
    durationMs: 500,
    heardSpeech: true
  }))
}))

const barge = vi.hoisted(() => ({
  callbacks: null as null | { onSpeech: () => void; onUtterance?: (audio: Blob | null) => void }
}))

vi.mock('./use-mic-recorder', () => ({
  useMicRecorder: () => ({
    handle: { cancel: vi.fn(), start: mic.start, stop: mic.stop },
    level: 0,
    recording: false
  })
}))

vi.mock('@/lib/voice-barge-in', () => ({
  monitorSpeechDuringPlayback: (callbacks: { onSpeech: () => void; onUtterance?: (audio: Blob | null) => void }) => {
    barge.callbacks = callbacks

    return vi.fn()
  }
}))

vi.mock('@/lib/voice-playback', () => ({
  markVoicePlaybackInterrupted: vi.fn(),
  playSpeechText: vi.fn(async () => undefined),
  startSpeechStream: vi.fn(async () => null),
  stopVoicePlayback: vi.fn()
}))

describe('useVoiceConversation frozen submit authority', () => {
  afterEach(() => {
    cleanup()
    mic.options = null
    barge.callbacks = null
    vi.clearAllMocks()
  })

  it('captures submit options before ordinary speech-to-text awaits', async () => {
    let authority = 'authority-a'
    const events: string[] = []
    const onSubmit = vi.fn(async () => undefined)

    const hook = renderHook(() =>
      useVoiceConversation({
        busy: false,
        captureSubmitOptions: () => {
          events.push(`capture:${authority}`)

          return { authority }
        },
        consumePendingResponse: vi.fn(),
        enabled: true,
        onSubmit,
        onTranscribeAudio: async () => {
          events.push('transcribe')
          authority = 'authority-b'

          return 'ordinary voice turn'
        },
        pendingResponse: () => null
      } as never)
    )

    await act(async () => hook.result.current.start())
    act(() => mic.options?.onSilence?.())

    await waitFor(() => expect(onSubmit).toHaveBeenCalledWith('ordinary voice turn', { authority: 'authority-a' }))
    expect(events).toEqual(['capture:authority-a', 'transcribe'])
  })

  it('captures submit options at barge-in speech detection rather than utterance completion', async () => {
    let authority = 'initial-authority'
    let pendingResponse: null | { id: string; pending: boolean; text: string } = null
    const onSubmit = vi.fn(async () => undefined)

    const onTranscribeAudio = vi
      .fn()
      .mockResolvedValueOnce('initial voice turn')
      .mockResolvedValueOnce('barge voice turn')

    const hook = renderHook(
      ({ busy }: { busy: boolean }) =>
        useVoiceConversation({
          busy,
          captureSubmitOptions: () => ({ authority }),
          consumePendingResponse: vi.fn(),
          enabled: true,
          onSubmit,
          onTranscribeAudio,
          pendingResponse: () => pendingResponse
        } as never),
      { initialProps: { busy: false } }
    )

    await act(async () => hook.result.current.start())
    hook.rerender({ busy: true })
    act(() => mic.options?.onSilence?.())
    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1))
    await waitFor(() => expect(hook.result.current.status).toBe('thinking'))

    pendingResponse = { id: 'assistant-1', pending: true, text: 'streaming reply' }
    hook.rerender({ busy: true })
    await waitFor(() => expect(barge.callbacks).not.toBeNull())

    authority = 'barge-authority-a'
    act(() => barge.callbacks?.onSpeech())
    authority = 'barge-authority-b'
    act(() => barge.callbacks?.onUtterance?.(new Blob(['barge'], { type: 'audio/webm' })))

    await waitFor(() =>
      expect(onSubmit).toHaveBeenLastCalledWith('barge voice turn', { authority: 'barge-authority-a' })
    )
  })
})
