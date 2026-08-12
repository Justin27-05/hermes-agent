import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { ManagedComposerAction } from '@/store/project-composer-queue'

import { ManagedProjectActionRow } from './managed-project-action-row'

vi.mock('@/i18n', () => ({
  useI18n: () => ({ t: { common: { retry: 'i18n retry frozen intent' } } })
}))

const retryAction: ManagedComposerAction = {
  binding_id: 'binding-a',
  intent_id: 'intent-frozen',
  local_id: 'local-a',
  message: 'Delivery is ambiguous.',
  project_id: 'project-a',
  session_id: 'session-a',
  status: 'retry_required',
  text: 'Ship this'
}

describe('ManagedProjectActionRow', () => {
  it('exposes an explicit Retry action for the frozen managed intent', () => {
    const onRetry = vi.fn()

    render(<ManagedProjectActionRow action={retryAction} onRetry={onRetry} />)
    fireEvent.click(screen.getByRole('button', { name: 'i18n retry frozen intent' }))

    expect(onRetry).toHaveBeenCalledTimes(1)
    expect(screen.getByText('Delivery is ambiguous.')).not.toBeNull()
  })

  it('shows a managed conflict without offering a fresh retry', () => {
    render(<ManagedProjectActionRow action={{ ...retryAction, status: 'conflict' }} onRetry={vi.fn()} />)

    expect(screen.queryByRole('button', { name: 'i18n retry frozen intent' })).toBeNull()
    expect(screen.getByText('Delivery is ambiguous.')).not.toBeNull()
  })
})
