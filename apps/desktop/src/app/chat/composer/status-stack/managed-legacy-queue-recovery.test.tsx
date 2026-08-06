import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { ManagedLegacyQueueRecovery } from './managed-legacy-queue-recovery'

vi.mock('@/i18n', () => ({
  useI18n: () => ({
    t: {
      statusStack: {
        managedProject: {
          legacyDraftRecovery: 'i18n legacy recovery',
          voiceDraftRecovery: 'i18n voice recovery',
          restoreDraft: 'i18n restore draft'
        }
      }
    }
  })
}))

describe('ManagedLegacyQueueRecovery', () => {
  it('presents legacy text only as an explicit Restore draft action', () => {
    const onRestore = vi.fn()

    render(
      <ManagedLegacyQueueRecovery
        entries={[{ attachments: [], id: 'legacy-a', queuedAt: 1, text: 'recover this text' }]}
        onRestore={onRestore}
      />
    )

    expect(screen.getByText('i18n legacy recovery')).not.toBeNull()
    expect(screen.queryByRole('button', { name: /send/i })).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'i18n restore draft' }))
    expect(onRestore).toHaveBeenCalledWith('legacy-a')
  })

  it('labels quarantined voice text as voice draft recovery', () => {
    render(
      <ManagedLegacyQueueRecovery
        entries={[{ attachments: [], id: 'voice-a', queuedAt: 1, text: 'voice draft' }]}
        kind="voice"
        onRestore={vi.fn()}
      />
    )

    expect(screen.getByText('i18n voice recovery')).not.toBeNull()
  })
})
