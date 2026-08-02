import { StatusRow } from '@/components/chat/status-row'
import { StatusSection } from '@/components/chat/status-section'
import { Button } from '@/components/ui/button'
import { Codicon } from '@/components/ui/codicon'
import { useI18n } from '@/i18n'
import type { QueuedPromptEntry } from '@/store/composer-queue'

interface ManagedLegacyQueueRecoveryProps {
  entries: QueuedPromptEntry[]
  kind?: 'legacy' | 'voice'
  onRestore: (id: string) => void
}

export function ManagedLegacyQueueRecovery({ entries, kind = 'legacy', onRestore }: ManagedLegacyQueueRecoveryProps) {
  const { t } = useI18n()

  if (!entries.length) {
    return null
  }

  return (
    <StatusSection
      defaultCollapsed={false}
      icon={<Codicon className="text-amber-500/80" name="archive" size="0.8rem" />}
      label={
        kind === 'voice'
          ? t.statusStack.managedProject.voiceDraftRecovery
          : t.statusStack.managedProject.legacyDraftRecovery
      }
    >
      {entries.map(entry => (
        <StatusRow
          key={entry.id}
          leading={<Codicon className="text-muted-foreground/70" name="edit" size="0.8rem" />}
          trailing={
            <Button onClick={() => onRestore(entry.id)} size="micro" type="button" variant="text">
              {t.statusStack.managedProject.restoreDraft}
            </Button>
          }
        >
          <span className="min-w-0 max-w-[24rem] truncate text-[0.73rem] leading-4 text-foreground/92">
            {entry.text || entry.attachments.map(attachment => attachment.label).join(', ')}
          </span>
        </StatusRow>
      ))}
    </StatusSection>
  )
}
