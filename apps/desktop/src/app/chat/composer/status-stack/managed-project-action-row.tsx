import { StatusRow } from '@/components/chat/status-row'
import { Button } from '@/components/ui/button'
import { Codicon } from '@/components/ui/codicon'
import { useI18n } from '@/i18n'
import type { ManagedComposerAction } from '@/store/project-composer-queue'

interface ManagedProjectActionRowProps {
  action: ManagedComposerAction
  onRetry: () => void
}

export function ManagedProjectActionRow({ action, onRetry }: ManagedProjectActionRowProps) {
  const { t } = useI18n()
  const canRetry = action.status === 'retry_required'

  return (
    <StatusRow
      leading={
        <Codicon
          className={canRetry ? 'text-amber-500/90' : 'text-destructive/80'}
          name={canRetry ? 'warning' : 'error'}
          size="0.8rem"
        />
      }
      trailing={
        canRetry ? (
          <Button onClick={onRetry} size="micro" type="button" variant="text">
            {t.common.retry}
          </Button>
        ) : undefined
      }
    >
      <span className="min-w-0 max-w-[24rem] truncate text-[0.73rem] leading-4 text-foreground/92">
        {action.message}
      </span>
    </StatusRow>
  )
}
