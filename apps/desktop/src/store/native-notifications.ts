import { atom } from 'nanostores'

import { persistString, storedString } from '@/lib/storage'

import { $gateway } from './gateway'
import { $activeGatewayProfile, normalizeProfileKey } from './profile'
import {
  type ManagedProjectApproval,
  managedProjectApprovalForSurface,
  resolveManagedProjectApproval
} from './project-approval'
import { type ApprovalRequest, clearApprovalRequest, sessionApprovalRequest } from './prompts'
import { $activeSessionId } from './session'
import { $sessionStates } from './session-states'

// Native OS notifications (Electron `Notification`), separate from the in-app
// toast feed in `notifications.ts`. Each kind toggles independently.
export type NativeNotificationKind = 'approval' | 'backgroundDone' | 'credits' | 'input' | 'turnDone' | 'turnError'

export const NATIVE_NOTIFICATION_KINDS: readonly NativeNotificationKind[] = [
  'approval',
  'input',
  'turnDone',
  'turnError',
  'backgroundDone',
  'credits'
]

// Blocking prompts — surface even while focused if they're for another session.
const ATTENTION_KINDS = new Set<NativeNotificationKind>(['approval', 'input'])

export interface NativeNotificationPrefs {
  enabled: boolean
  kinds: Record<NativeNotificationKind, boolean>
}

const STORAGE_KEY = 'hermes:native-notifications'

const DEFAULT_PREFS: NativeNotificationPrefs = {
  enabled: true,
  kinds: { approval: true, backgroundDone: true, credits: true, input: true, turnDone: true, turnError: true }
}

function readPrefs(): NativeNotificationPrefs {
  const raw = storedString(STORAGE_KEY)

  if (!raw) {
    return DEFAULT_PREFS
  }

  try {
    const parsed = JSON.parse(raw) as Partial<NativeNotificationPrefs>
    const kinds = { ...DEFAULT_PREFS.kinds }

    for (const kind of NATIVE_NOTIFICATION_KINDS) {
      const value = parsed.kinds?.[kind]

      if (typeof value === 'boolean') {
        kinds[kind] = value
      }
    }

    return {
      enabled: typeof parsed.enabled === 'boolean' ? parsed.enabled : DEFAULT_PREFS.enabled,
      kinds
    }
  } catch {
    return DEFAULT_PREFS
  }
}

export const $nativeNotifyPrefs = atom<NativeNotificationPrefs>(readPrefs())

function writePrefs(next: NativeNotificationPrefs) {
  $nativeNotifyPrefs.set(next)
  persistString(STORAGE_KEY, JSON.stringify(next))
}

export function setNativeNotifyEnabled(enabled: boolean) {
  writePrefs({ ...$nativeNotifyPrefs.get(), enabled })
}

export function setNativeNotifyKind(kind: NativeNotificationKind, on: boolean) {
  const prev = $nativeNotifyPrefs.get()
  writePrefs({ ...prev, kinds: { ...prev.kinds, [kind]: on } })
}

// De-dupe replayed events for the same kind+session. Self-evicting: entries
// older than the window are pruned on every dispatch, so the map can't grow.
const THROTTLE_MS = 1000
const lastFiredAt = new Map<string, number>()

function throttled(key: string, now: number): boolean {
  for (const [k, at] of lastFiredAt) {
    if (now - at >= THROTTLE_MS) {
      lastFiredAt.delete(k)
    }
  }

  if (lastFiredAt.has(key)) {
    return true
  }

  lastFiredAt.set(key, now)

  return false
}

// "Backgrounded" = the user isn't on Hermes. `document.hidden` only flips when
// minimized/occluded; an alt-tabbed window is visible-but-unfocused, so we also
// check `document.hasFocus()`.
function isBackgrounded(): boolean {
  if (typeof document === 'undefined') {
    return false
  }

  if (document.hidden) {
    return true
  }

  return typeof document.hasFocus === 'function' && !document.hasFocus()
}

function shouldFire(kind: NativeNotificationKind, sessionId?: null | string, global = false): boolean {
  // Global notifications aren't tied to a chat session (e.g. pet generation,
  // which runs from the command center with no active conversation). They fire
  // whenever the user is away, with no session-match requirement — otherwise a
  // background run started without an open session would be silently dropped.
  if (global) {
    return isBackgrounded()
  }

  // Attention kinds break through for an off-screen session even while focused.
  if (ATTENTION_KINDS.has(kind)) {
    return isBackgrounded() || (Boolean(sessionId) && sessionId !== $activeSessionId.get())
  }

  // Completion kinds: only the active session, only while away — so a busy
  // gateway (messaging, kanban, cron) can't spam a toast per background session.
  return isBackgrounded() && Boolean(sessionId) && sessionId === $activeSessionId.get()
}

export interface NativeNotificationAction {
  id: string
  text: string
}

export type NativeApprovalNotificationContext =
  | { approval: ManagedProjectApproval; kind: 'managed' }
  | { kind: 'legacy'; token: string }

export type NativeApprovalNotificationSource =
  | { approval: ManagedProjectApproval; kind: 'managed' }
  | { kind: 'legacy'; request: ApprovalRequest }

export interface NativeNotificationInput {
  kind: NativeNotificationKind
  title: string
  body?: string
  sessionId?: null | string
  /**
   * Not tied to a chat session (e.g. pet generation). Fires whenever the user
   * is away, bypassing the session-match gate that completion kinds normally
   * require.
   */
  global?: boolean
  silent?: boolean
  actions?: NativeNotificationAction[]
  approvalSource?: NativeApprovalNotificationSource
}

interface LegacyApprovalRegistration {
  gateway: ReturnType<typeof $gateway.get>
  profile: string
  request: ApprovalRequest
  sessionId: null | string
}

const MAX_LEGACY_APPROVAL_REGISTRATIONS = 64
const legacyApprovalRegistrations = new Map<string, LegacyApprovalRegistration>()
let legacyApprovalSequence = 0

function registerApprovalNotification(
  source: NativeApprovalNotificationSource | undefined
): NativeApprovalNotificationContext | undefined {
  if (!source) {
    return undefined
  }

  if (source.kind === 'managed') {
    return { approval: { ...source.approval }, kind: 'managed' }
  }

  for (const [token, registered] of legacyApprovalRegistrations) {
    if (registered.sessionId === source.request.sessionId) {
      legacyApprovalRegistrations.delete(token)
    }
  }

  const token = `legacy-approval-${Date.now()}-${++legacyApprovalSequence}`
  legacyApprovalRegistrations.set(token, {
    gateway: $gateway.get(),
    profile: normalizeProfileKey($activeGatewayProfile.get()),
    request: source.request,
    sessionId: source.request.sessionId
  })

  while (legacyApprovalRegistrations.size > MAX_LEGACY_APPROVAL_REGISTRATIONS) {
    const oldest = legacyApprovalRegistrations.keys().next().value as string | undefined

    if (!oldest) {
      break
    }

    legacyApprovalRegistrations.delete(oldest)
  }

  return { kind: 'legacy', token }
}

export function dispatchNativeNotification(input: NativeNotificationInput): void {
  const prefs = $nativeNotifyPrefs.get()

  if (!prefs.enabled || !prefs.kinds[input.kind]) {
    return
  }

  if (!shouldFire(input.kind, input.sessionId, input.global)) {
    return
  }

  if (throttled(`${input.kind}:${input.sessionId ?? (input.global ? 'global' : '')}`, Date.now())) {
    return
  }

  const approvalContext = input.kind === 'approval' ? registerApprovalNotification(input.approvalSource) : undefined

  void window.hermesDesktop?.notify({
    actions: input.actions,
    approvalContext,
    body: input.body,
    kind: input.kind,
    sessionId: input.sessionId ?? undefined,
    silent: input.silent,
    title: input.title
  })
}

// Resolve a pending approval from a notification button, mirroring the in-app
// Run/Reject bar. The transported context identifies the exact prompt that
// created this notification; session id alone never authorizes a response.
export async function respondToApprovalAction(
  sessionId: null | string,
  actionId: string,
  approvalContext?: NativeApprovalNotificationContext
): Promise<void> {
  const choice = actionId === 'approve' ? 'once' : actionId === 'reject' ? 'deny' : null

  if (!choice || !approvalContext || typeof approvalContext !== 'object') {
    return
  }

  const storedSessionId = sessionId ? $sessionStates.get()[sessionId]?.storedSessionId : null

  // A notification callback only carries the ephemeral gateway id. Never
  // reinterpret it as durable identity: the runtime cache is the upstream
  // authority that paired this live process id with its stored conversation.
  if (!sessionId || !storedSessionId) {
    return
  }

  const managedSelection = managedProjectApprovalForSurface(sessionId, storedSessionId).get()

  if (approvalContext.kind === 'managed') {
    if (!approvalContext.approval || typeof approvalContext.approval !== 'object') {
      return
    }

    const current = managedSelection.approval

    if (
      !managedSelection.managed ||
      !current ||
      current.approvalId !== approvalContext.approval.approvalId ||
      current.approvalKind !== approvalContext.approval.approvalKind ||
      current.bindingId !== approvalContext.approval.bindingId ||
      current.projectId !== approvalContext.approval.projectId ||
      current.requesterGeneration !== approvalContext.approval.requesterGeneration ||
      current.requesterScope !== approvalContext.approval.requesterScope ||
      current.runtimeSessionId !== approvalContext.approval.runtimeSessionId ||
      current.sessionId !== approvalContext.approval.sessionId ||
      current.storedSessionId !== approvalContext.approval.storedSessionId ||
      current.version !== approvalContext.approval.version
    ) {
      return
    }

    clearApprovalRequest(sessionId)

    try {
      await resolveManagedProjectApproval(approvalContext.approval, choice === 'once' ? 'approved' : 'denied')
    } catch {
      // Canonical state remains authoritative and the in-app managed surface
      // exposes conflict/retry/failure feedback.
    }

    return
  }

  if (approvalContext.kind !== 'legacy' || typeof approvalContext.token !== 'string') {
    return
  }

  const registered = legacyApprovalRegistrations.get(approvalContext.token)
  legacyApprovalRegistrations.delete(approvalContext.token)
  const gateway = $gateway.get()

  if (
    managedSelection.managed ||
    !registered ||
    !gateway ||
    registered.gateway !== gateway ||
    registered.profile !== normalizeProfileKey($activeGatewayProfile.get()) ||
    registered.sessionId !== sessionId ||
    sessionApprovalRequest(sessionId).get() !== registered.request
  ) {
    return
  }

  try {
    await gateway.request('approval.respond', { choice, session_id: sessionId ?? undefined })
    clearApprovalRequest(sessionId)
  } catch {
    // Leave the prompt parked so the user can still resolve it in-app.
  }
}

// Settings "send test" — bypasses gating. Returns whether the OS accepted it so
// the panel can flag a silent permission failure instead of looking dead.
export async function sendTestNativeNotification(title: string, body: string): Promise<boolean> {
  const bridge = window.hermesDesktop

  if (!bridge?.notify) {
    return false
  }

  try {
    return await bridge.notify({ body, kind: 'turnDone', title })
  } catch {
    return false
  }
}
