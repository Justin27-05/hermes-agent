import { readDesktopFileDataUrl } from '@/lib/desktop-fs'
import { filePathFromMediaPath, isRemoteGateway, mediaExternalUrl } from '@/lib/media'
import { normalizeProfileKey } from '@/store/profile'
import type { ProjectRuntimeState } from '@/store/project-runtime'
import type { ProjectInfo, SessionInfo, SessionMessage } from '@/types/hermes'

import type { SidebarProjectTree } from '../chat/sidebar/projects/workspace-groups'

export type ArtifactKind = 'image' | 'file' | 'link'
export type ArtifactFilter = 'all' | ArtifactKind
export const ARTIFACT_FILTERS: readonly ArtifactFilter[] = ['all', 'image', 'file', 'link']

export interface ArtifactRecord {
  /** Only canonical artifact presentations may set this to `canonical`. */
  source: 'canonical' | 'legacy'
  id: string
  kind: ArtifactKind
  /** Canonical local artifacts deliberately have no value or href. */
  value: string
  href: null | string
  label: string
  profile: string
  projectId: null | string
  sessionId: string
  sessionTitle: string
  timestamp: number
  sizeBytes: null | number
  sha256: null | string
}

const MARKDOWN_IMAGE_RE = /!\[([^\]]*)\]\(([^)\s]+)\)/g
const MARKDOWN_LINK_RE = /\[([^\]]+)\]\(([^)\s]+)\)/g
const URL_RE = /https?:\/\/[^\s<>"')]+/g
const PATH_RE = /(^|[\s("'`])((?:\/|~\/|\.\.?\/)[^\s"'`<>]+(?:\.[a-z0-9]{1,8})?)/gi
const IMAGE_EXT_RE = /\.(?:png|jpe?g|gif|webp|svg|bmp)(?:\?.*)?$/i
const FILE_EXT_RE = /\.(?:png|jpe?g|gif|webp|svg|bmp|pdf|txt|json|md|csv|zip|tar|gz|mp3|wav|mp4|mov)(?:\?.*)?$/i
const KEY_HINT_RE = /(path|file|url|image|artifact|output|download|result|target)/i

export function artifactSessionIdentity(profile: null | string | undefined, sessionId: string): string {
  return JSON.stringify([normalizeProfileKey(profile), sessionId])
}

function artifactIdentity(...parts: string[]): string {
  return JSON.stringify(parts)
}

function artifactSessionTitle(session: SessionInfo): string {
  return session.title?.trim() || session.preview?.trim() || 'Untitled session'
}

function normalizeValue(value: string): string {
  return value.trim().replace(/[),.;]+$/, '')
}

function parseMaybeJson(value: string): unknown {
  if (!value.trim()) {
    return null
  }

  try {
    return JSON.parse(value)
  } catch {
    return null
  }
}

function looksLikePathOrUrl(value: string): boolean {
  return (
    value.startsWith('http://') ||
    value.startsWith('https://') ||
    value.startsWith('file://') ||
    value.startsWith('data:image/') ||
    value.startsWith('/') ||
    value.startsWith('./') ||
    value.startsWith('../') ||
    value.startsWith('~/')
  )
}

function looksLikeArtifact(value: string): boolean {
  if (/^(?:https?:\/\/|data:image\/)/.test(value)) {
    return true
  }

  if (looksLikePathOrUrl(value) && (IMAGE_EXT_RE.test(value) || FILE_EXT_RE.test(value))) {
    return true
  }

  return value.startsWith('/') && value.includes('.')
}

function artifactKind(value: string): ArtifactKind {
  if (value.startsWith('data:image/') || IMAGE_EXT_RE.test(value)) {
    return 'image'
  }

  if (
    value.startsWith('/') ||
    value.startsWith('./') ||
    value.startsWith('../') ||
    value.startsWith('~/') ||
    value.startsWith('file://')
  ) {
    return 'file'
  }

  return 'link'
}

function artifactHref(value: string): string {
  if (value.startsWith('http://') || value.startsWith('https://') || value.startsWith('data:')) {
    return value
  }

  if (value.startsWith('file://') || value.startsWith('/')) {
    return mediaExternalUrl(value)
  }

  return value
}

export async function artifactImageSrc(value: string, href = artifactHref(value)): Promise<string> {
  if (/^(?:https?|data):/i.test(value)) {
    return href
  }

  if (typeof window !== 'undefined' && window.hermesDesktop && isRemoteGateway()) {
    return readDesktopFileDataUrl(filePathFromMediaPath(value))
  }

  return href
}

function artifactLabel(value: string): string {
  try {
    const url = new URL(value)
    const item = url.pathname.split('/').filter(Boolean).pop()

    return item || value
  } catch {
    const parts = value.split(/[\\/]/).filter(Boolean)

    return parts.pop() || value
  }
}

function messageText(message: SessionMessage): string {
  if (typeof message.content === 'string' && message.content.trim()) {
    return message.content
  }

  if (typeof message.text === 'string' && message.text.trim()) {
    return message.text
  }

  if (typeof message.context === 'string' && message.context.trim()) {
    return message.context
  }

  return ''
}

function collectStringValues(
  value: unknown,
  keyPath: string,
  collector: (value: string, keyPath: string) => void
): void {
  if (typeof value === 'string') {
    collector(value, keyPath)

    return
  }

  if (Array.isArray(value)) {
    value.forEach((entry, index) => collectStringValues(entry, `${keyPath}.${index}`, collector))

    return
  }

  if (!value || typeof value !== 'object') {
    return
  }

  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    collectStringValues(child, keyPath ? `${keyPath}.${key}` : key, collector)
  }
}

function collectArtifactsFromText(text: string, pushValue: (value: string) => void): void {
  for (const match of text.matchAll(MARKDOWN_IMAGE_RE)) {
    pushValue(match[2] || '')
  }

  for (const match of text.matchAll(MARKDOWN_LINK_RE)) {
    const start = match.index ?? 0

    if (start > 0 && text[start - 1] === '!') {
      continue
    }

    const value = match[2] || ''

    if (looksLikeArtifact(value)) {
      pushValue(value)
    }
  }

  for (const match of text.matchAll(URL_RE)) {
    const value = match[0] || ''

    if (looksLikeArtifact(value)) {
      pushValue(value)
    }
  }

  for (const match of text.matchAll(PATH_RE)) {
    pushValue(match[2] || '')
  }
}

function collectArtifactsFromMessage(message: SessionMessage, pushValue: (value: string) => void): void {
  const text = messageText(message)

  if (text) {
    collectArtifactsFromText(text, pushValue)
  }

  if (message.role !== 'tool' && !Array.isArray(message.tool_calls)) {
    return
  }

  if (Array.isArray(message.tool_calls)) {
    for (const call of message.tool_calls) {
      collectStringValues(call, 'tool_call', (value, keyPath) => {
        const normalized = normalizeValue(value)

        if (!normalized) {
          return
        }

        if (KEY_HINT_RE.test(keyPath) && (looksLikePathOrUrl(normalized) || FILE_EXT_RE.test(normalized))) {
          pushValue(normalized)
        }
      })
    }
  }

  const parsed = parseMaybeJson(text)

  if (parsed !== null) {
    collectStringValues(parsed, 'tool_result', (value, keyPath) => {
      const normalized = normalizeValue(value)

      if (!normalized) {
        return
      }

      if ((KEY_HINT_RE.test(keyPath) || looksLikePathOrUrl(normalized)) && looksLikeArtifact(normalized)) {
        pushValue(normalized)
      }
    })
  }
}

export function collectArtifactsForSession(session: SessionInfo, messages: SessionMessage[]): ArtifactRecord[] {
  const found = new Map<string, ArtifactRecord>()
  const profile = normalizeProfileKey(session.profile)
  const title = artifactSessionTitle(session)

  for (const message of messages) {
    if (message.role !== 'assistant' && message.role !== 'tool') {
      continue
    }

    collectArtifactsFromMessage(message, candidate => {
      const value = normalizeValue(candidate)

      if (!value || !looksLikeArtifact(value)) {
        return
      }

      const key = artifactIdentity('legacy', profile, session.id, value)

      if (found.has(key)) {
        return
      }

      found.set(key, {
        source: 'legacy',
        id: key,
        kind: artifactKind(value),
        value,
        href: artifactHref(value),
        label: artifactLabel(value),
        profile,
        projectId: null,
        sessionId: session.id,
        sessionTitle: title,
        timestamp: message.timestamp || session.last_active || session.started_at || Date.now(),
        sizeBytes: null,
        sha256: null
      })
    })
  }

  return Array.from(found.values())
}

/**
 * Converts the allowlisted runtime presentation to the artifact index model.
 * It intentionally has no transcript argument: managed artifact provenance is
 * exclusively the canonical ProjectRuntime snapshot.
 */
export function collectArtifactsForProjectRuntimes(
  runtimes: Readonly<Record<string, ProjectRuntimeState>>,
  sessions: readonly SessionInfo[],
  profile: string
): ArtifactRecord[] {
  const runtimeProfile = normalizeProfileKey(profile)

  const sessionTitles = new Map(
    sessions
      .filter(session => normalizeProfileKey(session.profile) === runtimeProfile)
      .map(session => [artifactSessionIdentity(runtimeProfile, session.id), artifactSessionTitle(session)])
  )

  const records: ArtifactRecord[] = []

  for (const state of Object.values(runtimes)) {
    const { snapshot } = state
    const sessionId = snapshot.canonical_session_id
    const sessionIdentity = artifactSessionIdentity(runtimeProfile, sessionId)
    const sessionTitle = sessionTitles.get(sessionIdentity) || 'Managed project'

    for (const artifact of snapshot.artifacts) {
      const { presentation } = artifact
      const href = presentation.open_target?.href ?? null

      records.push({
        source: 'canonical',
        id: artifactIdentity(
          'canonical',
          runtimeProfile,
          snapshot.project_id,
          snapshot.canonical_session_id,
          snapshot.binding_id,
          artifact.artifact_id
        ),
        kind: presentation.kind,
        value: href || '',
        href,
        label: presentation.label,
        profile: runtimeProfile,
        projectId: snapshot.project_id,
        sessionId,
        sessionTitle,
        timestamp: presentation.created_at,
        sizeBytes: presentation.size_bytes,
        sha256: presentation.sha256
      })
    }
  }

  return records
}

/** Session transcripts are retained only for sessions with no managed runtime. */
export function managedCanonicalSessionIdentities(
  runtimes: Readonly<Record<string, { snapshot: { canonical_session_id: string } }>>,
  profile: string
): ReadonlySet<string> {
  return new Set(
    Object.values(runtimes).map(({ snapshot }) => artifactSessionIdentity(profile, snapshot.canonical_session_id))
  )
}

export function legacyArtifactSessions(
  sessions: readonly SessionInfo[],
  runtimes: Readonly<Record<string, { snapshot: { canonical_session_id: string } }>>,
  projects: readonly ProjectInfo[],
  projectScope = 'default',
  _projectTree: readonly SidebarProjectTree[] = [],
  projectsScope: null | string = projectScope,
  projectsGeneration: null | number = 0,
  projectsContextGeneration = projectsGeneration
): SessionInfo[] {
  const normalizedScope = normalizeProfileKey(projectScope)

  const catalogIsCurrent =
    projectsScope !== null &&
    normalizeProfileKey(projectsScope) === normalizedScope &&
    projectsGeneration !== null &&
    projectsGeneration === projectsContextGeneration

  const projectsById = new Map(projects.map(project => [project.id, project]))
  const managedSessionIdentities = managedCanonicalSessionIdentities(runtimes, normalizedScope)

  return sessions.filter(session => {
    const profile = normalizeProfileKey(session.profile)
    const projectId = session.project_id

    if (profile === normalizedScope && managedSessionIdentities.has(artifactSessionIdentity(profile, session.id))) {
      return false
    }

    if (projectId === null || projectId === undefined || projectId === '') {
      return true
    }

    if (profile !== normalizedScope) {
      return false
    }

    return catalogIsCurrent && projectsById.get(projectId)?.managed === false
  })
}

interface ArtifactAuthority {
  currentScope: string
  loadedScope: null | string
  projects: readonly ProjectInfo[]
  projectsContextGeneration: number
  projectsGeneration: null | number
  projectsScope: null | string
  runtimeScope: null | string
  runtimes: Readonly<Record<string, ProjectRuntimeState>>
  sessions: readonly SessionInfo[]
}

/**
 * Synchronous render boundary for already indexed rows. Authority changes may
 * arrive while an older transcript refresh is still pending, so the view must
 * prune stale legacy and canonical records without waiting for that request.
 */
export function artifactsForCurrentAuthority(
  artifacts: readonly ArtifactRecord[],
  authority: ArtifactAuthority
): ArtifactRecord[] {
  if (authority.loadedScope === null || authority.loadedScope !== authority.currentScope) {
    return []
  }

  const sessionsByIdentity = new Map(
    authority.sessions.map(session => [artifactSessionIdentity(session.profile, session.id), session])
  )

  const runtimeIsCurrent =
    authority.runtimeScope !== null &&
    normalizeProfileKey(authority.runtimeScope) === normalizeProfileKey(authority.currentScope)

  const currentRuntimes = runtimeIsCurrent ? authority.runtimes : {}

  const legacySessions = new Set(
    legacyArtifactSessions(
      authority.sessions,
      currentRuntimes,
      authority.projects,
      authority.currentScope,
      [],
      authority.projectsScope,
      authority.projectsGeneration,
      authority.projectsContextGeneration
    ).map(session => artifactSessionIdentity(session.profile, session.id))
  )

  const currentCanonicalArtifacts = runtimeIsCurrent
    ? collectArtifactsForProjectRuntimes(currentRuntimes, authority.sessions, authority.currentScope)
    : []

  const validLegacyArtifacts = artifacts.filter(artifact => {
    const identity = artifactSessionIdentity(artifact.profile, artifact.sessionId)

    return artifact.source === 'legacy' && sessionsByIdentity.has(identity) && legacySessions.has(identity)
  })

  const deduplicated = new Map(
    [...validLegacyArtifacts, ...currentCanonicalArtifacts].map(artifact => [artifact.id, artifact])
  )

  return [...deduplicated.values()].sort(
    (left, right) => right.timestamp - left.timestamp || left.id.localeCompare(right.id)
  )
}
