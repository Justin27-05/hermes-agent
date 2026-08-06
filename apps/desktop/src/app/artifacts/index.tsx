import { useStore } from '@nanostores/react'
import type * as React from 'react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'

import { ZoomableImage } from '@/components/chat/zoomable-image'
import { PageLoader } from '@/components/page-loader'
import { Button } from '@/components/ui/button'
import { CopyButton } from '@/components/ui/copy-button'
import {
  Pagination,
  PaginationButton,
  PaginationContent,
  PaginationEllipsis,
  PaginationItem,
  PaginationNext,
  PaginationPrevious
} from '@/components/ui/pagination'
import { RowButton } from '@/components/ui/row-button'
import { Tip } from '@/components/ui/tooltip'
import { getSessionMessages, listAllProfileSessions } from '@/hermes'
import { type Translations, useI18n } from '@/i18n'
import { ExternalLink, ExternalLinkIcon, hostPathLabel, urlSlugTitleLabel, useLinkTitle } from '@/lib/external-link'
import { FileImage, FileText, FolderOpen, Link2, Loader2, RefreshCw } from '@/lib/icons'
import { downloadGatewayMediaFile, isRemoteGateway } from '@/lib/media'
import { normalize } from '@/lib/text'
import { fmtDayTime } from '@/lib/time'
import { cn } from '@/lib/utils'
import { notifyError } from '@/store/notifications'
import {
  $activeGatewayProfile,
  ensureGatewayProfile,
  gatewayProfileAuthorityGeneration,
  gatewayProfileAuthorityTarget,
  normalizeProfileKey
} from '@/store/profile'
import { $projectRuntimes, projectRuntimeScope } from '@/store/project-runtime'
import { $projectCatalogAuthority, $projects } from '@/store/projects'
import type { SessionInfo } from '@/types/hermes'

import { useRefreshHotkey } from '../hooks/use-refresh-hotkey'
import { useRouteEnumParam } from '../hooks/use-route-enum-param'
import { PageSearchShell } from '../page-search-shell'
import { sessionRoute } from '../routes'
import type { SetStatusbarItemGroup } from '../shell/statusbar-controls'

import {
  ARTIFACT_FILTERS,
  type ArtifactFilter,
  artifactImageSrc,
  type ArtifactRecord,
  artifactSessionIdentity,
  artifactsForCurrentAuthority,
  collectArtifactsForProjectRuntimes,
  collectArtifactsForSession,
  legacyArtifactSessions
} from './artifact-utils'

function formatArtifactTime(timestamp: number): string {
  return fmtDayTime.format(new Date(timestamp))
}

function pageRangeLabel(total: number, page: number, pageSize: number, a: Translations['artifacts']): string {
  if (total === 0) {
    return a.zero
  }

  const start = (page - 1) * pageSize + 1
  const end = Math.min(total, page * pageSize)

  return a.rangeOf(start, end, total)
}

function paginationItems(page: number, pageCount: number): Array<number | 'ellipsis'> {
  if (pageCount <= 7) {
    return Array.from({ length: pageCount }, (_, index) => index + 1)
  }

  const pages: Array<number | 'ellipsis'> = [1]
  const start = Math.max(2, page - 1)
  const end = Math.min(pageCount - 1, page + 1)

  if (start > 2) {
    pages.push('ellipsis')
  }

  for (let nextPage = start; nextPage <= end; nextPage += 1) {
    pages.push(nextPage)
  }

  if (end < pageCount - 1) {
    pages.push('ellipsis')
  }

  pages.push(pageCount)

  return pages
}

type CellCtx = {
  onOpen: (href: string) => void | Promise<void>
  onOpenChat: (artifact: ArtifactRecord) => void | Promise<void>
}

interface ArtifactColumn {
  Cell: (props: { artifact: ArtifactRecord; ctx: CellCtx }) => React.ReactElement
  bodyClassName: string
  header: (filter: ArtifactFilter, a: Translations['artifacts']) => string
  id: 'location' | 'primary' | 'session'
  width: (filter: ArtifactFilter) => string
}

const itemsLabel = (f: ArtifactFilter, a: Translations['artifacts']) =>
  f === 'link' ? a.itemsLink : f === 'file' ? a.itemsFile : a.itemsGeneric

interface ArtifactsViewProps extends React.ComponentProps<'section'> {
  setStatusbarItemGroup?: SetStatusbarItemGroup
}

export function ArtifactsView({ setStatusbarItemGroup: _setStatusbarItemGroup, ...props }: ArtifactsViewProps) {
  const { t } = useI18n()
  const a = t.artifacts
  const navigate = useNavigate()
  const activeProfile = normalizeProfileKey(useStore($activeGatewayProfile))
  const projectRuntimes = useStore($projectRuntimes)
  const projectsAuthority = useStore($projectCatalogAuthority)
  const projects = useStore($projects)
  const [artifacts, setArtifacts] = useState<ArtifactRecord[] | null>(null)
  const [artifactSessions, setArtifactSessions] = useState<SessionInfo[]>([])
  const [loadedScope, setLoadedScope] = useState<null | string>(null)
  const [query, setQuery] = useState('')

  const [kindFilter, setKindFilter] = useRouteEnumParam('tab', ARTIFACT_FILTERS, 'all')

  const [failedImageIds, setFailedImageIds] = useState<Set<string>>(() => new Set())
  const [imagePage, setImagePage] = useState(1)
  const [filePage, setFilePage] = useState(1)

  const [refreshing, setRefreshing] = useState(false)
  const refreshGeneration = useRef(0)
  const openChatGeneration = useRef(0)

  const refreshArtifacts = useCallback(async () => {
    const generation = ++refreshGeneration.current
    const requestedScope = activeProfile
    const projectsAuthorityAtRequest = projectsAuthority

    const projectsAtRequest =
      projectsAuthorityAtRequest.profile !== null &&
      normalizeProfileKey(projectsAuthorityAtRequest.profile) === requestedScope &&
      projectsAuthorityAtRequest.catalogGeneration !== null &&
      projectsAuthorityAtRequest.catalogGeneration === projectsAuthorityAtRequest.contextGeneration
        ? projects
        : []

    const runtimesAtRequest = normalizeProfileKey(projectRuntimeScope()) === requestedScope ? projectRuntimes : {}

    setRefreshing(true)

    try {
      const sessions = (await listAllProfileSessions(30, 1)).sessions

      const legacySessions = legacyArtifactSessions(
        sessions,
        runtimesAtRequest,
        projectsAtRequest,
        requestedScope,
        [],
        projectsAuthorityAtRequest.profile,
        projectsAuthorityAtRequest.catalogGeneration,
        projectsAuthorityAtRequest.contextGeneration
      )

      const results = await Promise.allSettled(
        legacySessions.map(session => getSessionMessages(session.id, session.profile))
      )

      if (
        generation !== refreshGeneration.current ||
        requestedScope !== normalizeProfileKey($activeGatewayProfile.get())
      ) {
        return
      }

      const currentRuntimeScope = projectRuntimeScope()

      const currentRuntimes =
        currentRuntimeScope !== null && normalizeProfileKey(currentRuntimeScope) === requestedScope
          ? $projectRuntimes.get()
          : {}

      const currentProjects = $projects.get()
      const currentProjectsAuthority = $projectCatalogAuthority.get()

      const currentLegacySessionIdentities = new Set(
        legacyArtifactSessions(
          sessions,
          currentRuntimes,
          currentProjects,
          requestedScope,
          [],
          currentProjectsAuthority.profile,
          currentProjectsAuthority.catalogGeneration,
          currentProjectsAuthority.contextGeneration
        ).map(session => artifactSessionIdentity(session.profile, session.id))
      )

      const nextArtifacts =
        currentRuntimeScope !== null && normalizeProfileKey(currentRuntimeScope) === requestedScope
          ? collectArtifactsForProjectRuntimes(currentRuntimes, sessions, requestedScope)
          : []

      results.forEach((result, index) => {
        const session = legacySessions[index]

        if (
          result.status !== 'fulfilled' ||
          !currentLegacySessionIdentities.has(artifactSessionIdentity(session.profile, session.id))
        ) {
          return
        }

        nextArtifacts.push(...collectArtifactsForSession(session, result.value.messages))
      })

      setArtifactSessions(sessions)
      setLoadedScope(requestedScope)
      setArtifacts(nextArtifacts.sort((left, right) => right.timestamp - left.timestamp))
    } catch (err) {
      if (
        generation === refreshGeneration.current &&
        requestedScope === normalizeProfileKey($activeGatewayProfile.get())
      ) {
        notifyError(err, a.failedLoad)
        setArtifactSessions([])
        setLoadedScope(requestedScope)
        setArtifacts([])
      }
    } finally {
      if (generation === refreshGeneration.current) {
        setRefreshing(false)
      }
    }
  }, [a, activeProfile, projectRuntimes, projects, projectsAuthority])

  useRefreshHotkey(refreshArtifacts)

  useEffect(() => {
    void refreshArtifacts()
  }, [refreshArtifacts])

  const authoritativeArtifacts = useMemo(() => {
    if (artifacts === null) {
      return null
    }

    return artifactsForCurrentAuthority(artifacts, {
      currentScope: activeProfile,
      loadedScope,
      projects,
      projectsContextGeneration: projectsAuthority.contextGeneration,
      projectsGeneration: projectsAuthority.catalogGeneration,
      projectsScope: projectsAuthority.profile,
      runtimeScope: projectRuntimeScope(),
      runtimes: projectRuntimes,
      sessions: artifactSessions
    })
  }, [activeProfile, artifactSessions, artifacts, loadedScope, projectRuntimes, projects, projectsAuthority])

  useEffect(() => {
    const isCurrent =
      artifacts?.length === authoritativeArtifacts?.length &&
      artifacts?.every((artifact, index) => {
        const authoritative = authoritativeArtifacts?.[index]

        return (
          authoritative !== undefined &&
          artifact.href === authoritative.href &&
          artifact.id === authoritative.id &&
          artifact.kind === authoritative.kind &&
          artifact.label === authoritative.label &&
          artifact.profile === authoritative.profile &&
          artifact.projectId === authoritative.projectId &&
          artifact.sessionId === authoritative.sessionId &&
          artifact.sessionTitle === authoritative.sessionTitle &&
          artifact.sha256 === authoritative.sha256 &&
          artifact.sizeBytes === authoritative.sizeBytes &&
          artifact.source === authoritative.source &&
          artifact.timestamp === authoritative.timestamp &&
          artifact.value === authoritative.value
        )
      })

    if (artifacts && authoritativeArtifacts && !isCurrent) {
      setArtifacts(authoritativeArtifacts)
    }
  }, [artifacts, authoritativeArtifacts])

  useEffect(() => {
    setImagePage(1)
    setFilePage(1)
  }, [authoritativeArtifacts, kindFilter, query])

  const visibleArtifacts = useMemo(() => {
    if (!authoritativeArtifacts) {
      return []
    }

    const q = normalize(query)

    return authoritativeArtifacts.filter(artifact => {
      if (kindFilter !== 'all' && artifact.kind !== kindFilter) {
        return false
      }

      if (!q) {
        return true
      }

      return (
        artifact.label.toLowerCase().includes(q) ||
        artifact.value.toLowerCase().includes(q) ||
        artifact.sessionTitle.toLowerCase().includes(q)
      )
    })
  }, [authoritativeArtifacts, kindFilter, query])

  const visibleImageArtifacts = useMemo(
    () => visibleArtifacts.filter(artifact => artifact.kind === 'image'),
    [visibleArtifacts]
  )

  const visibleFileArtifacts = useMemo(
    () => visibleArtifacts.filter(artifact => artifact.kind !== 'image'),
    [visibleArtifacts]
  )

  const imagePageCount = Math.max(1, Math.ceil(visibleImageArtifacts.length / 24))
  const filePageCount = Math.max(1, Math.ceil(visibleFileArtifacts.length / 100))
  const currentImagePage = Math.min(imagePage, imagePageCount)
  const currentFilePage = Math.min(filePage, filePageCount)

  const pagedImageArtifacts = useMemo(
    () => visibleImageArtifacts.slice((currentImagePage - 1) * 24, currentImagePage * 24),
    [currentImagePage, visibleImageArtifacts]
  )

  const pagedFileArtifacts = useMemo(
    () => visibleFileArtifacts.slice((currentFilePage - 1) * 100, currentFilePage * 100),
    [currentFilePage, visibleFileArtifacts]
  )

  // Rotating placeholder nudges from real data — search matches file paths and
  // session titles, not just labels; show it.
  const searchHints = useMemo(() => {
    if (!authoritativeArtifacts?.length) {
      return undefined
    }

    const extensions = [
      ...new Set(
        authoritativeArtifacts.map(artifact => /\.(\w{2,4})$/.exec(artifact.value)?.[1]?.toLowerCase()).filter(Boolean)
      )
    ].slice(0, 3) as string[]

    const titles = [...new Set(authoritativeArtifacts.map(artifact => artifact.sessionTitle).filter(Boolean))].slice(
      0,
      2
    )

    const hints = [
      ...extensions.map(ext => t.common.tryHint(`.${ext}`)),
      ...titles.map(title => t.common.tryHint(title))
    ]

    return hints.length > 0 ? hints : undefined
  }, [authoritativeArtifacts, t])

  const counts = useMemo(() => {
    const all = authoritativeArtifacts || []

    return {
      all: all.length,
      image: all.filter(artifact => artifact.kind === 'image').length,
      file: all.filter(artifact => artifact.kind === 'file').length,
      link: all.filter(artifact => artifact.kind === 'link').length
    }
  }, [authoritativeArtifacts])

  const openArtifact = useCallback(
    async (href: string) => {
      try {
        // A gateway-local file resolves to file:// in remote mode (the file
        // lives on the gateway, not this disk). Opening that locally fails —
        // and an OAuth remote connection has no query token to build a download
        // URL. Fetch the bytes over the authenticated fs bridge instead.
        if (isRemoteGateway() && /^file:/i.test(href)) {
          await downloadGatewayMediaFile(href)

          return
        }

        if (window.hermesDesktop?.openExternal) {
          await window.hermesDesktop.openExternal(href)
        } else {
          window.open(href, '_blank', 'noopener,noreferrer')
        }
      } catch (err) {
        notifyError(err, a.openFailed)
      }
    },
    [a]
  )

  const markImageFailed = useCallback((id: string) => {
    setFailedImageIds(current => {
      if (current.has(id)) {
        return current
      }

      return new Set(current).add(id)
    })
  }, [])

  const openArtifactChat = useCallback(
    async (artifact: ArtifactRecord) => {
      const generation = ++openChatGeneration.current
      const activation = ensureGatewayProfile(artifact.profile)
      const profileGeneration = gatewayProfileAuthorityGeneration()

      try {
        await activation

        if (
          generation !== openChatGeneration.current ||
          profileGeneration !== gatewayProfileAuthorityGeneration() ||
          normalizeProfileKey($activeGatewayProfile.get()) !== normalizeProfileKey(artifact.profile)
        ) {
          return
        }

        navigate(sessionRoute(artifact.sessionId))
      } catch (err) {
        if (
          generation === openChatGeneration.current &&
          profileGeneration === gatewayProfileAuthorityGeneration() &&
          normalizeProfileKey(artifact.profile) === gatewayProfileAuthorityTarget()
        ) {
          notifyError(err, a.openFailed)
        }
      }
    },
    [a.openFailed, navigate]
  )

  useEffect(
    () => () => {
      openChatGeneration.current += 1
    },
    []
  )

  const cellCtx: CellCtx = {
    onOpen: openArtifact,
    onOpenChat: openArtifactChat
  }

  return (
    <PageSearchShell
      {...props}
      activeTab={kindFilter}
      onSearchChange={setQuery}
      onTabChange={id => setKindFilter(id as typeof kindFilter)}
      searchHidden={counts.all === 0}
      searchHints={searchHints}
      searchPlaceholder={a.search}
      searchTrailingAction={
        <Tip label={refreshing ? a.refreshing : a.refresh}>
          <Button
            aria-label={refreshing ? a.refreshing : a.refresh}
            className="text-(--ui-text-tertiary) hover:bg-(--chrome-action-hover) hover:text-foreground"
            disabled={refreshing}
            onClick={() => void refreshArtifacts()}
            size="icon-titlebar"
            variant="ghost"
          >
            {refreshing ? <Loader2 className="animate-spin" /> : <RefreshCw />}
          </Button>
        </Tip>
      }
      searchValue={query}
      tabs={[
        { id: 'all', label: a.tabAll, meta: authoritativeArtifacts ? counts.all : null },
        { id: 'image', label: a.tabImages, meta: authoritativeArtifacts ? counts.image : null },
        { id: 'file', label: a.tabFiles, meta: authoritativeArtifacts ? counts.file : null },
        { id: 'link', label: a.tabLinks, meta: authoritativeArtifacts ? counts.link : null }
      ]}
    >
      {!authoritativeArtifacts ? (
        <PageLoader label={a.indexing} />
      ) : visibleArtifacts.length === 0 ? (
        <div className="grid h-full place-items-center px-6 text-center">
          <div>
            <div className="text-sm font-medium">{a.noArtifactsTitle}</div>
            <div className="mt-1 text-xs text-muted-foreground">{a.noArtifactsDesc}</div>
          </div>
        </div>
      ) : (
        <div className="h-full overflow-y-auto [scrollbar-gutter:stable]">
          <div className="flex flex-col gap-3 px-3 pb-2">
            {visibleImageArtifacts.length > 0 && (
              <section className="flex flex-col">
                <div className="sticky top-0 z-10 -mx-3 flex h-7 items-center gap-3 overflow-x-auto bg-background px-3">
                  <ArtifactsPagination
                    className="ml-auto justify-end px-0"
                    itemLabel={a.itemsImage}
                    onPageChange={setImagePage}
                    page={currentImagePage}
                    pageSize={24}
                    total={visibleImageArtifacts.length}
                  />
                </div>
                <div className="grid grid-cols-[repeat(auto-fill,minmax(11rem,1fr))] items-start gap-2 pt-1.5">
                  {pagedImageArtifacts.map(artifact => (
                    <ArtifactImageCard
                      artifact={artifact}
                      failedImage={failedImageIds.has(artifact.id)}
                      key={artifact.id}
                      onImageError={markImageFailed}
                      onOpenChat={openArtifactChat}
                    />
                  ))}
                </div>
              </section>
            )}

            {visibleFileArtifacts.length > 0 && (
              <section className="flex flex-col">
                <div className="sticky top-0 z-10 -mx-3 flex h-7 items-center gap-3 overflow-x-auto bg-background px-3">
                  <ArtifactsPagination
                    className="ml-auto justify-end px-0"
                    itemLabel={itemsLabel(kindFilter, a)}
                    onPageChange={setFilePage}
                    page={currentFilePage}
                    pageSize={100}
                    total={visibleFileArtifacts.length}
                  />
                </div>
                <div className="overflow-x-auto rounded-lg border border-(--ui-stroke-tertiary) bg-(--ui-chat-bubble-background)">
                  <ArtifactTable artifacts={pagedFileArtifacts} ctx={cellCtx} filter={kindFilter} />
                </div>
              </section>
            )}
          </div>
        </div>
      )}
    </PageSearchShell>
  )
}

interface ArtifactsPaginationProps {
  className?: string
  itemLabel: string
  onPageChange: (page: number) => void
  page: number
  pageSize: number
  total: number
}

function ArtifactsPagination({ className, itemLabel, onPageChange, page, pageSize, total }: ArtifactsPaginationProps) {
  const { t } = useI18n()
  const a = t.artifacts
  const pageCount = Math.max(1, Math.ceil(total / pageSize))

  return (
    <div className={cn('flex h-6 items-center justify-between gap-2 px-1', className)}>
      <div className="shrink-0 text-[0.62rem] text-muted-foreground">
        {pageRangeLabel(total, page, pageSize, a)} {itemLabel}
      </div>
      {pageCount > 1 && (
        <Pagination className="mx-0 w-auto min-w-0 justify-end">
          <PaginationContent className="gap-0.5">
            <PaginationItem>
              <PaginationPrevious disabled={page <= 1} onClick={() => onPageChange(Math.max(1, page - 1))} />
            </PaginationItem>
            {paginationItems(page, pageCount).map((item, index) => (
              <PaginationItem key={`${item}-${index}`}>
                {item === 'ellipsis' ? (
                  <PaginationEllipsis />
                ) : (
                  <PaginationButton
                    aria-label={a.goToPage(itemLabel, item)}
                    isActive={page === item}
                    onClick={() => onPageChange(item)}
                  >
                    {item}
                  </PaginationButton>
                )}
              </PaginationItem>
            ))}
            <PaginationItem>
              <PaginationNext
                disabled={page >= pageCount}
                onClick={() => onPageChange(Math.min(pageCount, page + 1))}
              />
            </PaginationItem>
          </PaginationContent>
        </Pagination>
      )}
    </div>
  )
}

interface ArtifactImageCardProps {
  artifact: ArtifactRecord
  failedImage: boolean
  onImageError: (id: string) => void
  onOpenChat: (artifact: ArtifactRecord) => void | Promise<void>
}

function ArtifactImageCard({ artifact, failedImage, onImageError, onOpenChat }: ArtifactImageCardProps) {
  const { t } = useI18n()
  const a = t.artifacts
  const kindLabel = artifact.kind === 'image' ? a.kindImage : artifact.kind === 'file' ? a.kindFile : a.kindLink
  const [src, setSrc] = useState('')

  useEffect(() => {
    let active = true

    setSrc('')

    if (!artifact.href) {
      return () => {
        active = false
      }
    }

    void artifactImageSrc(artifact.value, artifact.href)
      .then(nextSrc => {
        if (active) {
          setSrc(nextSrc)
        }
      })
      .catch(() => {
        if (active) {
          onImageError(artifact.id)
        }
      })

    return () => {
      active = false
    }
  }, [artifact.href, artifact.id, artifact.value, onImageError])

  return (
    <article className="group/artifact overflow-hidden rounded-lg border border-(--ui-stroke-tertiary) bg-(--ui-chat-bubble-background)">
      <div
        className={cn(
          'relative flex h-40 w-full items-center justify-center overflow-hidden border-b border-(--ui-stroke-tertiary) bg-(--ui-bg-quinary) p-1.5',
          failedImage && 'cursor-default'
        )}
      >
        {!failedImage && src && (
          <ZoomableImage
            alt={artifact.label}
            className="max-h-40 max-w-full cursor-zoom-in rounded-md object-contain"
            containerClassName="max-h-full"
            decoding="async"
            loading="lazy"
            onError={() => onImageError(artifact.id)}
            slot="artifact-media"
            src={src}
          />
        )}
      </div>

      <div className="space-y-1.5 p-2">
        <div className="min-w-0">
          <div className="mb-0.5 flex items-center gap-1 text-[0.625rem] uppercase tracking-[0.08em] text-(--ui-text-tertiary)">
            <FileImage className="size-3" />
            {kindLabel}
          </div>
          <div className="truncate text-[length:var(--conversation-caption-font-size)] font-medium">
            {artifact.label}
          </div>
          {artifact.value && (
            <div className="mt-0.5 truncate text-[0.625rem] text-(--ui-text-tertiary)">{artifact.value}</div>
          )}
        </div>

        {artifact.source === 'canonical' && (
          <ArtifactPresentationDetails artifact={artifact} className="text-[0.625rem]" />
        )}

        <div className="truncate text-[0.625rem] text-(--ui-text-tertiary)">
          {artifact.sessionTitle} · {formatArtifactTime(artifact.timestamp)}
        </div>

        <div className="flex flex-wrap gap-1.5">
          <Button onClick={() => void onOpenChat(artifact)} size="xs" type="button" variant="textStrong">
            <FolderOpen className="size-3" />
            {a.chat}
          </Button>
        </div>
      </div>
    </article>
  )
}

// Single click target for any row cell. External URLs render as <ExternalLink>;
// local actions render as <button>. Padding lives here, NOT on the <td>, so
// the entire cell area is hoverable and clickable in both branches.
function ArtifactCellAction({
  children,
  disabled = false,
  href,
  onClick,
  title
}: {
  children: React.ReactNode
  disabled?: boolean
  href?: string
  onClick?: () => void
  title?: string
}) {
  if (href && !disabled) {
    return (
      <ExternalLink
        className="flex h-full w-full min-w-0 items-center gap-2 px-2.5 py-1.5 text-left text-[length:var(--conversation-caption-font-size)] leading-(--conversation-caption-line-height) font-normal text-(--ui-text-secondary) no-underline underline-offset-4 decoration-current/20 transition-colors hover:text-foreground hover:underline"
        href={href}
        showExternalIcon={false}
        title={title}
      >
        {children}
      </ExternalLink>
    )
  }

  return (
    <RowButton
      className="flex h-full w-full min-w-0 items-center gap-2 px-2.5 py-1.5 text-left text-[length:var(--conversation-caption-font-size)] leading-(--conversation-caption-line-height) font-normal text-(--ui-text-secondary) no-underline underline-offset-4 decoration-current/20 transition-colors hover:text-foreground hover:underline"
      disabled={disabled}
      onClick={onClick}
    >
      {children}
    </RowButton>
  )
}

function PrimaryCell({ artifact, ctx }: { artifact: ArtifactRecord; ctx: CellCtx }) {
  const href = artifact.href
  const linkHref = artifact.kind === 'link' ? href : null
  const isLink = linkHref !== null
  const Icon = isLink ? Link2 : FileText
  const fetchedTitle = useLinkTitle(linkHref)
  const label = linkHref ? fetchedTitle || urlSlugTitleLabel(linkHref) : artifact.label
  const canOpen = artifact.source === 'legacy' || href !== null

  return (
    <ArtifactCellAction
      disabled={!canOpen}
      href={linkHref ?? undefined}
      onClick={linkHref || !href ? undefined : () => void ctx.onOpen(href)}
      title={label}
    >
      <span className="mt-0.5 grid size-6 shrink-0 place-items-center self-start rounded-md bg-(--ui-bg-tertiary) text-(--ui-text-tertiary)">
        <Icon className="size-3.5" />
      </span>
      <span className={cn('min-w-0 flex-1', isLink ? 'wrap-anywhere' : 'truncate')}>
        {label}
        {isLink && <ExternalLinkIcon />}
      </span>
    </ArtifactCellAction>
  )
}

function LocationCell({ artifact }: { artifact: ArtifactRecord; ctx: CellCtx }) {
  const { t } = useI18n()
  const isLink = artifact.kind === 'link' && artifact.href !== null
  const value = isLink ? hostPathLabel(artifact.value) : artifact.value
  const copyLabel = isLink ? t.artifacts.copyUrl : t.artifacts.copyPath

  if (!value) {
    return (
      <div className="min-w-0 text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)">
        â€”
        <ArtifactPresentationDetails artifact={artifact} className="mt-0.5" />
      </div>
    )
  }

  return (
    <div className="min-w-0">
      <div className="group/location flex min-w-0 items-center gap-1.5">
        <Tip label={artifact.value}>
          <div
            className={cn(
              'min-w-0 flex-1 truncate text-[length:var(--conversation-caption-font-size)] text-(--ui-text-tertiary)',
              isLink ? 'font-normal' : 'font-mono'
            )}
          >
            {value}
          </div>
        </Tip>
        <CopyButton
          appearance="icon"
          buttonSize="icon-xs"
          className="shrink-0 text-muted-foreground opacity-0 transition-opacity hover:text-foreground focus-visible:opacity-100 group-hover/location:opacity-100"
          iconClassName="size-3.5"
          label={copyLabel}
          text={artifact.value}
          title={copyLabel}
        />
      </div>
      <ArtifactPresentationDetails artifact={artifact} className="mt-0.5" />
    </div>
  )
}

function ArtifactPresentationDetails({ artifact, className }: { artifact: ArtifactRecord; className?: string }) {
  if (artifact.source !== 'canonical') {
    return null
  }

  return (
    <div className={cn('min-w-0 text-(--ui-text-tertiary)', className)}>
      <div>{artifact.sizeBytes === null ? 'Size: â€”' : `Size: ${artifact.sizeBytes} bytes`}</div>
      <div className="wrap-anywhere" title={artifact.sha256 || undefined}>
        {artifact.sha256 ? `SHA-256: ${artifact.sha256}` : 'SHA-256: â€”'}
      </div>
    </div>
  )
}

function SessionCell({ artifact, ctx }: { artifact: ArtifactRecord; ctx: CellCtx }) {
  return (
    <ArtifactCellAction onClick={() => void ctx.onOpenChat(artifact)} title={artifact.sessionTitle}>
      <span className="flex min-w-0 flex-col">
        <span className="truncate">{artifact.sessionTitle}</span>
        <span className="truncate text-[0.6875rem] font-normal text-(--ui-text-tertiary)">
          {formatArtifactTime(artifact.timestamp)}
        </span>
      </span>
    </ArtifactCellAction>
  )
}

const ARTIFACT_COLUMNS: readonly ArtifactColumn[] = [
  {
    Cell: PrimaryCell,
    bodyClassName: 'p-0',
    header: (filter, a) =>
      filter === 'link' ? a.colTitleLink : filter === 'file' ? a.colTitleFile : a.colTitleDefault,
    id: 'primary',
    width: filter => (filter === 'link' ? 'w-[50%]' : 'w-[35%]')
  },
  {
    Cell: LocationCell,
    bodyClassName: 'px-2.5 py-1.5',
    header: (filter, a) =>
      filter === 'link' ? a.colLocationLink : filter === 'file' ? a.colLocationFile : a.colLocationDefault,
    id: 'location',
    width: filter => (filter === 'link' ? 'w-[30%]' : 'w-[41%]')
  },
  {
    Cell: SessionCell,
    bodyClassName: 'p-0',
    header: (_filter, a) => a.colSession,
    id: 'session',
    width: filter => (filter === 'link' ? 'w-[20%]' : 'w-[24%]')
  }
]

function ArtifactTable({
  artifacts,
  ctx,
  filter
}: {
  artifacts: readonly ArtifactRecord[]
  ctx: CellCtx
  filter: ArtifactFilter
}) {
  const { t } = useI18n()

  return (
    <table className="w-full min-w-176 table-fixed text-left text-[length:var(--conversation-caption-font-size)]">
      <thead className="border-b border-(--ui-stroke-tertiary) bg-(--ui-bg-quinary) text-[0.625rem] uppercase tracking-[0.08em] text-(--ui-text-tertiary)">
        <tr>
          {ARTIFACT_COLUMNS.map(col => (
            <th className={cn(col.width(filter), 'px-2.5 py-1.5 font-medium')} key={col.id}>
              {col.header(filter, t.artifacts)}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {artifacts.map(artifact => (
          <tr className="group/artifact" key={artifact.id}>
            {ARTIFACT_COLUMNS.map(col => {
              const Cell = col.Cell

              return (
                <td className={cn('align-middle', col.bodyClassName)} key={col.id}>
                  <Cell artifact={artifact} ctx={ctx} />
                </td>
              )
            })}
          </tr>
        ))}
      </tbody>
    </table>
  )
}
