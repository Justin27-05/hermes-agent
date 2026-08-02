const CREDENTIAL_URL_KEY_FRAGMENTS = [
  'accesskey',
  'apikey',
  'auth',
  'authorization',
  'credential',
  'password',
  'privatekey',
  'secret',
  'signature',
  'token'
]

const MAX_URL_COMPONENT_LENGTH = 4096
const MAX_PERCENT_DECODE_PASSES = 4

const isCredentialLikeKey = (key: string): boolean =>
  CREDENTIAL_URL_KEY_FRAGMENTS.some(fragment =>
    key
      .toLowerCase()
      .replaceAll(/[^a-z0-9]/g, '')
      .includes(fragment)
  )

const hasForbiddenUrlCharacter = (value: string): boolean => /[\s\p{C}]/u.test(value)

function decodeUrlComponent(value: string): string | undefined {
  if (value.length > MAX_URL_COMPONENT_LENGTH || hasForbiddenUrlCharacter(value)) {
    return undefined
  }

  let decoded = value

  for (let pass = 0; pass < MAX_PERCENT_DECODE_PASSES; pass += 1) {
    if (!decoded.includes('%')) {
      return decoded
    }

    try {
      decoded = decodeURIComponent(decoded)
    } catch {
      return undefined
    }

    if (decoded.length > MAX_URL_COMPONENT_LENGTH || hasForbiddenUrlCharacter(decoded)) {
      return undefined
    }
  }

  return decoded.includes('%') ? undefined : decoded
}

const hasCredentialLikeKey = (params: URLSearchParams): boolean => {
  let found = false
  params.forEach((_value, key) => {
    const decodedKey = decodeUrlComponent(key)
    found ||= decodedKey === undefined || isCredentialLikeKey(decodedKey)
  })

  return found
}

function hasCredentialLikeParameters(value: string): boolean {
  const decoded = decodeUrlComponent(value)

  return decoded === undefined || hasCredentialLikeKey(new URLSearchParams(decoded))
}

const IPV4_NON_GLOBAL_RANGES = [
  [0x00000000, 8],
  [0x0a000000, 8],
  [0x64400000, 10],
  [0x7f000000, 8],
  [0xa9fe0000, 16],
  [0xac100000, 12],
  [0xc0000000, 24],
  [0xc0000200, 24],
  [0xc0a80000, 16],
  [0xc6120000, 15],
  [0xc6336400, 24],
  [0xcb007100, 24],
  [0xe0000000, 4],
  [0xf0000000, 4]
] as const

function isIpv4InCidr(address: number, base: number, prefix: number): boolean {
  const mask = (0xffffffff << (32 - prefix)) >>> 0

  return (address & mask) >>> 0 === base
}

function isNonExternalIpv4(host: string): boolean {
  const octets = host.split('.').map(part => Number(part))

  if (octets.length !== 4 || octets.some(octet => !Number.isInteger(octet) || octet < 0 || octet > 255)) {
    return false
  }

  const address = octets.reduce((value, octet) => (value * 256 + octet) >>> 0, 0)

  if (address === 0xc0000009 || address === 0xc000000a) {
    return false
  }

  return IPV4_NON_GLOBAL_RANGES.some(([base, prefix]) => isIpv4InCidr(address, base, prefix))
}

function expandIpv6(host: string): number[] | undefined {
  const chunks = host.split('::')

  if (chunks.length > 2) {
    return undefined
  }

  const parse = (side: string): number[] | undefined => {
    if (!side) {
      return []
    }

    const parts = side.split(':')

    if (parts.some(part => !/^[a-f0-9]{1,4}$/.test(part))) {
      return undefined
    }

    return parts.map(part => Number.parseInt(part, 16))
  }

  const left = parse(chunks[0])
  const right = parse(chunks[1] ?? '')

  if (!left || !right) {
    return undefined
  }

  if (chunks.length === 1) {
    return left.length === 8 ? left : undefined
  }

  const zeros = 8 - left.length - right.length

  return zeros >= 1 ? [...left, ...Array<number>(zeros).fill(0), ...right] : undefined
}

function isNonExternalIpv6(host: string): boolean {
  const groups = expandIpv6(host)

  if (!groups) {
    return false
  }

  const isIpv4Mapped = groups.slice(0, 5).every(group => group === 0) && groups[5] === 0xffff

  if (isIpv4Mapped) {
    return isNonExternalIpv4(`${groups[6] >> 8}.${groups[6] & 0xff}.${groups[7] >> 8}.${groups[7] & 0xff}`)
  }

  const is2001Exception =
    (groups[0] === 0x2001 &&
      groups[1] === 1 &&
      groups.slice(2, 7).every(group => group === 0) &&
      (groups[7] === 1 || groups[7] === 2)) ||
    (groups[0] === 0x2001 && groups[1] === 3) ||
    (groups[0] === 0x2001 && groups[1] === 4 && groups[2] === 0x0112) ||
    (groups[0] === 0x2001 && ((groups[1] & 0xfff0) === 0x0020 || (groups[1] & 0xfff0) === 0x0030))

  if (is2001Exception) {
    return false
  }

  return (
    groups.every(group => group === 0) ||
    (groups.slice(0, 7).every(group => group === 0) && groups[7] === 1) ||
    (groups[0] === 0x0064 && groups[1] === 0xff9b && groups[2] === 1) ||
    (groups[0] === 0x0100 && groups.slice(1, 4).every(group => group === 0)) ||
    (groups[0] === 0x2001 && groups[1] < 0x0200) ||
    (groups[0] === 0x2001 && groups[1] === 0x0db8) ||
    groups[0] === 0x2002 ||
    (groups[0] === 0x3fff && (groups[1] & 0xf000) === 0) ||
    (groups[0] & 0xfe00) === 0xfc00 ||
    (groups[0] & 0xffc0) === 0xfe80 ||
    (groups[0] & 0xffc0) === 0xfec0 ||
    (groups[0] & 0xff00) === 0xff00
  )
}

function isNonExternalHost(hostname: string): boolean {
  const host = hostname
    .toLowerCase()
    .replace(/^\[|\]$/g, '')
    .replace(/\.+$/, '')

  const isLocalName = ['localhost', 'local', 'home.arpa'].some(suffix => host === suffix || host.endsWith(`.${suffix}`))

  return isLocalName || isNonExternalIpv4(host) || (host.includes(':') && isNonExternalIpv6(host))
}

/** Validates the safe public URL projection, never a local gateway path. */
export function isCredentialFreeHttpUrl(value: string): boolean {
  if (!/^https?:\/\//i.test(value) || hasForbiddenUrlCharacter(value)) {
    return false
  }

  try {
    const target = new URL(value)
    const fragment = decodeUrlComponent(target.hash.slice(1))

    return (
      (target.protocol === 'http:' || target.protocol === 'https:') &&
      !!target.hostname &&
      !isNonExternalHost(target.hostname) &&
      !target.username &&
      !target.password &&
      !hasCredentialLikeParameters(target.search.slice(1)) &&
      fragment !== undefined &&
      (!fragment.includes('=') || !hasCredentialLikeParameters(fragment))
    )
  } catch {
    return false
  }
}
