/**
 * End-to-end-ish dispatcher tests: mock `@actions/core`, mock global
 * `fetch`, drive the action through every operation, assert outputs and
 * failure modes.
 *
 * The mock surface is intentionally small. Anything we forget to stub
 * here is an honest test gap, not a hidden dependency.
 */
import { jest } from '@jest/globals'
import * as core from '../__fixtures__/core.js'
import { resetInputs, setInput } from '../__fixtures__/core.js'

// A fresh `jest.fn` per test eliminates the `mockResolvedValueOnce` queue
// leaks we saw when reusing a single mock across tests. `mocks` is
// reassigned in `beforeEach`; closures captured here see the latest
// value because `mocks` is a `let` binding.
let mocks: {
  fetch: jest.Mock<typeof fetch>
  fs: {
    readFile: jest.Mock
    mkdir: jest.Mock
    writeFile: jest.Mock
  }
}

jest.unstable_mockModule('@actions/core', () => core)
jest.unstable_mockModule('@actions/glob', () => ({
  create: async (patterns: string) => ({
    globGenerator: async function* () {
      // Echo patterns as the file list so deterministic tests can
      // assert on a single-element `filenames`.
      for (const pattern of patterns.split('\n').map((s) => s.trim())) {
        if (pattern.length > 0) yield pattern
      }
    }
  })
}))
jest.unstable_mockModule('node:fs/promises', () => ({
  // Returned by value at the moment of import; the per-test
  // `mocks.fs.writeFile` reference may be a different jest.fn after
  // `beforeEach` reassigns it. We expose a `Proxy` so production code
  // always sees the latest wrapper, even when the module record is
  // cached by Node's loader.
  readFile: (...args: unknown[]) => mocks.fs.readFile(...args),
  mkdir: (...args: unknown[]) => mocks.fs.mkdir(...args),
  writeFile: (...args: unknown[]) => mocks.fs.writeFile(...args)
}))

const { run } = await import('../src/main.js')

/** Helper: queue a single fetch reply. */
function mockFetch(
  status: number,
  body: unknown,
  headers: Record<string, string> = {}
): void {
  const responseBody =
    body === null || body === undefined
      ? null
      : typeof body === 'string'
        ? body
        : JSON.stringify(body)
  mocks.fetch.mockResolvedValueOnce(
    new Response(responseBody, {
      status,
      headers:
        body === null || body === undefined
          ? headers
          : { 'Content-Type': 'application/json', ...headers }
    })
  )
}

/** Helper: assert the action was marked failed with a substring. */
function expectFailedWith(substring: string): void {
  expect(core.setFailed).toHaveBeenCalledTimes(1)
  const arg = (core.setFailed as unknown as jest.Mock).mock.calls[0][0]
  const message = typeof arg === 'string' ? arg : (arg as Error).message
  expect(message).toContain(substring)
}

beforeEach(() => {
  resetInputs()
  mocks = {
    fetch: jest.fn<typeof fetch>(),
    fs: {
      readFile: jest.fn(async () => 'mocked file contents'),
      mkdir: jest.fn(async () => undefined),
      writeFile: jest.fn(async () => undefined)
    }
  }
  globalThis.fetch = mocks.fetch as unknown as typeof fetch
})

// (Diagnostic hooks removed once state pollution was diagnosed: a fresh
// `mocks.fetch` per test, paired with `globalThis.fetch` reassignment,
// is enough to keep tests independent.)

describe('main.ts dispatcher', () => {
  it('defaults operation to download when input is empty', async () => {
    setInput('operation', '')
    setInput('gist-id', 'abc123')
    mockFetch(200, {
      id: 'abc123',
      files: {
        'readme.md': { filename: 'readme.md', raw_url: 'https://x/readme.md' }
      }
    })
    mockFetch(200, 'hello content')

    await run()
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'abc123')
  })

  it('create: succeeds and surfaces the new gist id', async () => {
    setInput('operation', 'create')
    setInput('token', 'gh_test')
    setInput('filenames', '/tmp/note.md')
    mockFetch(201, { id: 'new-gist-id' })

    await run()
    expect(mocks.fetch).toHaveBeenCalledTimes(1)
    const [url, init] = mocks.fetch.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('https://api.github.com/gists')
    expect(init.method).toBe('POST')
    const body = JSON.parse(init.body as string)
    expect(body.public).toBe(false)
    expect(body.files['note.md']).toBeDefined()
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'new-gist-id')
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  it('create: public flag only triggers on exact lowercase `true`', async () => {
    setInput('operation', 'create')
    setInput('token', 'gh_test')
    setInput('filenames', '/tmp/note.md')
    setInput('create-public-gist', 'True')
    mockFetch(201, { id: 'new-gist-id' })

    await run()
    const body = JSON.parse(
      (mocks.fetch.mock.calls[0] as [string, RequestInit])[1].body as string
    )
    expect(body.public).toBe(false) // 'True' is NOT 'true'
  })

  it('create: fails when token is missing', async () => {
    setInput('operation', 'create')
    setInput('filenames', '/tmp/note.md')
    delete process.env['GITHUB_TOKEN']
    delete process.env['GH_TOKEN']

    await run()
    expectFailedWith('token with gist write permission')
    expect(mocks.fetch).not.toHaveBeenCalled()
  })

  it('update: PATCHes the gist', async () => {
    setInput('operation', 'update')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    setInput('filenames', '/tmp/x.md')
    mockFetch(200, { id: 'abc' })

    await run()
    const [url, init] = mocks.fetch.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('https://api.github.com/gists/abc')
    expect(init.method).toBe('PATCH')
  })

  it('download: fetches metadata anonymously when no token is provided', async () => {
    setInput('operation', 'download')
    setInput('gist-id', 'abc')
    // Empty `filenames` → download every file. The dispatcher does NOT
    // glob-expand `filenames` for the `download` operation, so leaving it
    // blank exercises the "download all" branch.

    // First fetch: gist metadata.
    mockFetch(200, {
      id: 'abc',
      files: {
        foo: {
          filename: 'foo',
          raw_url: 'https://gist.githubusercontent.com/foo'
        },
        bar: {
          filename: 'bar',
          raw_url: 'https://gist.githubusercontent.com/bar'
        }
      }
    })
    // Second and third: one raw fetch per file.
    mockFetch(200, 'foo content')
    mockFetch(200, 'bar content')

    await run()
    expect(mocks.fetch).toHaveBeenCalledTimes(3)
    // Metadata request carries no `Authorization` header.
    const metaInit = (mocks.fetch.mock.calls[0] as [string, RequestInit])[1]
    expect(
      (metaInit.headers as Record<string, string>).Authorization
    ).toBeUndefined()
    // Raw downloads also stay anonymous.
    const rawInit = (mocks.fetch.mock.calls[1] as [string, RequestInit])[1]
    expect(
      (rawInit.headers as Record<string, string>).Authorization
    ).toBeUndefined()
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'abc')
  })

  it('download: forwards the token to the metadata call when provided', async () => {
    setInput('operation', 'download')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    mockFetch(200, {
      id: 'abc',
      files: {
        foo: {
          filename: 'foo',
          raw_url: 'https://gist.githubusercontent.com/foo'
        }
      }
    })
    mockFetch(200, 'foo content')

    await run()
    // Metadata request gets a Bearer token.
    const metaInit = (mocks.fetch.mock.calls[0] as [string, RequestInit])[1]
    expect((metaInit.headers as Record<string, string>).Authorization).toBe(
      'Bearer gh_test'
    )
    // Raw download stays anonymous even when a token is configured.
    const rawInit = (mocks.fetch.mock.calls[1] as [string, RequestInit])[1]
    expect(
      (rawInit.headers as Record<string, string>).Authorization
    ).toBeUndefined()
  })

  it('download: expands glob patterns against Gist filenames', async () => {
    setInput('operation', 'download')
    setInput('gist-id', 'abc')
    // `out/*.txt` should download every Gist file ending in `.txt` into `out/`.
    setInput('filenames', 'out/*.txt')

    // Metadata: a.txt, b.txt, README.md — only the .txt files should be fetched.
    mockFetch(200, {
      id: 'abc',
      files: {
        'a.txt': {
          filename: 'a.txt',
          raw_url: 'https://gist.githubusercontent.com/a.txt'
        },
        'b.txt': {
          filename: 'b.txt',
          raw_url: 'https://gist.githubusercontent.com/b.txt'
        },
        'README.md': {
          filename: 'README.md',
          raw_url: 'https://gist.githubusercontent.com/README.md'
        }
      }
    })
    // Two raw fetches — one per matching file.
    mockFetch(200, 'a content')
    mockFetch(200, 'b content')

    await run()
    // 1 metadata + 2 raw = 3 fetches. README.md is NOT fetched.
    expect(mocks.fetch).toHaveBeenCalledTimes(3)
    // The two raw fetches are for a.txt and b.txt; their raw_url
    // includes the filename, so we can check ordering.
    const thirdCall = mocks.fetch.mock.calls[2] as [string, RequestInit]
    expect(thirdCall[0]).toContain('b.txt')
  })

  it('download: later entries overwrite earlier ones with the same destination', async () => {
    setInput('operation', 'download')
    setInput('gist-id', 'abc')
    // The two entries both target a.txt: first as a literal, then as a
    // glob hit. The second download's body should be the one on disk.
    setInput('filenames', 'a.txt\n*.txt')

    mockFetch(200, {
      id: 'abc',
      files: {
        'a.txt': {
          filename: 'a.txt',
          raw_url: 'https://gist.githubusercontent.com/a.txt'
        },
        'b.txt': {
          filename: 'b.txt',
          raw_url: 'https://gist.githubusercontent.com/b.txt'
        }
      }
    })
    // 3 raw fetches: a.txt (literal), a.txt (glob), b.txt (glob).
    mockFetch(200, 'first a.txt body')
    mockFetch(200, 'second a.txt body')
    mockFetch(200, 'b.txt body')

    await run()
    expect(mocks.fetch).toHaveBeenCalledTimes(4) // 1 metadata + 3 raw
    // The "writeFile" mock receives the LAST body for `a.txt` ('second a.txt body')
    // because the literal entry comes first and the glob expansion comes
    // second; the second one wins.
    const writeFileCalls = (
      mocks.fs.writeFile as unknown as jest.Mock
    ).mock.calls.filter((c) => c[0] === 'a.txt')
    expect(writeFileCalls).toHaveLength(2)
    expect(writeFileCalls[1][1]).toBe('second a.txt body')
  })

  it('download: fails when no requested file matched', async () => {
    setInput('operation', 'download')
    setInput('gist-id', 'abc')
    setInput('filenames', 'missing.md')
    mockFetch(200, {
      id: 'abc',
      files: { other: { filename: 'other', raw_url: 'https://x/other' } }
    })

    await run()
    expect.assertions(2)
    expectFailedWith('None of the requested files matched')
  })

  it('delete-files: sends { name: null }', async () => {
    setInput('operation', 'delete-files')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    setInput('filenames', 'a.md\nb.md')
    mockFetch(200, {})

    await run()
    const [, init] = mocks.fetch.mock.calls[0] as [string, RequestInit]
    const body = JSON.parse(init.body as string)
    expect(body.files).toEqual({ 'a.md': null, 'b.md': null })
  })

  it('delete-files: normalizes local paths to basenames before PATCH', async () => {
    setInput('operation', 'delete-files')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    // When passing a local absolute path, you must use basename
    // as the gist filename, otherwise PATCH /gists/{id} will return 422
    // due to "malformed path components"
    setInput('filenames', '/tmp/test/test-content.txt\nREADME.md')
    mockFetch(200, {})

    await run()
    const [, init] = mocks.fetch.mock.calls[0] as [string, RequestInit]
    const body = JSON.parse(init.body as string)
    expect(body.files).toEqual({
      'test-content.txt': null,
      'README.md': null
    })
  })

  it('delete-files: expands glob patterns against Gist filenames', async () => {
    setInput('operation', 'delete-files')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    setInput('filenames', '*.txt')

    // First call: GET metadata. Second call: PATCH.
    mockFetch(200, {
      id: 'abc',
      files: {
        'a.txt': { filename: 'a.txt', raw_url: 'x' },
        'b.txt': { filename: 'b.txt', raw_url: 'x' },
        'README.md': { filename: 'README.md', raw_url: 'x' }
      }
    })
    mockFetch(200, {})

    await run()
    expect(mocks.fetch).toHaveBeenCalledTimes(2)
    const patchInit = mocks.fetch.mock.calls[1] as [string, RequestInit]
    expect(patchInit[0]).toBe('https://api.github.com/gists/abc')
    expect(patchInit[1].method).toBe('PATCH')
    const body = JSON.parse(patchInit[1].body as string)
    expect(body.files).toEqual({ 'a.txt': null, 'b.txt': null })
  })

  it('delete-files: literal and glob entries can be mixed, dedup wins', async () => {
    setInput('operation', 'delete-files')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    // `a.md` is also matched by `*.md`; the Set deduplicates so we send
    // one entry per file.
    setInput('filenames', 'a.md\n*.md')

    mockFetch(200, {
      id: 'abc',
      files: {
        'a.md': { filename: 'a.md', raw_url: 'x' },
        'b.md': { filename: 'b.md', raw_url: 'x' }
      }
    })
    mockFetch(200, {})

    await run()
    const patchInit = mocks.fetch.mock.calls[1] as [string, RequestInit]
    const body = JSON.parse(patchInit[1].body as string)
    expect(body.files).toEqual({ 'a.md': null, 'b.md': null })
  })

  it('delete-files: glob with zero matches only warns, does not fail', async () => {
    setInput('operation', 'delete-files')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    setInput('filenames', '*.nope')

    mockFetch(200, {
      id: 'abc',
      files: { 'a.txt': { filename: 'a.txt', raw_url: 'x' } }
    })
    // Even with no matches we still PATCH with an empty `files` object.
    mockFetch(200, {})

    await run()
    expect(core.setFailed).not.toHaveBeenCalled()
    expect(core.warning).toHaveBeenCalled()
    const patchInit = mocks.fetch.mock.calls[1] as [string, RequestInit]
    const body = JSON.parse(patchInit[1].body as string)
    expect(body.files).toEqual({})
  })

  it('delete-gist: DELETEs the gist and echoes input id', async () => {
    setInput('operation', 'delete-gist')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    // 204 No Content cannot carry a body in the Fetch spec.
    mockFetch(204, null)

    await run()
    const [url, init] = mocks.fetch.mock.calls[0] as [string, RequestInit]
    expect(url).toBe('https://api.github.com/gists/abc')
    expect(init.method).toBe('DELETE')
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'abc')
  })

  it('rejects invalid operation strings', async () => {
    setInput('operation', 'smash')
    await run()
    expect.assertions(2)
    expectFailedWith('Invalid operation')
  })

  it('rejects empty filenames for create', async () => {
    setInput('operation', 'create')
    setInput('token', 'gh_test')
    setInput('filenames', '')
    await run()
    expect.assertions(2)
    expectFailedWith('filenames` is required for the create')
  })

  it('API failures surface the status and body', async () => {
    setInput('operation', 'delete-gist')
    setInput('token', 'gh_test')
    setInput('gist-id', 'abc')
    mockFetch(404, '{"message":"Not Found"}')

    await run()
    expect.assertions(4)
    expectFailedWith('GitHub API request failed')
    expectFailedWith('404')
  })
})
