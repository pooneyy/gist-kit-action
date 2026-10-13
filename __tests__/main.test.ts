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
let mocks: { fetch: jest.Mock<typeof fetch> }

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
  readFile: async () => 'mocked file contents',
  mkdir: async () => undefined,
  writeFile: async () => undefined
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
  mocks = { fetch: jest.fn<typeof fetch>() }
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

  it('download: fetches metadata then each file anonymously', async () => {
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
    const metaInit = (mocks.fetch.mock.calls[0] as [string, RequestInit])[1]
    expect(
      (metaInit.headers as Record<string, string>).Authorization
    ).toBeUndefined()
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'abc')
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

  it('log-level=error suppresses debug and info calls', async () => {
    setInput('operation', 'create')
    setInput('token', 'gh_test')
    setInput('filenames', '/tmp/note.md')
    setInput('log-level', 'error')
    mockFetch(201, { id: 'new-gist-id' })

    await run()
    // At error level, both debug and info are filtered out.
    expect(core.debug).not.toHaveBeenCalled()
    expect(core.info).not.toHaveBeenCalled()
    // The action itself still succeeded — only logging was suppressed.
    expect(core.setOutput).toHaveBeenCalledWith('gist-id', 'new-gist-id')
    expect(core.setFailed).not.toHaveBeenCalled()
  })

  it('log-level=debug forwards debug calls too', async () => {
    setInput('operation', 'create')
    setInput('token', 'gh_test')
    setInput('filenames', '/tmp/note.md')
    setInput('log-level', 'debug')
    mockFetch(201, { id: 'new-gist-id' })

    await run()
    expect(core.debug).toHaveBeenCalled()
    expect(core.info).toHaveBeenCalledTimes(1)
  })
})
