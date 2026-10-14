/**
 * Direct tests for `utils.ts`. These run without any mocking because the
 * helpers under test don't touch the filesystem, the network, or
 * `@actions/core` (apart from the documented `debug` call in
 * `downloadRaw`'s retry path — covered indirectly from `main.test.ts`).
 */
import {
  basenameOf,
  expandFilenames,
  parseBooleanStrict,
  parseOperation,
  resolveToken
} from '../src/utils.js'

const origToken = process.env['GITHUB_TOKEN']
const origGh = process.env['GH_TOKEN']

describe('utils', () => {
  beforeEach(() => {
    delete process.env['GITHUB_TOKEN']
    delete process.env['GH_TOKEN']
  })
  afterAll(() => {
    if (origToken) process.env['GITHUB_TOKEN'] = origToken
    if (origGh) process.env['GH_TOKEN'] = origGh
  })

  describe('resolveToken', () => {
    it('prefers the explicit input', () => {
      process.env['GITHUB_TOKEN'] = 'env'
      expect(resolveToken('explicit')).toBe('explicit')
    })

    it('falls back to GITHUB_TOKEN then GH_TOKEN', () => {
      process.env['GITHUB_TOKEN'] = 'env'
      expect(resolveToken('')).toBe('env')
      delete process.env['GITHUB_TOKEN']
      process.env['GH_TOKEN'] = 'gh'
      expect(resolveToken('')).toBe('gh')
    })

    it('returns null when nothing is configured', () => {
      expect(resolveToken('')).toBeNull()
    })

    it('ignores empty env vars', () => {
      process.env['GITHUB_TOKEN'] = ''
      expect(resolveToken('')).toBeNull()
    })
  })

  describe('parseBooleanStrict', () => {
    it.each([
      ['true', true],
      ['false', false],
      ['', false],
      ['True', false],
      ['TRUE', false],
      ['1', false],
      ['yes', false]
    ])('parseBooleanStrict(%j) → %s', (input, expected) => {
      expect(parseBooleanStrict(input)).toBe(expected)
    })
  })

  describe('parseOperation', () => {
    it('defaults to download for empty input', () => {
      expect(parseOperation('')).toBe('download')
      expect(parseOperation('   ')).toBe('download')
    })

    it('accepts every documented operation', () => {
      for (const op of [
        'create',
        'update',
        'download',
        'delete-files',
        'delete-gist'
      ]) {
        expect(parseOperation(op)).toBe(op)
      }
    })

    it('throws on unknown values', () => {
      expect(() => parseOperation('smash')).toThrow(/Invalid operation/)
    })
  })

  describe('basenameOf', () => {
    it.each([
      ['/tmp/a.txt', 'a.txt'],
      ['/tmp/sub/a.txt', 'a.txt'],
      ['C:\\\\tmp\\\\a.txt', 'a.txt'],
      ['plain.md', 'plain.md'],
      ['', '']
    ])('basenameOf(%j) → %s', (input, expected) => {
      expect(basenameOf(input)).toBe(expected)
    })
  })

  describe('expandFilenames', () => {
    it('returns [] for empty input', async () => {
      const out = await expandFilenames('', async () => ({
        globGenerator: async function* () {
          // empty
        }
      }))
      expect(out).toEqual([])
    })

    it('dedupes results from the globber', async () => {
      const out = await expandFilenames('a.txt\nb.txt', async () => ({
        globGenerator: async function* () {
          yield 'a.txt'
          yield 'b.txt'
          yield 'a.txt'
        }
      }))
      expect(out).toEqual(['a.txt', 'b.txt'])
    })
  })
})
