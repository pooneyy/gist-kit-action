/**
 * Tests for `src/logger.ts`
 *
 * Validates that `makeLogger` filters by level:
 *     - levels below the threshold are dropped
 *     - levels at or above the threshold are forwarded to `@actions/core`
 *     - `error` is always forwarded (it carries diagnostic weight and
 *       should never be silently dropped)
 */
import { jest } from '@jest/globals'

const coreMock = {
  debug: jest.fn(),
  info: jest.fn(),
  warning: jest.fn(),
  error: jest.fn()
}

jest.unstable_mockModule('@actions/core', () => coreMock)

const { makeLogger } = await import('../src/logger.js')

beforeEach(() => {
  coreMock.debug.mockClear()
  coreMock.info.mockClear()
  coreMock.warning.mockClear()
  coreMock.error.mockClear()
})

describe('makeLogger', () => {
  it.each([
    // At level=debug, every severity is forwarded (debug once, info once, warn once, error once).
    ['debug', 1, 1, 1, 1],
    // At level=info, debug is dropped; the rest pass through.
    ['info', 0, 1, 1, 1],
    // At level=warn, debug and info are dropped.
    ['warn', 0, 0, 1, 1],
    // At level=error, only `error` is forwarded (warn is also dropped here).
    ['error', 0, 0, 0, 1]
  ] as const)(
    'level=%s → debug=%i info=%i warn=%i error=%i',
    (level, debugCalls, infoCalls, warnCalls, errorCalls) => {
      const log = makeLogger(level)
      log.debug('d')
      log.info('i')
      log.warn('w')
      log.error('e')
      expect(coreMock.debug).toHaveBeenCalledTimes(debugCalls)
      expect(coreMock.info).toHaveBeenCalledTimes(infoCalls)
      expect(coreMock.warning).toHaveBeenCalledTimes(warnCalls)
      expect(coreMock.error).toHaveBeenCalledTimes(errorCalls)
    }
  )

  it('always forwards error, even at error level', () => {
    const log = makeLogger('error')
    log.error('boom')
    expect(coreMock.error).toHaveBeenCalledWith('boom')
  })
})
