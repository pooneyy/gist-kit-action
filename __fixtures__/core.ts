/**
 * Mock for `@actions/core` so tests don't need to spawn a runner.
 *
 * `getInput` reads from an override map populated by `setInput(...)`. The
 * map keeps a separate slot per input name; missing names return `''`,
 * matching GitHub's behaviour when an input has no default value.
 */
import type * as core from '@actions/core'
import { jest } from '@jest/globals'

type InputName =
  | 'token'
  | 'gist-id'
  | 'operation'
  | 'create-public-gist'
  | 'filenames'
  | 'log-level'

const inputs: Partial<Record<InputName, string>> = {}

export function setInput(name: InputName, value: string): void {
  inputs[name] = value
}

export function resetInputs(): void {
  for (const key of Object.keys(inputs) as Array<keyof typeof inputs>) {
    delete inputs[key]
  }
}

export const debug = jest.fn<typeof core.debug>()
export const info = jest.fn<typeof core.info>()
export const warning = jest.fn<typeof core.warning>()
export const notice = jest.fn<typeof core.notice>()
export const error = jest.fn<typeof core.error>()
export const setOutput = jest.fn<typeof core.setOutput>()
export const setFailed = jest.fn<typeof core.setFailed>()

/**
 * `getInput` reads from the override map populated by `setInput(...)`.
 *
 * Important: this is a **plain function**, not a `jest.fn` wrapper.
 * `resetAllMocks()` would otherwise strip the implementation, leaving
 * `core.getInput(name)` returning `undefined` and causing `parseOperation`
 * to throw "Cannot read properties of undefined (reading 'trim')". Tests
 * that need to assert on calls should use `jest.spyOn(core, 'getInput')`.
 */
function getInputImpl(name: string): string {
  return inputs[name as InputName] ?? ''
}

export function getInput(name: string): string {
  return getInputImpl(name)
}
