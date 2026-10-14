/**
 * `create` 操作：以 `files: { <name>: { content } }` 调用 POST /gists
 */
import * as core from '@actions/core'
import { readFile } from 'node:fs/promises'
import { apiFetch, basenameOf } from '../utils.js'
import type { OperationResult, ResolvedInputs } from '../types.js'

interface CreatedGistResponse {
  id: string
}

export async function run(inputs: ResolvedInputs): Promise<OperationResult> {
  if (!inputs.filenames || inputs.filenames.length === 0) {
    throw new Error('`filenames` is required for the create operation')
  }
  if (!inputs.token) {
    throw new Error(
      'A token with gist write permission is required for the create operation'
    )
  }

  core.debug(`Creating Gist with ${inputs.filenames.length} file(s)`)

  const files: Record<string, { content: string }> = {}
  for (const filePath of inputs.filenames) {
    files[basenameOf(filePath)] = { content: await readFile(filePath, 'utf8') }
  }

  const body = {
    description: 'Created by gist-kit-action',
    public: inputs.createPublic === true,
    files
  }

  const response = await apiFetch('/gists', {
    method: 'POST',
    token: inputs.token,
    body
  })
  const created = (await response.json()) as CreatedGistResponse
  core.info(`Created Gist ${created.id}`)
  return { newGistId: created.id }
}
