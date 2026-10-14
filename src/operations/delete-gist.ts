/**
 * `delete-gist` 操作：调用 DELETE /gists/{gist_id}。
 *
 * Gist API 成功时返回 204 No Content
 */
import * as core from '@actions/core'
import { apiFetch } from '../utils.js'
import type { OperationResult, ResolvedInputs } from '../types.js'

export async function run(inputs: ResolvedInputs): Promise<OperationResult> {
  if (!inputs.gistId) {
    throw new Error('`gist-id` is required for the delete-gist operation')
  }
  if (!inputs.token) {
    throw new Error(
      'A token with gist write permission is required for the delete-gist operation'
    )
  }

  core.debug(`Deleting Gist ${inputs.gistId}`)
  await apiFetch(`/gists/${encodeURIComponent(inputs.gistId)}`, {
    method: 'DELETE',
    token: inputs.token
  })
  core.info(`Deleted Gist ${inputs.gistId}`)
  return {}
}
