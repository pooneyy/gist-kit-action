/**
 * `delete-files` 操作：以 files: { "name1": null, "name2": null, ... }
 * 调用 PATCH /gists/{gist_id}
 */
import type { Logger } from '../logger.js'
import { apiFetch, basenameOf } from '../utils.js'
import type { OperationResult, ResolvedInputs } from '../types.js'

export async function run(
  inputs: ResolvedInputs,
  log: Logger
): Promise<OperationResult> {
  if (!inputs.gistId) {
    throw new Error('`gist-id` is required for the delete-files operation')
  }
  if (!inputs.filenames || inputs.filenames.length === 0) {
    throw new Error('`filenames` is required for the delete-files operation')
  }
  if (!inputs.token) {
    throw new Error(
      'A token with gist write permission is required for the delete-files operation'
    )
  }

  // 丢弃空行，并确保能从可能的传入的路径中获取文件名
  const names = inputs.filenames.filter((n) => n.length > 0).map(basenameOf)
  const files: Record<string, null> = {}
  for (const name of names) files[name] = null

  log.debug(
    `Deleting ${names.length} file(s) from Gist ${inputs.gistId}: ${names.join(', ')}`
  )

  await apiFetch(`/gists/${encodeURIComponent(inputs.gistId)}`, {
    method: 'PATCH',
    token: inputs.token,
    body: { files }
  })
  log.info(`Deleted ${names.length} file(s) from Gist ${inputs.gistId}`)
  return {}
}
