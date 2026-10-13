/**
 * `update` 操作：调用 PATCH /gists/{gist_id}
 *
 * GitHub PATCH 的语义：
 *   - `files` 中列出的文件会被 upsert（已存在则覆盖，新增则创建）。
 *   - 没有在 `files` 中列出的文件保持不变。
 *   - 若要从 Gist 中删除文件，将其值设为 `null`
 *
 * 这里仅支持 "upsert" 子集；删除操作由独立的 delete-files.ts 处理。
 */
import { readFile } from 'node:fs/promises'
import type { Logger } from '../logger.js'
import { basenameOf, apiFetch } from '../utils.js'
import type { OperationResult, ResolvedInputs } from '../types.js'

export async function run(
  inputs: ResolvedInputs,
  log: Logger
): Promise<OperationResult> {
  if (!inputs.gistId) {
    throw new Error('`gist-id` is required for the update operation')
  }
  if (!inputs.filenames || inputs.filenames.length === 0) {
    throw new Error('`filenames` is required for the update operation')
  }
  if (!inputs.token) {
    throw new Error(
      'A token with gist write permission is required for the update operation'
    )
  }

  log.debug(
    `Updating Gist ${inputs.gistId} with ${inputs.filenames.length} file(s)`
  )

  const files: Record<string, { content: string }> = {}
  for (const filePath of inputs.filenames) {
    files[basenameOf(filePath)] = { content: await readFile(filePath, 'utf8') }
  }

  await apiFetch(`/gists/${encodeURIComponent(inputs.gistId)}`, {
    method: 'PATCH',
    token: inputs.token,
    body: { files }
  })
  log.info(`Updated Gist ${inputs.gistId}`)
  return {}
}
