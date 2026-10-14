/**
 * `delete-files` 操作：以 files: { "name1": null, "name2": null, ... }
 * 调用 PATCH /gists/{gist_id}
 *
 * `filenames` 列表里的每一行支持两种形式：
 *   - 字面文件名（或带路径前缀）：`a.txt` / `/tmp/foo.txt`。
 *     取其 `basename` 作为 Gist 中的文件名，删除该文件。
 *   - glob 兼容（包含 `*` / `?`）: `*.txt` / `prefix/*.log`
 *
 * 0 命中时只发出 warning，不让 action 失败——用户可能想清理一个本来
 * 就是空的集合；与 `download` 的"0 匹配则 fail"策略有意区别。
 */
import * as core from '@actions/core'
import {
  GLOB_META_CHARS,
  apiFetch,
  basenameOf,
  compileTemplate
} from '../utils.js'
import type {
  GistApiResponse,
  OperationResult,
  ResolvedInputs
} from '../types.js'

export async function run(inputs: ResolvedInputs): Promise<OperationResult> {
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

  const literalEntries: string[] = []
  const globEntries: string[] = []
  for (const entry of inputs.filenames.filter((n) => n.length > 0)) {
    if (GLOB_META_CHARS.test(entry)) globEntries.push(entry)
    else literalEntries.push(entry)
  }

  const names = new Set<string>()
  for (const entry of literalEntries) {
    names.add(basenameOf(entry))
  }

  if (globEntries.length > 0) {
    const metaResponse = await apiFetch(
      `/gists/${encodeURIComponent(inputs.gistId)}`,
      { method: 'GET', token: inputs.token }
    )
    const meta = (await metaResponse.json()) as GistApiResponse
    const gistFileNames = Object.keys(meta.files)

    for (const template of globEntries) {
      const re = compileTemplate(basenameOf(template))
      let hits = 0
      for (const name of gistFileNames) {
        if (re.test(name)) {
          names.add(name)
          hits += 1
        }
      }
      if (hits === 0) {
        core.warning(
          `Glob "${template}" did not match any file in Gist ${inputs.gistId}; ignoring`
        )
      } else {
        core.debug(`Glob "${template}" matched ${hits} file(s)`)
      }
    }
  }

  if (names.size === 0) {
    core.warning(
      `No files matched the delete-files input for Gist ${inputs.gistId}; PATCH will be a no-op`
    )
  }

  const files: Record<string, null> = {}
  for (const name of names) files[name] = null

  core.debug(
    `Deleting ${names.size} file(s) from Gist ${inputs.gistId}: ${[...names].join(', ')}`
  )

  await apiFetch(`/gists/${encodeURIComponent(inputs.gistId)}`, {
    method: 'PATCH',
    token: inputs.token,
    body: { files }
  })
  core.info(`Deleted ${names.size} file(s) from Gist ${inputs.gistId}`)
  return {}
}
