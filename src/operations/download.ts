/**
 * `download` 步骤:
 * 1. 匿名调用 GET /gists/{gist_id} 拉取元数据
 * 2. 遍历返回的 `files` 字段
 * 3. 匿名拉取每个 `raw_url`
 * 4. 写入指定的目标路径
 *
 * 文件名语义（依据规范）：
 *   - `filenames` 可选。未提供时下载全部文件，写入当前工作目录下以其
 *     basename 命名的文件。
 *   - 提供时，则是期望的目标路径。去掉前面的目录部分（`basename`），
 *     用它与 Gist 中的文件名匹配。匹配成功后，将文件写入调用者提供的
 *     目标路径
 *     例: `/tmp/a.txt` 会写到 `/tmp/a.txt`，而不是 `./a.txt`
 *   - 未匹配上的会被警告并跳过。如果没有任何文件匹配成功，则让 action
 *     失败
 */
import { mkdir, writeFile } from 'node:fs/promises'
import * as path from 'node:path'
import type { Logger } from '../logger.js'
import { apiFetch, basenameOf, downloadRaw } from '../utils.js'
import type {
  GistApiResponse,
  OperationResult,
  ResolvedInputs
} from '../types.js'

export async function run(
  inputs: ResolvedInputs,
  log: Logger
): Promise<OperationResult> {
  if (!inputs.gistId) {
    throw new Error('`gist-id` is required for the download operation')
  }

  log.debug(`Fetching metadata for Gist ${inputs.gistId} (anonymous)`)

  // 拉取 gist 元数据。API 允许匿名调用
  const metaResponse = await apiFetch(
    `/gists/${encodeURIComponent(inputs.gistId)}`,
    { method: 'GET', token: null }
  )
  const meta = (await metaResponse.json()) as GistApiResponse

  // 构造 {目标路径: 期望文件名} 的配对
  //   - 输入为空则将每个文件下载到当前工作目录下以其 basename 命名的文件
  //   - 输入非空则对每行按 basename 进行匹配；匹配成功后，写入调用者最初
  //     给出的完整路径
  type Desired = { destination: string; name: string }
  const desired: Desired[] =
    inputs.filenames && inputs.filenames.length > 0
      ? inputs.filenames.map((destination) => ({
          destination,
          name: basenameOf(destination)
        }))
      : Object.keys(meta.files).map((name) => ({
          destination: name,
          name
        }))

  let matched = 0
  for (const { destination, name } of desired) {
    const entry = meta.files[name]
    if (!entry) {
      log.warn(
        `File "${name}" was requested but does not exist in Gist ${inputs.gistId}; skipping`
      )
      continue
    }
    matched += 1
    log.debug(`Downloading ${name} → ${destination}`)
    const raw = await downloadRaw(entry.raw_url, 1, log)
    const body = await raw.text()
    await mkdir(path.dirname(destination), { recursive: true })
    await writeFile(destination, body, 'utf8')
  }

  if (desired.length > 0 && matched === 0) {
    throw new Error(
      `None of the requested files matched any file in Gist ${inputs.gistId}`
    )
  }

  log.info(
    `Downloaded ${matched} file(s) from Gist ${inputs.gistId}` +
      (matched < desired.length ? ` (${desired.length - matched} skipped)` : '')
  )
  return {}
}
