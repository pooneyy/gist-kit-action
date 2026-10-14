/**
 * `download` 步骤:
 * 1. 匿名调用 GET /gists/{gist_id} 拉取元数据
 * 2. 遍历返回的 `files` 字段
 * 3. 匿名拉取每个 `raw_url`
 * 4. 写入指定的目标路径
 *
 * 文件名语义:
 *   - `filenames` 可选。未提供时下载全部文件，写入当前工作目录下以其
 *     basename 命名的文件。
 *   - 提供时则是"模板路径"列表：
 *     - 如果某行包含 glob 元字符（`*` / `?`），按 glob 匹配 Gist 中的
 *       文件名展开。命中后，把模板里 glob 部分替换成实际文件名，得到
 *       下载目标路径。例：`out/*.txt` 命中 Gist 的 `a.txt` / `b.txt`，
 *       则写入 `out/a.txt` / `out/b.txt`。
 *     - 如果某行不含 glob, 则按 `basename` 与 Gist 文件名精确匹配。
 *       命中后，写入传入时提供的完整目标路径。例：`/tmp/a.txt` 写到
 *       `/tmp/a.txt`。
 *   - 未匹配上的会被警告并跳过。如果所有条目都没有命中任何文件，则让
 *     action 失败
 */
import * as core from '@actions/core'
import { mkdir, writeFile } from 'node:fs/promises'
import * as path from 'node:path'
import {
  GLOB_META_CHARS,
  apiFetch,
  basenameOf,
  compileTemplate,
  downloadRaw
} from '../utils.js'
import type {
  GistApiResponse,
  OperationResult,
  ResolvedInputs
} from '../types.js'

type Desired = { destination: string; name: string }

/**
 * 把模板路径展开成 {name, destination} 列表。
 *   - 用模板的 `basename` 部分作为 glob 模式编译,匹配 Gist 文件名
 *   - 模板的 `dirname` 部分作为目标目录前缀
 *   - 命中 0 个时返回 []
 *   - 命中 N 个时每个匹配生成一条 {name, destination}
 *
 * 例:模板 `out/*.txt` 命中 Gist 的 `a.txt` / `b.txt` 时,生成
 *   `[{ name: 'a.txt', destination: 'out/a.txt' },
 *     { name: 'b.txt', destination: 'out/b.txt' }]`
 */
function expandTemplate(template: string, names: string[]): Desired[] {
  const dir = path.win32.dirname(template)
  const namePattern = path.win32.basename(template)
  const re = compileTemplate(namePattern)
  return names
    .filter((n) => re.test(n))
    .map((name) => ({
      name,
      destination: dir === '.' || dir === '' ? name : `${dir}/${name}`
    }))
}

export async function run(inputs: ResolvedInputs): Promise<OperationResult> {
  if (!inputs.gistId) {
    throw new Error('`gist-id` is required for the download operation')
  }

  core.debug(`Fetching metadata for Gist ${inputs.gistId}`)

  // 拉取 gist 元数据
  //   - 当 `token` 为空字符串或未提供时,使用匿名请求
  //   - 当用户提供了 token 时,带上 `Authorization: Bearer` 请求头
  //     （可能提升 rate limit、访问私有 gist 等）
  //   - raw_url 的下载始终保持匿名
  const metaResponse = await apiFetch(
    `/gists/${encodeURIComponent(inputs.gistId)}`,
    { method: 'GET', token: inputs.token || null }
  )
  const meta = (await metaResponse.json()) as GistApiResponse
  const gistFileNames = Object.keys(meta.files)

  // 构造 {目标路径: 期望文件名} 的配对
  //   - 输入为空则将每个文件下载到当前工作目录下以其 basename 命名的文件
  //   - 输入非空：含 glob 的当模板展开；不含 glob 的当字面量，basename 匹配
  let desired: Desired[]
  if (inputs.filenames && inputs.filenames.length > 0) {
    desired = []
    for (const entry of inputs.filenames) {
      if (GLOB_META_CHARS.test(entry)) {
        desired.push(...expandTemplate(entry, gistFileNames))
      } else {
        desired.push({ destination: entry, name: basenameOf(entry) })
      }
    }
  } else {
    desired = gistFileNames.map((name) => ({ destination: name, name }))
  }

  let matched = 0
  for (const { destination, name } of desired) {
    const entry = meta.files[name]
    if (!entry) {
      core.warning(
        `File "${name}" was requested but does not exist in Gist ${inputs.gistId}; skipping`
      )
      continue
    }
    matched += 1
    core.debug(`Downloading ${name} → ${destination}`)
    const raw = await downloadRaw(entry.raw_url)
    const body = await raw.text()
    await mkdir(path.dirname(destination), { recursive: true })
    await writeFile(destination, body, 'utf8')
  }

  if (desired.length > 0 && matched === 0) {
    throw new Error(
      `None of the requested files matched any file in Gist ${inputs.gistId}`
    )
  }

  core.info(
    `Downloaded ${matched} file(s) from Gist ${inputs.gistId}` +
      (matched < desired.length ? ` (${desired.length - matched} skipped)` : '')
  )
  return {}
}
