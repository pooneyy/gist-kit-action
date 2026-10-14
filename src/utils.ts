/**
 * 公共工具：token 解析、输入解析、glob 展开以及所有操作共用的 HTTP 辅助函数
 *
 */
import * as core from '@actions/core'
import * as path from 'node:path'
import type { Operation } from './types.js'
import { OPERATIONS } from './types.js'

const GITHUB_API_ROOT = 'https://api.github.com'
const RAW_DOWNLOAD_RETRIES = 3
const RAW_DOWNLOAD_RETRY_DELAY_MS = 1000

/** 用于重试退避的休眠辅助函数 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * 对 `fetch` 进行的间接封装。我们刻意通过绑定到 `globalThis.fetch` 的
 * `const` 来调用，这样打包器/编译器就无法在静态分析时将调用点替换成
 * 模块作用域的绑定（那会破坏通过 `globalThis.fetch = jest.fn(...)` 进行
 * 测试 mock 的可行性）。
 */
const g = globalThis as { fetch: typeof fetch }
function doFetch(input: string, init?: RequestInit): Promise<Response> {
  return g.fetch(input, init)
}

/**
 * 按以下优先级获取 Gist 访问令牌:
 *   1. 显式传入的 `with.token` 输入
 *   2. `GITHUB_TOKEN` 环境变量
 *   3. `GH_TOKEN` 环境变量
 *
 * 当没有任何 token 时返回 `null`；由调用者决定当前操作是否必须使用 token
 *
 * 写操作总是需要 token, 只读的 `download` 不需要 token
 */
export function resolveToken(inputToken: string): string | null {
  if (inputToken) return inputToken
  const envToken = process.env['GITHUB_TOKEN'] ?? process.env['GH_TOKEN']
  return envToken && envToken.length > 0 ? envToken : null
}

/**
 * 严格的布尔解析器: `true` 区分大小写，其他任何值一律为 false。
 */
export function parseBooleanStrict(raw: string): boolean {
  return raw === 'true'
}

/**
 * 解析 `operation` 输入。空字符串回退到默认值（`download`）
 * 任何非空但不在允许列表中的值都会抛出异常，由分发器将 action 标记为失败
 */
export function parseOperation(raw: string): Operation {
  const value = raw.trim()
  if (value === '') return 'download'
  if ((OPERATIONS as readonly string[]).includes(value)) {
    return value as Operation
  }
  throw new Error(
    `Invalid operation "${raw}". Allowed: ${OPERATIONS.join(', ')}`
  )
}

/**
 * 将 `filenames` 输入（以换行分隔、可包含 glob 模式）展开为去重且排序后的
 * 本地文件路径列表。
 *
 * 输入为空时返回 `[]`；是否允许为空由调用者决定。
 */
export async function expandFilenames(
  raw: string,
  globberFactory: (
    patterns: string
  ) => Promise<{ globGenerator: () => AsyncGenerator<string, void> }>
): Promise<string[]> {
  const patterns = raw
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)

  if (patterns.length === 0) return []

  const globber = await globberFactory(patterns.join('\n'))
  const seen = new Set<string>()
  for await (const file of globber.globGenerator()) {
    seen.add(file)
  }
  return [...seen].sort()
}

/** 以跨平台的方式提取路径的最后一段 */
export function basenameOf(p: string): string {
  return path.win32.basename(p)
}

/**
 * 构造访问 GitHub REST API 所使用的请求头。当提供 `token` 时，请求会附上
 * 标准的 `Bearer` 授权头；否则为匿名请求（`download` 操作采用这种形式）。
 */
function buildHeaders(token: string | null): Record<string, string> {
  const headers: Record<string, string> = {
    Accept: 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28'
  }
  if (token) headers['Authorization'] = `Bearer ${token}`
  return headers
}

/**
 * 当 API 没有返回 2xx 时，抛出包含响应体的友好错误。把响应体一并附上，
 * 在 Actions 日志中可直接看到 GitHub 给出的错误信息。
 */
async function raiseForResponse(
  response: Response,
  context: string
): Promise<never> {
  const text = await response.text().catch(() => '<unreadable body>')
  throw new Error(
    `GitHub API request failed (${context}): ${response.status} ${response.statusText}\n${text}`
  )
}

/**
 * 向 GitHub REST API 发起的鉴权（或匿名）JSON 请求
 */
export async function apiFetch(
  path: string,
  init: { method: string; token: string | null; body?: unknown }
): Promise<Response> {
  const response = await doFetch(`${GITHUB_API_ROOT}${path}`, {
    method: init.method,
    headers: {
      ...buildHeaders(init.token),
      ...(init.body !== undefined ? { 'Content-Type': 'application/json' } : {})
    },
    body: init.body !== undefined ? JSON.stringify(init.body) : undefined
  })
  if (!response.ok) await raiseForResponse(response, `${init.method} ${path}`)
  return response
}

/**
 * 下载 GitHub Gist 的原始文件内容
 *
 * 在任意失败时会重试，最多重试 {@link RAW_DOWNLOAD_RETRIES} 次，
 * 两次尝试之间间隔 {@link RAW_DOWNLOAD_RETRY_DELAY_MS} 毫秒。
 *
 * 将"最后一次尝试"计为第 (n+1) 次，因此 `RAW_DOWNLOAD_RETRIES = 3`
 * 表示"总共最多 3 次尝试"，而不是"在首次尝试之外再重试 3 次"。
 */
export async function downloadRaw(url: string, attempt = 1): Promise<Response> {
  let response: Response
  try {
    response = await doFetch(url, {
      headers: { Accept: 'application/octet-stream' }
    })
  } catch (err) {
    if (attempt < RAW_DOWNLOAD_RETRIES) {
      await sleep(RAW_DOWNLOAD_RETRY_DELAY_MS)
      return downloadRaw(url, attempt + 1)
    }
    throw new Error(
      `Network error downloading ${url} after ${RAW_DOWNLOAD_RETRIES} attempts: ${
        err instanceof Error ? err.message : String(err)
      }`,
      { cause: err }
    )
  }
  if (!response.ok && attempt < RAW_DOWNLOAD_RETRIES) {
    core.debug(
      `raw download attempt ${attempt} failed (${response.status}); retrying in ${RAW_DOWNLOAD_RETRY_DELAY_MS}ms`
    )
    await sleep(RAW_DOWNLOAD_RETRY_DELAY_MS)
    return downloadRaw(url, attempt + 1)
  }
  if (!response.ok) {
    await raiseForResponse(response, `GET ${url}`)
  }
  return response
}

/** 检测路径/名称字符串中是否包含 glob 元字符 `*` / `?` / `[` */
export const GLOB_META_CHARS = /[*?[\]]/

/**
 * 将含 glob 元字符的字符串编译成正则表达式。
 *
 * 只支持 `*` / `?` 两个通配符——因为 GitHub Gist 文件名不允许包含 `/`,
 * 所以 `**` 的"跨目录"语义不会出现在这个 action 的合法输入里。
 *
 * `*` 匹配零或多个非 `/` 字符，`?` 匹配单个非 `/` 字符，其他字符做
 * 正则转义。
 */
export function compileTemplate(template: string): RegExp {
  let body = ''
  for (const ch of template) {
    if (ch === '*') body += '[^/]*'
    else if (ch === '?') body += '[^/]'
    else body += ch.replace(/[\\^$+(){}|]/g, '\\$&')
  }
  return new RegExp('^' + body + '$')
}
