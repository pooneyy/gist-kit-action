/**
 * gist-kit-action 的共享类型定义
 */

/**
 * 本 action 支持的操作集合
 */
export type Operation =
  'create' | 'update' | 'download' | 'delete-files' | 'delete-gist'

export const OPERATIONS: readonly Operation[] = [
  'create',
  'update',
  'download',
  'delete-files',
  'delete-gist'
] as const

/**
 * action 模块所使用的、已解析并校验过的输入。`main.ts` 中的分发器负责执行校验
 * 并产出这种形态的数据
 *
 * `createPublic` 的类型为 `boolean | undefined`：
 *   - `true`: 创建公开 Gist
 *   - `false`: 创建私有 Gist，若传入了无法识别的值，也会默认创建私有 Gist
 *   - `undefined`: 当前 action 不适用该字段
 *
 * `filenames` 本地文件路径列表
 */
export interface ResolvedInputs {
  token: string
  gistId?: string
  operation: Operation
  createPublic?: boolean
  /** 若当前操作不需要则为 undefined */
  filenames?: string[]
  logLevel: 'debug' | 'info' | 'warn' | 'error'
}

/**
 * 实际用到的 Gist REST API 响应字段
 *
 * 参考：https://docs.github.com/en/rest/gists/gists#get-a-gist
 */
export interface GistApiResponse {
  id: string
  files: Record<
    string,
    {
      filename: string
      raw_url: string
      truncated?: boolean
      content?: string
    }
  >
}

/**
 * 每个操作的 `run()` 交给分发器的返回结构。`newGistId` 仅由 `create` 操作设置
 *（因为只有它会由后端分配新的 id）；其他操作则原样回显输入的 `gistId`
 */
export interface OperationResult {
  newGistId?: string
}
