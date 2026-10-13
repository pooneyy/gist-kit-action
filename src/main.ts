/**
 * main.ts 负责:
 *
 *   1. 读取并解析 action 的输入参数
 *   2. 校验跨输入的依赖条件（例如 "create 操作需要 token"）
 *   3. 转发到对应的 `operations/*.ts` 模块
 *   4. 将最终的 `gist-id` 暴露为 action 输出
 *
 * 其他一切 HTTP 调用、文件 I/O、重试逻辑全部放在 `utils.ts` 或
 * 对应的具体操作模块中
 */
import * as core from '@actions/core'
import * as glob from '@actions/glob'
import { makeLogger } from './logger.js'
import {
  basenameOf,
  expandFilenames,
  parseBooleanStrict,
  parseOperation,
  resolveLogLevel,
  resolveToken
} from './utils.js'
import type { OperationResult, ResolvedInputs } from './types.js'
import { run as createRun } from './operations/create.js'
import { run as updateRun } from './operations/update.js'
import { run as downloadRun } from './operations/download.js'
import { run as deleteFilesRun } from './operations/delete-files.js'
import { run as deleteGistRun } from './operations/delete-gist.js'
import type { Logger } from './logger.js'

const HANDLERS: Record<
  ResolvedInputs['operation'],
  (i: ResolvedInputs, log: Logger) => Promise<OperationResult>
> = {
  create: createRun,
  update: updateRun,
  download: downloadRun,
  'delete-files': deleteFilesRun,
  'delete-gist': deleteGistRun
}

export async function run(): Promise<void> {
  try {
    const rawToken = core.getInput('token')
    const rawGistId = core.getInput('gist-id')
    const rawOperation = core.getInput('operation')
    const rawCreatePublic = core.getInput('create-public-gist')
    const rawFilenames = core.getInput('filenames')
    const rawLogLevel = core.getInput('log-level')

    const operation = parseOperation(rawOperation)
    const token = resolveToken(rawToken)
    const logLevel = resolveLogLevel(rawLogLevel)
    const log = makeLogger(logLevel)
    log.debug(`Resolved operation: ${operation}`)
    log.debug(`Resolved log-level: ${logLevel}`)
    log.debug(`Token present: ${token !== null}`)
    if (rawGistId) log.debug(`Gist id: ${rawGistId}`)
    if (rawCreatePublic) {
      log.debug(
        `create-public-gist=${rawCreatePublic} → ${parseBooleanStrict(rawCreatePublic)}`
      )
    }

    // `filenames` 的语义随操作类型不同而不同：
    //   - `create` / `update`：必填，作为本地文件系统的匹配模式
    //     输入为空或没有匹配项则失败
    //   - `delete-files`：必填，按字面量解析为要删除的 Gist 文件名列表，
    //     不进行 glob 展开
    //   - `download`：可选。输入为空表示"下载全部文件"；否则按
    //     `basename` 与 Gist 文件名进行匹配，不展开本地文件系统
    //   - `delete-gist`：忽略
    let filenames: string[] | undefined
    switch (operation) {
      case 'create':
      case 'update': {
        if (rawFilenames.trim().length === 0) {
          throw new Error(
            `\`filenames\` is required for the ${operation} operation`
          )
        }
        const expanded = await expandFilenames(rawFilenames, glob.create)
        if (expanded.length === 0) {
          throw new Error(
            `\`filenames\` expanded to zero matches for the ${operation} operation`
          )
        }
        filenames = expanded
        break
      }
      case 'delete-files': {
        const list = rawFilenames
          .split('\n')
          .map((s) => s.trim())
          .filter((s) => s.length > 0)
        if (list.length === 0) {
          throw new Error(
            '`filenames` is required for the delete-files operation'
          )
        }
        filenames = list
        break
      }
      case 'download': {
        if (rawFilenames.trim().length > 0) {
          filenames = rawFilenames
            .split('\n')
            .map((s) => s.trim())
            .filter((s) => s.length > 0)
        }
        break
      }
      case 'delete-gist': {
        if (rawFilenames.trim().length > 0) {
          log.debug('Ignoring `filenames` for the delete-gist operation')
        }
        break
      }
    }

    // `create-public-gist` 仅对 create 操作有效。
    let createPublic: boolean | undefined
    if (operation === 'create') {
      createPublic = parseBooleanStrict(rawCreatePublic)
    } else if (rawCreatePublic.trim().length > 0) {
      log.debug(
        `Ignoring \`create-public-gist\` for the ${operation} operation`
      )
    }

    const inputs: ResolvedInputs = {
      token: token ?? '',
      operation,
      logLevel,
      ...(rawGistId ? { gistId: rawGistId } : {}),
      ...(filenames ? { filenames } : {}),
      ...(createPublic !== undefined ? { createPublic } : {})
    }

    const handler = HANDLERS[operation]
    const result = await handler(inputs, log)

    // `create` 操作会产生新的 id，其他操作则原样回显传入的 id。
    const outputGistId = result.newGistId ?? rawGistId
    core.setOutput('gist-id', outputGistId)
  } catch (err) {
    // 所有失败路径汇聚此处
    if (err instanceof Error) core.setFailed(err)
    else core.setFailed(String(err))
  }
}

// 重新导出该工具，以便单元测试在不深入 `utils.ts` 的前提下断言路径处理逻辑。
// 之所以放在这里（而不是在别处再 import）是因为测试需要与分发器使用同一条
// 代码路径。
export { basenameOf }
