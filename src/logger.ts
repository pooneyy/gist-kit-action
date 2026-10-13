/**
 * 受 `log-level` 输入控制的简易日志器
 *
 * 设计要点:
 *   - 通过 `core.*` 写日志，以保留 GitHub Actions 的输出格式与注释、
 *     grouping 等特性
 *   - 按阈值过滤 `debug / info / warn`，`error` 直接放行（`error` 几乎
 *     总是代表"调用方不会绕过我们的失败路径"，让它到达用户即可；若
 *     被吞掉，反而会误导排障）
 *   - 以工厂 + 闭包形式提供，让调用点直接调用 `log.debug(...)`，与
 *     `console.*` / `core.*` 的写法几乎一致
 */
import * as core from '@actions/core'
import type { ResolvedInputs } from './types.js'

export type LogLevel = ResolvedInputs['logLevel']

const ORDER = { debug: 10, info: 20, warn: 30, error: 40 } as const

export interface Logger {
  debug: (message: string) => void
  info: (message: string) => void
  warn: (message: string) => void
  error: (message: string) => void
}

/**
 * 构造一个受 `level` 阈值控制的 logger。
 *
 * 未识别的级别（理论上不会发生，因为 `resolveLogLevel` 已经归一化过）
 * 退化为 `info`，与 `resolveLogLevel` 的默认行为保持一致。
 */
export function makeLogger(level: LogLevel): Logger {
  const threshold = ORDER[level] ?? ORDER.info
  return {
    debug: (message: string) => {
      if (threshold <= ORDER.debug) core.debug(message)
    },
    info: (message: string) => {
      if (threshold <= ORDER.info) core.info(message)
    },
    warn: (message: string) => {
      if (threshold <= ORDER.warn) core.warning(message)
    },
    error: (message: string) => {
      // `error` 始终透传 —— 它影响 Actions 的 annotation，吞掉反而会
      // 误导排障。
      core.error(message)
    }
  }
}
