// src/client/logger.ts
//
// Lightweight internal logger used by OrionDB across the startup sequence
// and lifecycle methods. All output is gated by the configured minimum level.

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Severity levels supported by the OrionDB internal logger. */
export type LogLevel = "debug" | "info" | "warn" | "error";

/**
 * Minimal logger interface — one method per log level.
 * Each method accepts an optional `meta` object for structured context.
 */
export interface Logger {
  debug(message: string, meta?: Record<string, unknown>): void;
  info(message: string, meta?: Record<string, unknown>): void;
  warn(message: string, meta?: Record<string, unknown>): void;
  error(message: string, meta?: Record<string, unknown>): void;
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

/**
 * Numeric ordering of log levels, lowest to highest severity.
 * Used to determine whether a given log call should be emitted.
 */
const LOG_LEVELS: Record<LogLevel, number> = {
  debug: 0,
  info: 1,
  warn: 2,
  error: 3,
};

// ---------------------------------------------------------------------------
// createLogger
// ---------------------------------------------------------------------------

/**
 * Creates an `Logger` instance that emits messages at or above
 * the given `minLevel`.
 *
 * All output goes to `console.warn` (for debug/info/warn) and
 * `console.error` (for error) so no additional log transports are needed.
 * Output is prefixed with `[OrionDB:<level>]` for easy filtering.
 *
 * @param minLevel - Minimum level to emit. Default: `'warn'`.
 */
export const createLogger = (minLevel: LogLevel = "warn"): Logger => {
  const minN = LOG_LEVELS[minLevel];

  const emit = (level: LogLevel, message: string, meta?: Record<string, unknown>): void => {
    if (LOG_LEVELS[level] < minN) return;

    const prefix = `[OrionDB:${level}]`;
    const output = meta !== undefined ? `${prefix} ${message} ${JSON.stringify(meta)}` : `${prefix} ${message}`;

    if (level === "error") {
      // eslint-disable-next-line no-console
      console.error(output);
    } else {
      // eslint-disable-next-line no-console
      console.warn(output);
    }
  };

  return {
    debug: (message, meta) => emit("debug", message, meta),
    info: (message, meta) => emit("info", message, meta),
    warn: (message, meta) => emit("warn", message, meta),
    error: (message, meta) => emit("error", message, meta),
  };
};
