export const PROTOCOL_VERSION = 1 as const;
export const MAX_SCRIPT_BYTES = 1024 * 1024;
export const MAX_REQUEST_BODY_BYTES = 1024 * 1024;
export const MAX_RESULT_FIELD_BYTES = 64 * 1024;
export const MIN_POLL_TIMEOUT_MS = 1_000;
export const MAX_POLL_TIMEOUT_MS = 30_000;
export const DEFAULT_POLL_TIMEOUT_MS = 25_000;

const MAX_METADATA_BYTES = 255;
const DECIMAL_INTEGER = /^(?:0|[1-9]\d*)$/;
const POLL_PARAMETER_NAMES: Readonly<Record<string, true>> = Object.freeze({
  after: true,
  timeout: true,
  token: true,
  transport: true,
  nonce: true,
});
const SUCCESS_RESULT_FIELDS: Readonly<Record<string, true>> = Object.freeze({
  success: true,
  output: true,
});
const FAILURE_RESULT_FIELDS: Readonly<Record<string, true>> = Object.freeze({
  success: true,
  error: true,
});

export interface ExecuteEvent {
  readonly id: number;
  readonly type: "execute";
  readonly name: string;
  readonly languageId: string;
  readonly source: string;
}

export interface EventEnvelope {
  readonly protocol: typeof PROTOCOL_VERSION;
  readonly event: ExecuteEvent;
}

export interface ExecuteEventInput {
  readonly name: string;
  readonly languageId: string;
  readonly source: string;
}

export type ExecutionResult =
  | Readonly<{ success: true; output?: string }>
  | Readonly<{ success: false; error: string }>;

export interface PollParameters {
  readonly after: number;
  readonly timeoutMs: number;
  readonly gameHttpGetCompatibility: boolean;
}

export type ProtocolValidationCode = "invalid_field" | "payload_too_large";

export class ProtocolValidationError extends Error {
  public constructor(
    public readonly code: ProtocolValidationCode,
    message: string,
  ) {
    super(message);
    this.name = "ProtocolValidationError";
  }
}

export function utf8ByteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

export function parsePollParameters(parameters: URLSearchParams): PollParameters {
  rejectUnknownParameters(parameters, POLL_PARAMETER_NAMES);

  const after = parseSingleIntegerParameter(parameters, "after", true);
  const requestedTimeout = parseSingleIntegerParameter(parameters, "timeout", false);
  const transports = parameters.getAll("transport");
  parseSingleIntegerParameter(parameters, "nonce", false);
  if (transports.length > 1) {
    throw invalidField('Query parameter "transport" must appear once');
  }
  const transport = transports[0];
  if (transport !== undefined && transport !== "game-httpget") {
    throw invalidField('Query parameter "transport" is unsupported');
  }

  return Object.freeze({
    after,
    gameHttpGetCompatibility: transport === "game-httpget",
    timeoutMs:
      requestedTimeout === undefined
        ? DEFAULT_POLL_TIMEOUT_MS
        : Math.min(
            MAX_POLL_TIMEOUT_MS,
            Math.max(MIN_POLL_TIMEOUT_MS, requestedTimeout),
          ),
  });
}

export function parseEventId(value: string): number {
  return parseSafeInteger(value, "event id", 1);
}

export function parseExecutionResult(value: unknown): ExecutionResult {
  if (!isRecord(value)) {
    throw invalidField("Result body must be a JSON object");
  }

  const success = value.success;
  if (typeof success !== "boolean") {
    throw invalidField('Field "success" must be a boolean');
  }

  if (success) {
    rejectUnknownFields(value, SUCCESS_RESULT_FIELDS);
    const output = value.output;
    if (output !== undefined && typeof output !== "string") {
      throw invalidField('Field "output" must be a string');
    }
    if (output !== undefined) {
      assertFieldSize("output", output, MAX_RESULT_FIELD_BYTES);
      return Object.freeze({ success: true, output });
    }
    return Object.freeze({ success: true });
  }

  rejectUnknownFields(value, FAILURE_RESULT_FIELDS);
  if (typeof value.error !== "string") {
    throw invalidField('Field "error" must be a string');
  }
  assertFieldSize("error", value.error, MAX_RESULT_FIELD_BYTES);
  return Object.freeze({ success: false, error: value.error });
}

export function validateEventInput(input: ExecuteEventInput): void {
  assertNonEmptyMetadata("name", input.name);
  assertNonEmptyMetadata("languageId", input.languageId);
  if (typeof input.source !== "string") {
    throw invalidField('Field "source" must be a string');
  }
  assertFieldSize("source", input.source, MAX_SCRIPT_BYTES);
}

function parseSingleIntegerParameter(
  parameters: URLSearchParams,
  name: string,
  required: true,
): number;
function parseSingleIntegerParameter(
  parameters: URLSearchParams,
  name: string,
  required: false,
): number | undefined;
function parseSingleIntegerParameter(
  parameters: URLSearchParams,
  name: string,
  required: boolean,
): number | undefined {
  const values = parameters.getAll(name);
  if (values.length === 0) {
    if (required) {
      throw invalidField(`Missing query parameter "${name}"`);
    }
    return undefined;
  }
  if (values.length !== 1) {
    throw invalidField(`Query parameter "${name}" must appear once`);
  }
  return parseSafeInteger(values[0] ?? "", `query parameter "${name}"`, 0);
}

function parseSafeInteger(value: string, label: string, minimum: number): number {
  if (!DECIMAL_INTEGER.test(value)) {
    throw invalidField(`${label} must be a decimal integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < minimum) {
    throw invalidField(`${label} is outside the supported range`);
  }
  return parsed;
}

function rejectUnknownParameters(
  parameters: URLSearchParams,
  allowed: Readonly<Record<string, true>>,
): void {
  for (const key of parameters.keys()) {
    if (!Object.hasOwn(allowed, key)) {
      throw invalidField(`Unknown query parameter "${key}"`);
    }
  }
}

function rejectUnknownFields(
  value: Readonly<Record<string, unknown>>,
  allowed: Readonly<Record<string, true>>,
): void {
  for (const key of Object.keys(value)) {
    if (!Object.hasOwn(allowed, key)) {
      throw invalidField(`Unknown field "${key}"`);
    }
  }
}

function assertNonEmptyMetadata(name: string, value: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw invalidField(`Field "${name}" must be a non-empty string`);
  }
  assertFieldSize(name, value, MAX_METADATA_BYTES);
}

function assertFieldSize(name: string, value: string, maximum: number): void {
  if (utf8ByteLength(value) > maximum) {
    throw new ProtocolValidationError(
      "payload_too_large",
      `Field "${name}" exceeds ${maximum} UTF-8 bytes`,
    );
  }
}

function invalidField(message: string): ProtocolValidationError {
  return new ProtocolValidationError("invalid_field", message);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
