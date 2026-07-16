import { timingSafeEqual } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";

import {
  ActivePollError,
  EventBroker,
  ResultConflictError,
  UnknownEventError,
} from "./broker";
import {
  MAX_REQUEST_BODY_BYTES,
  type ExecutionResult,
  PROTOCOL_VERSION,
  ProtocolValidationError,
  parseEventId,
  parseExecutionResult,
  parsePollParameters,
} from "./protocol";

export const LOOPBACK_HOST = "127.0.0.1";

const RESULT_PATH = /^\/v1\/events\/(\d+)\/result$/;
const JSON_CONTENT_TYPE = /^application\/json(?:\s*;\s*charset\s*=\s*utf-8)?$/i;
const DECIMAL_INTEGER = /^(?:0|[1-9]\d*)$/;
const POST_RESULT_QUERY_PARAMETERS: Readonly<Record<string, true>> = Object.freeze({
  token: true,
});
const GET_RESULT_QUERY_PARAMETERS: Readonly<Record<string, true>> = Object.freeze({
  token: true,
  success: true,
  output: true,
  error: true,
  nonce: true,
});

interface ErrorBody {
  readonly error: Readonly<{
    code: string;
    message: string;
  }>;
}

class RequestBodyTooLargeError extends Error {
  public constructor() {
    super(`Request body exceeds ${MAX_REQUEST_BODY_BYTES} bytes`);
    this.name = "RequestBodyTooLargeError";
  }
}

class RequestAbortedError extends Error {
  public constructor() {
    super("Request aborted");
    this.name = "RequestAbortedError";
  }
}

export interface ScriptFerryServerOptions {
  readonly port: number;
  readonly token: string;
  readonly broker: EventBroker;
}

export class ScriptFerryServer {
  private server: Server | undefined;

  public constructor(private readonly options: ScriptFerryServerOptions) {
    if (
      !Number.isSafeInteger(options.port) ||
      options.port < 0 ||
      options.port > 65_535
    ) {
      throw new RangeError("port must be an integer from 0 through 65535");
    }
    if (options.token.length === 0) {
      throw new RangeError("token must not be empty");
    }
  }

  public get isRunning(): boolean {
    return this.server?.listening === true;
  }

  public get port(): number | undefined {
    const address = this.server?.address();
    return typeof address === "object" && address !== null
      ? (address as AddressInfo).port
      : undefined;
  }

  public get host(): typeof LOOPBACK_HOST {
    return LOOPBACK_HOST;
  }

  public async start(): Promise<void> {
    if (this.server !== undefined) {
      if (this.server.listening) {
        return;
      }
      throw new Error("ScriptFerry server is already starting or stopping");
    }

    const server = createServer((request, response) => {
      void this.routeRequest(request, response).catch((error: unknown) => {
        this.respondToError(response, error);
      });
    });
    server.maxHeadersCount = 32;
    server.headersTimeout = 10_000;
    server.requestTimeout = 35_000;
    this.server = server;

    try {
      await new Promise<void>((resolve, reject) => {
        const onError = (error: Error): void => {
          server.off("listening", onListening);
          reject(error);
        };
        const onListening = (): void => {
          server.off("error", onError);
          resolve();
        };
        server.once("error", onError);
        server.once("listening", onListening);
        server.listen(this.options.port, LOOPBACK_HOST);
      });
    } catch (error) {
      this.server = undefined;
      throw error;
    }
  }

  public async stop(): Promise<void> {
    this.options.broker.stop();
    const server = this.server;
    this.server = undefined;
    if (server === undefined || !server.listening) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error !== undefined) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  private async routeRequest(
    request: IncomingMessage,
    response: ServerResponse,
  ): Promise<void> {
    let url: URL;
    try {
      url = new URL(request.url ?? "", `http://${LOOPBACK_HOST}`);
    } catch {
      this.sendError(response, 400, "malformed_request", "Malformed request URL");
      return;
    }

    if (request.method === "GET" && url.pathname === "/health") {
      if (url.search !== "") {
        this.sendError(
          response,
          400,
          "invalid_query",
          "Health endpoint does not accept query parameters",
        );
        return;
      }
      if (!this.requireEmptyBody(request, response)) {
        return;
      }
      this.sendJson(response, 200, { status: "ok", protocol: PROTOCOL_VERSION });
      return;
    }

    if (request.method === "GET" && url.pathname === "/v1/events") {
      if (!this.authenticate(request, url.searchParams)) {
        this.sendError(
          response,
          401,
          "unauthorized",
          "Invalid authentication token",
        );
        return;
      }
      if (!this.requireEmptyBody(request, response)) {
        return;
      }
      const parameters = parsePollParameters(url.searchParams);
      await this.handlePoll(
        request,
        response,
        parameters.after,
        parameters.timeoutMs,
        parameters.gameHttpGetCompatibility,
      );
      return;
    }

    const resultMatch = RESULT_PATH.exec(url.pathname);
    if (
      resultMatch !== null &&
      (request.method === "POST" || request.method === "GET")
    ) {
      if (!this.authenticate(request, url.searchParams)) {
        this.sendError(
          response,
          401,
          "unauthorized",
          "Invalid authentication token",
        );
        return;
      }

      const eventId = parseEventId(resultMatch[1] ?? "");
      let result: ExecutionResult;
      if (request.method === "GET") {
        if (!this.requireEmptyBody(request, response)) {
          return;
        }
        result = this.parseGameHttpGetResult(url.searchParams);
      } else {
        this.validateResultQuery(
          url.searchParams,
          POST_RESULT_QUERY_PARAMETERS,
        );
        const contentType = request.headers["content-type"];
        if (
          typeof contentType !== "string" ||
          !JSON_CONTENT_TYPE.test(contentType)
        ) {
          this.sendError(
            response,
            415,
            "unsupported_content_type",
            "Content-Type must be application/json; charset=utf-8",
          );
          return;
        }

        const body = await this.readRequestBody(request);
        let decoded: unknown;
        try {
          decoded = JSON.parse(body.toString("utf8")) as unknown;
        } catch {
          this.sendError(response, 400, "malformed_json", "Malformed JSON body");
          return;
        }
        result = parseExecutionResult(decoded);
      }

      const disposition = this.options.broker.submitResult(eventId, result);
      this.sendJson(response, 200, { status: disposition });
      return;
    }

    this.sendError(response, 404, "not_found", "Route not found");
  }

  private async handlePoll(
    request: IncomingMessage,
    response: ServerResponse,
    after: number,
    timeoutMs: number,
    gameHttpGetCompatibility: boolean,
  ): Promise<void> {
    const controller = new AbortController();
    const abort = (): void => controller.abort();
    request.once("aborted", abort);
    request.socket.once("close", abort);
    response.once("close", abort);

    try {
      const event = await this.options.broker.waitForEvent(
        after,
        timeoutMs,
        controller.signal,
      );
      if (controller.signal.aborted || response.destroyed) {
        return;
      }
      if (event === null) {
        if (gameHttpGetCompatibility) {
          this.sendJson(response, 200, {
            protocol: PROTOCOL_VERSION,
            timeout: true,
          });
        } else {
          this.sendNoContent(response);
        }
        return;
      }
      this.sendJson(response, 200, {
        protocol: PROTOCOL_VERSION,
        event,
      });
    } finally {
      request.off("aborted", abort);
      request.socket.off("close", abort);
      response.off("close", abort);
    }
  }

  private authenticate(
    request: IncomingMessage,
    parameters: URLSearchParams,
  ): boolean {
    const queryTokens = parameters.getAll("token");
    if (queryTokens.length > 1) {
      return false;
    }

    const authorization = request.headers.authorization;
    let candidate: string | undefined;
    if (authorization !== undefined) {
      if (!authorization.startsWith("Bearer ")) {
        return false;
      }
      candidate = authorization.slice("Bearer ".length);
      if (
        candidate.length === 0 ||
        (queryTokens.length === 1 && queryTokens[0] !== candidate)
      ) {
        return false;
      }
    } else {
      candidate = queryTokens[0];
    }

    if (candidate === undefined) {
      return false;
    }
    const actual = Buffer.from(candidate, "utf8");
    const expected = Buffer.from(this.options.token, "utf8");
    return actual.length === expected.length && timingSafeEqual(actual, expected);
  }

  private validateResultQuery(
    parameters: URLSearchParams,
    allowed: Readonly<Record<string, true>>,
  ): void {
    for (const key of parameters.keys()) {
      if (!Object.hasOwn(allowed, key)) {
        throw new ProtocolValidationError(
          "invalid_field",
          `Unknown query parameter "${key}"`,
        );
      }
    }
  }

  private parseGameHttpGetResult(
    parameters: URLSearchParams,
  ): ExecutionResult {
    this.validateResultQuery(parameters, GET_RESULT_QUERY_PARAMETERS);
    const successes = parameters.getAll("success");
    const outputs = parameters.getAll("output");
    const errors = parameters.getAll("error");
    const nonces = parameters.getAll("nonce");
    if (
      nonces.length > 1 ||
      (nonces[0] !== undefined &&
        (!DECIMAL_INTEGER.test(nonces[0]) ||
          !Number.isSafeInteger(Number(nonces[0]))))
    ) {
      throw new ProtocolValidationError(
        "invalid_field",
        'Query parameter "nonce" must be a safe decimal integer',
      );
    }
    if (successes.length !== 1) {
      throw new ProtocolValidationError(
        "invalid_field",
        'Query parameter "success" must appear once',
      );
    }
    if (outputs.length > 1 || errors.length > 1) {
      throw new ProtocolValidationError(
        "invalid_field",
        "Result query fields must appear at most once",
      );
    }
    if (successes[0] !== "true" && successes[0] !== "false") {
      throw new ProtocolValidationError(
        "invalid_field",
        'Query parameter "success" must be true or false',
      );
    }

    const decoded: Record<string, unknown> = {
      success: successes[0] === "true",
    };
    if (outputs[0] !== undefined) {
      decoded.output = outputs[0];
    }
    if (errors[0] !== undefined) {
      decoded.error = errors[0];
    }
    return parseExecutionResult(decoded);
  }

  private requireEmptyBody(
    request: IncomingMessage,
    response: ServerResponse,
  ): boolean {
    const contentLength = request.headers["content-length"];
    if (
      request.headers["transfer-encoding"] !== undefined ||
      (contentLength !== undefined && contentLength !== "0")
    ) {
      this.sendError(
        response,
        400,
        "unexpected_body",
        "This endpoint does not accept a request body",
      );
      return false;
    }
    return true;
  }

  private async readRequestBody(request: IncomingMessage): Promise<Buffer> {
    const declaredLength = request.headers["content-length"];
    if (
      declaredLength !== undefined &&
      Number.parseInt(declaredLength, 10) > MAX_REQUEST_BODY_BYTES
    ) {
      request.resume();
      throw new RequestBodyTooLargeError();
    }

    return await new Promise<Buffer>((resolve, reject) => {
      const chunks: Buffer[] = [];
      let byteLength = 0;
      let settled = false;

      const cleanup = (): void => {
        request.off("data", onData);
        request.off("end", onEnd);
        request.off("aborted", onAborted);
        request.off("error", onError);
      };
      const fail = (error: Error): void => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        request.resume();
        reject(error);
      };
      const onData = (chunk: Buffer): void => {
        byteLength += chunk.length;
        if (byteLength > MAX_REQUEST_BODY_BYTES) {
          fail(new RequestBodyTooLargeError());
          return;
        }
        chunks.push(chunk);
      };
      const onEnd = (): void => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        resolve(Buffer.concat(chunks, byteLength));
      };
      const onAborted = (): void => fail(new RequestAbortedError());
      const onError = (error: Error): void => fail(error);

      request.on("data", onData);
      request.once("end", onEnd);
      request.once("aborted", onAborted);
      request.once("error", onError);
    });
  }

  private respondToError(response: ServerResponse, error: unknown): void {
    if (response.writableEnded || response.destroyed) {
      return;
    }
    if (error instanceof RequestAbortedError) {
      return;
    }
    if (error instanceof RequestBodyTooLargeError) {
      this.sendError(
        response,
        413,
        "payload_too_large",
        "Request body exceeds the allowed size",
      );
      return;
    }
    if (error instanceof ProtocolValidationError) {
      const status = error.code === "payload_too_large" ? 413 : 400;
      this.sendError(response, status, error.code, error.message);
      return;
    }
    if (error instanceof ActivePollError) {
      this.sendError(response, 409, "poll_conflict", error.message);
      return;
    }
    if (error instanceof UnknownEventError) {
      this.sendError(response, 404, "unknown_event", "Unknown event");
      return;
    }
    if (error instanceof ResultConflictError) {
      this.sendError(
        response,
        409,
        "result_conflict",
        "Event already has a different result",
      );
      return;
    }
    this.sendError(
      response,
      500,
      "internal_error",
      "Unexpected internal server failure",
    );
  }

  private sendJson(response: ServerResponse, status: number, value: unknown): void {
    const body = Buffer.from(JSON.stringify(value), "utf8");
    response.writeHead(status, {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
      "Content-Length": body.length,
    });
    response.end(body);
  }

  private sendNoContent(response: ServerResponse): void {
    response.writeHead(204, { "Cache-Control": "no-store" });
    response.end();
  }

  private sendError(
    response: ServerResponse,
    status: number,
    code: string,
    message: string,
  ): void {
    const body: ErrorBody = Object.freeze({
      error: Object.freeze({ code, message }),
    });
    this.sendJson(response, status, body);
  }
}
