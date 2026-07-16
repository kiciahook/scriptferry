import { randomBytes } from "node:crypto";
import path from "node:path";

import * as vscode from "vscode";

import {
  type DeliveryOutcome,
  EventBroker,
  QueueFullError,
} from "./broker";
import { MAX_SCRIPT_BYTES, utf8ByteLength } from "./protocol";
import { ScriptFerryServer } from "./server";

const SECRET_KEY = "scriptFerry.authenticationToken";
const DEFAULT_PORT = 6767;
const DEFAULT_EXECUTION_TIMEOUT_MS = 30_000;
const DEFAULT_ALLOWED_LANGUAGE_IDS = Object.freeze(["lua", "luau"]);

interface RunningServer {
  readonly broker: EventBroker;
  readonly server: ScriptFerryServer;
  readonly port: number;
  readonly token: string;
}

export type SendCommandResult =
  | DeliveryOutcome
  | Readonly<{ kind: "rejected"; reason: string }>;

class PortUnavailableError extends Error {
  public constructor(public readonly port: number) {
    super(
      `Port ${port} is already in use. Change scriptFerry.port, then start ScriptFerry again.`,
    );
    this.name = "PortUnavailableError";
  }
}

class ExtensionController {
  private running: RunningServer | undefined;
  private starting: Promise<RunningServer> | undefined;
  private nextEventId = 1;

  public constructor(private readonly context: vscode.ExtensionContext) {}

  public registerCommands(): vscode.Disposable[] {
    return [
      vscode.commands.registerCommand("scriptferry.sendCurrentFile", () =>
        this.sendCurrentFile(),
      ),
      vscode.commands.registerCommand("scriptferry.startServer", () =>
        this.startFromCommand(),
      ),
      vscode.commands.registerCommand("scriptferry.stopServer", () =>
        this.stopFromCommand(),
      ),
      vscode.commands.registerCommand(
        "scriptferry.copyConnectionSnippet",
        () => this.copyConnectionSnippet(),
      ),
    ];
  }

  public async startAutomatically(): Promise<void> {
    try {
      await this.ensureServer();
    } catch (error) {
      this.showStartError(error);
    }
  }

  public onConfigurationChanged(event: vscode.ConfigurationChangeEvent): void {
    if (!event.affectsConfiguration("scriptFerry.port")) {
      return;
    }
    const running = this.running;
    if (running === undefined) {
      return;
    }
    const configuredPort = this.readPort();
    if (configuredPort !== running.port) {
      void vscode.window.showWarningMessage(
        `ScriptFerry is still listening on port ${running.port}. The new port ${configuredPort} takes effect after Stop Server and Start Server.`,
      );
    }
  }

  public async shutdown(): Promise<void> {
    let running = this.running;
    if (running === undefined && this.starting !== undefined) {
      try {
        running = await this.starting;
      } catch {
        return;
      }
    }
    if (running === undefined) {
      return;
    }
    this.running = undefined;
    this.nextEventId = running.broker.nextId;
    await running.server.stop();
  }

  private async startFromCommand(): Promise<number | undefined> {
    const wasRunning = this.running !== undefined;
    try {
      const running = await this.ensureServer();
      void vscode.window.showInformationMessage(
        wasRunning
          ? `ScriptFerry is already listening on 127.0.0.1:${running.port}.`
          : `ScriptFerry started on 127.0.0.1:${running.port}.`,
      );
      return running.port;
    } catch (error) {
      this.showStartError(error);
      return undefined;
    }
  }

  private async stopFromCommand(): Promise<boolean> {
    if (this.running === undefined && this.starting === undefined) {
      void vscode.window.showInformationMessage("ScriptFerry is already stopped.");
      return false;
    }
    await this.shutdown();
    void vscode.window.showInformationMessage("ScriptFerry stopped.");
    return true;
  }

  private async copyConnectionSnippet(): Promise<boolean> {
    let running: RunningServer;
    try {
      running = await this.ensureServer();
    } catch (error) {
      this.showStartError(error);
      return false;
    }

    const snippet = [
      "local CONFIG = {",
      `    baseUrl = \"http://127.0.0.1:${running.port}\",`,
      `    token = \"${running.token}\",`,
      "    silentMode = true,",
      "    pollTimeoutMs = 25_000,",
      "    retryDelaySeconds = 1,",
      "}",
    ].join("\n");
    await vscode.env.clipboard.writeText(snippet);
    void vscode.window.showInformationMessage(
      "ScriptFerry connection snippet copied to the clipboard.",
    );
    return true;
  }

  private async sendCurrentFile(): Promise<SendCommandResult> {
    const editor = vscode.window.activeTextEditor;
    if (editor === undefined) {
      const reason = "Open a Lua or Luau editor before sending.";
      void vscode.window.showWarningMessage(`ScriptFerry: ${reason}`);
      return Object.freeze({ kind: "rejected", reason });
    }

    const document = editor.document;
    const configuration = vscode.workspace.getConfiguration("scriptFerry");
    const allowedLanguageIds = configuration.get<readonly string[]>(
      "allowedLanguageIds",
      DEFAULT_ALLOWED_LANGUAGE_IDS,
    );
    if (!allowedLanguageIds.includes(document.languageId)) {
      const reason = `Language '${document.languageId}' is not allowed by scriptFerry.allowedLanguageIds.`;
      void vscode.window.showWarningMessage(`ScriptFerry: ${reason}`);
      return Object.freeze({ kind: "rejected", reason });
    }

    const source = document.getText();
    if (utf8ByteLength(source) > MAX_SCRIPT_BYTES) {
      const reason = `The editor snapshot exceeds the ${MAX_SCRIPT_BYTES}-byte source limit.`;
      void vscode.window.showErrorMessage(`ScriptFerry: ${reason}`);
      return Object.freeze({ kind: "rejected", reason });
    }
    const name = path.basename(document.fileName) || "Untitled";

    let running: RunningServer;
    try {
      running = await this.ensureServer();
    } catch (error) {
      this.showStartError(error);
      return Object.freeze({ kind: "rejected", reason: "server-start-failed" });
    }

    const configuredTimeout = configuration.get<number>(
      "executionTimeoutMs",
      DEFAULT_EXECUTION_TIMEOUT_MS,
    );
    const executionTimeoutMs =
      Number.isSafeInteger(configuredTimeout) && configuredTimeout > 0
        ? configuredTimeout
        : DEFAULT_EXECUTION_TIMEOUT_MS;

    let outcome: Promise<DeliveryOutcome>;
    let eventId: number;
    try {
      const enqueued = running.broker.enqueue(
        { name, languageId: document.languageId, source },
        executionTimeoutMs,
      );
      outcome = enqueued.outcome;
      eventId = enqueued.event.id;
    } catch (error) {
      if (error instanceof QueueFullError) {
        const reason = "The 32-event queue is full; wait for a result or timeout.";
        void vscode.window.showErrorMessage(`ScriptFerry: ${reason}`);
        return Object.freeze({ kind: "rejected", reason });
      }
      const reason = "The editor snapshot could not be queued.";
      void vscode.window.showErrorMessage(`ScriptFerry: ${reason}`);
      return Object.freeze({ kind: "rejected", reason });
    }

    const status = vscode.window.setStatusBarMessage(
      `$(sync~spin) ScriptFerry: waiting for result for ${name} (event ${eventId})`,
    );
    try {
      const result = await outcome;
      this.showDeliveryOutcome(name, result);
      return result;
    } finally {
      status.dispose();
    }
  }

  private async ensureServer(): Promise<RunningServer> {
    if (this.running !== undefined) {
      return this.running;
    }
    if (this.starting !== undefined) {
      return await this.starting;
    }

    const starting = this.createServer();
    this.starting = starting;
    try {
      const running = await starting;
      this.running = running;
      return running;
    } finally {
      if (this.starting === starting) {
        this.starting = undefined;
      }
    }
  }

  private async createServer(): Promise<RunningServer> {
    const port = this.readPort();
    let token = await this.context.secrets.get(SECRET_KEY);
    if (token === undefined || token.length === 0) {
      token = randomBytes(32).toString("base64url");
      await this.context.secrets.store(SECRET_KEY, token);
    }

    const broker = new EventBroker(this.nextEventId);
    const server = new ScriptFerryServer({ port, token, broker });
    try {
      await server.start();
    } catch (error) {
      broker.stop();
      if (
        typeof error === "object" &&
        error !== null &&
        "code" in error &&
        error.code === "EADDRINUSE"
      ) {
        throw new PortUnavailableError(port);
      }
      throw error;
    }
    return Object.freeze({ broker, server, port, token });
  }

  private readPort(): number {
    const port = vscode.workspace
      .getConfiguration("scriptFerry")
      .get<number>("port", DEFAULT_PORT);
    return Number.isSafeInteger(port) && port >= 1_024 && port <= 65_535
      ? port
      : DEFAULT_PORT;
  }

  private showStartError(error: unknown): void {
    const message =
      error instanceof PortUnavailableError
        ? error.message
        : "The loopback server could not be started.";
    void vscode.window.showErrorMessage(`ScriptFerry: ${message}`);
  }

  private showDeliveryOutcome(name: string, outcome: DeliveryOutcome): void {
    switch (outcome.kind) {
      case "success": {
        const output = outcome.output?.trim();
        const suffix =
          output === undefined || output.length === 0
            ? ""
            : ` Output: ${this.truncateMessage(output)}`;
        void vscode.window.showInformationMessage(
          `ScriptFerry: ${name} executed successfully.${suffix}`,
        );
        break;
      }
      case "failure":
        void vscode.window.showErrorMessage(
          `ScriptFerry: ${name} failed: ${this.truncateMessage(outcome.error)}`,
        );
        break;
      case "timeout":
        void vscode.window.showWarningMessage(
          `ScriptFerry: ${name} was delivered, but no result arrived before the execution timeout.`,
        );
        break;
      case "no-listener":
        void vscode.window.showWarningMessage(
          `ScriptFerry: no listener received ${name} before the execution timeout.`,
        );
        break;
      case "stopped":
        void vscode.window.showWarningMessage(
          `ScriptFerry: delivery of ${name} stopped with the server.`,
        );
        break;
    }
  }

  private truncateMessage(message: string): string {
    const singleLine = message.replace(/\s+/g, " ").trim();
    return singleLine.length <= 240
      ? singleLine
      : `${singleLine.slice(0, 237)}...`;
  }
}

let controller: ExtensionController | undefined;

export async function activate(
  context: vscode.ExtensionContext,
): Promise<void> {
  const activeController = new ExtensionController(context);
  controller = activeController;
  context.subscriptions.push(
    ...activeController.registerCommands(),
    vscode.workspace.onDidChangeConfiguration((event) => {
      activeController.onConfigurationChanged(event);
    }),
  );

  const autoStart = vscode.workspace
    .getConfiguration("scriptFerry")
    .get<boolean>("autoStart", false);
  if (autoStart) {
    await activeController.startAutomatically();
  }
}

export async function deactivate(): Promise<void> {
  const activeController = controller;
  controller = undefined;
  await activeController?.shutdown();
}
