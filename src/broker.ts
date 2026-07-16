import { createHash } from "node:crypto";

import {
  type ExecuteEvent,
  type ExecuteEventInput,
  type ExecutionResult,
  parseExecutionResult,
  validateEventInput,
} from "./protocol";

export const MAX_RETAINED_EVENTS = 32;

export type DeliveryOutcome =
  | Readonly<{ kind: "success"; output?: string }>
  | Readonly<{ kind: "failure"; error: string }>
  | Readonly<{ kind: "timeout" }>
  | Readonly<{ kind: "no-listener" }>
  | Readonly<{ kind: "stopped" }>;

export interface EnqueuedEvent {
  readonly event: ExecuteEvent;
  readonly outcome: Promise<DeliveryOutcome>;
}

interface EventRecord {
  readonly event: ExecuteEvent;
  readonly resolveOutcome: (outcome: DeliveryOutcome) => void;
  readonly deadline: NodeJS.Timeout;
  delivered: boolean;
}

interface PendingPoll {
  readonly after: number;
  readonly finish: (event: ExecuteEvent | null) => void;
}

export class QueueFullError extends Error {
  public constructor() {
    super(`ScriptFerry already retains ${MAX_RETAINED_EVENTS} events`);
    this.name = "QueueFullError";
  }
}

export class ActivePollError extends Error {
  public constructor() {
    super("Another listener poll is already active");
    this.name = "ActivePollError";
  }
}

export class UnknownEventError extends Error {
  public constructor(public readonly eventId: number) {
    super(`Event ${eventId} is not awaiting a result`);
    this.name = "UnknownEventError";
  }
}

export class ResultConflictError extends Error {
  public constructor(public readonly eventId: number) {
    super(`Event ${eventId} already has a different result`);
    this.name = "ResultConflictError";
  }
}

export class BrokerStoppedError extends Error {
  public constructor() {
    super("The ScriptFerry broker has stopped");
    this.name = "BrokerStoppedError";
  }
}

export class EventBroker {
  private readonly records = new Map<number, EventRecord>();
  private readonly completedResultDigests = new Map<number, Buffer>();
  private nextEventId: number;
  private pendingPoll: PendingPoll | undefined;
  private stopped = false;

  public constructor(initialEventId = 1) {
    if (!Number.isSafeInteger(initialEventId) || initialEventId < 1) {
      throw new RangeError("initialEventId must be a positive safe integer");
    }
    this.nextEventId = initialEventId;
  }

  public get retainedEventCount(): number {
    return this.records.size;
  }

  public get hasActivePoll(): boolean {
    return this.pendingPoll !== undefined;
  }

  public get nextId(): number {
    return this.nextEventId;
  }

  public enqueue(input: ExecuteEventInput, executionTimeoutMs: number): EnqueuedEvent {
    if (this.stopped) {
      throw new BrokerStoppedError();
    }
    validateEventInput(input);
    if (!Number.isSafeInteger(executionTimeoutMs) || executionTimeoutMs < 1) {
      throw new RangeError("executionTimeoutMs must be a positive safe integer");
    }
    if (this.records.size >= MAX_RETAINED_EVENTS) {
      throw new QueueFullError();
    }
    if (!Number.isSafeInteger(this.nextEventId)) {
      throw new RangeError("The event ID space is exhausted");
    }

    const event: ExecuteEvent = Object.freeze({
      id: this.nextEventId,
      type: "execute",
      name: input.name,
      languageId: input.languageId,
      source: input.source,
    });
    this.nextEventId += 1;

    let resolveOutcome!: (outcome: DeliveryOutcome) => void;
    const outcome = new Promise<DeliveryOutcome>((resolve) => {
      resolveOutcome = resolve;
    });
    const deadline = setTimeout(() => {
      const record = this.records.get(event.id);
      if (record === undefined) {
        return;
      }
      this.records.delete(event.id);
      record.resolveOutcome(
        Object.freeze({ kind: record.delivered ? "timeout" : "no-listener" }),
      );
    }, executionTimeoutMs);

    this.records.set(event.id, {
      event,
      resolveOutcome,
      deadline,
      delivered: false,
    });
    this.fulfillPendingPoll();

    return Object.freeze({ event, outcome });
  }

  public waitForEvent(
    after: number,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<ExecuteEvent | null> {
    if (this.stopped) {
      return Promise.resolve(null);
    }
    if (!Number.isSafeInteger(after) || after < 0) {
      throw new RangeError("after must be a non-negative safe integer");
    }
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) {
      throw new RangeError("timeoutMs must be a positive safe integer");
    }
    if (signal?.aborted === true) {
      return Promise.resolve(null);
    }
    if (this.pendingPoll !== undefined) {
      throw new ActivePollError();
    }

    const available = this.oldestEventAfter(after);
    if (available !== undefined) {
      const record = this.records.get(available.id);
      if (record !== undefined) {
        record.delivered = true;
      }
      return Promise.resolve(available);
    }

    return new Promise<ExecuteEvent | null>((resolve) => {
      let timeout: NodeJS.Timeout;
      const abort = (): void => finish(null);
      const finish = (event: ExecuteEvent | null): void => {
        if (this.pendingPoll?.finish !== finish) {
          return;
        }
        this.pendingPoll = undefined;
        clearTimeout(timeout);
        signal?.removeEventListener("abort", abort);
        if (event !== null) {
          const record = this.records.get(event.id);
          if (record !== undefined) {
            record.delivered = true;
          }
        }
        resolve(event);
      };

      timeout = setTimeout(() => finish(null), timeoutMs);
      this.pendingPoll = { after, finish };
      signal?.addEventListener("abort", abort, { once: true });
    });
  }

  public submitResult(
    eventId: number,
    result: ExecutionResult,
  ): "accepted" | "duplicate" {
    if (!Number.isSafeInteger(eventId) || eventId < 1) {
      throw new UnknownEventError(eventId);
    }

    const validatedResult = parseExecutionResult(result);
    const completedDigest = this.completedResultDigests.get(eventId);
    if (completedDigest !== undefined) {
      const incomingDigest = digestResult(validatedResult);
      if (completedDigest.equals(incomingDigest)) {
        return "duplicate";
      }
      throw new ResultConflictError(eventId);
    }

    const record = this.records.get(eventId);
    if (record === undefined) {
      throw new UnknownEventError(eventId);
    }

    clearTimeout(record.deadline);
    this.records.delete(eventId);
    this.completedResultDigests.set(eventId, digestResult(validatedResult));
    if (validatedResult.success) {
      record.resolveOutcome(
        validatedResult.output === undefined
          ? Object.freeze({ kind: "success" })
          : Object.freeze({ kind: "success", output: validatedResult.output }),
      );
    } else {
      record.resolveOutcome(
        Object.freeze({ kind: "failure", error: validatedResult.error }),
      );
    }
    return "accepted";
  }

  public stop(): void {
    if (this.stopped) {
      return;
    }
    this.stopped = true;
    this.pendingPoll?.finish(null);
    for (const record of this.records.values()) {
      clearTimeout(record.deadline);
      record.resolveOutcome(Object.freeze({ kind: "stopped" }));
    }
    this.records.clear();
    this.completedResultDigests.clear();
  }

  private oldestEventAfter(after: number): ExecuteEvent | undefined {
    for (const record of this.records.values()) {
      if (record.event.id > after) {
        return record.event;
      }
    }
    return undefined;
  }

  private fulfillPendingPoll(): void {
    const poll = this.pendingPoll;
    if (poll === undefined) {
      return;
    }
    const event = this.oldestEventAfter(poll.after);
    if (event !== undefined) {
      poll.finish(event);
    }
  }
}

function digestResult(result: ExecutionResult): Buffer {
  const canonical = result.success
    ? JSON.stringify({ success: true, output: result.output ?? null })
    : JSON.stringify({ success: false, error: result.error });
  return createHash("sha256").update(canonical, "utf8").digest();
}
