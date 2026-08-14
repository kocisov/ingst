export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface EventInput<Payload = JsonValue> {
  source: string;
  type: string;
  timestamp: string;
  payload: Payload;
}

export interface Event<Payload = JsonValue> extends EventInput<Payload> {}

export type Fetch = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export interface IngstClientOptions {
  baseUrl: string;
  apiKey: string;
  fetch?: Fetch;
  headers?: HeadersInit;
}

export interface RequestOptions {
  signal?: AbortSignal;
}

export interface CreateEventOptions extends RequestOptions {
  idempotencyKey?: string;
}

export interface ListEventsOptions extends RequestOptions {
  source?: string;
  type?: string;
  limit?: number;
  cursor?: string;
}

export interface ListEventsResponse<Payload = JsonValue> {
  events: Event<Payload>[];
  nextCursor?: string;
}

export interface StatusResponse {
  status: "ok";
}

export type ApiErrorBody = JsonValue | undefined;

export class IngstApiError extends Error {
  readonly status: number;
  readonly body: ApiErrorBody;
  readonly requestId?: string;

  constructor(status: number, body: ApiErrorBody, requestId?: string) {
    const message = getErrorMessage(body) ?? `ingst request failed with status ${status}`;
    super(message);
    this.name = "IngstApiError";
    this.status = status;
    this.body = body;
    this.requestId = requestId;
  }
}

export class IngstClient {
  readonly #baseUrl: URL;
  readonly #apiKey: string;
  readonly #fetch: Fetch;
  readonly #headers: Headers;

  constructor(options: IngstClientOptions) {
    if (!options.apiKey) {
      throw new TypeError("apiKey is required");
    }

    this.#baseUrl = new URL(ensureTrailingSlash(options.baseUrl));
    this.#apiKey = options.apiKey;
    this.#fetch = options.fetch ?? globalThis.fetch;
    this.#headers = new Headers(options.headers);
  }

  createEvent<Payload>(
    event: EventInput<Payload>,
    options: CreateEventOptions = {},
  ): Promise<Event<Payload>> {
    const headers = new Headers();
    headers.set("Content-Type", "application/json");
    if (options.idempotencyKey) headers.set("Idempotency-Key", options.idempotencyKey);

    return this.#request<Event<Payload>>(
      "events",
      {
        method: "POST",
        headers,
        body: JSON.stringify(event),
        signal: options.signal,
      },
      true,
    );
  }

  async listEvents<Payload = JsonValue>(
    options: ListEventsOptions = {},
  ): Promise<ListEventsResponse<Payload>> {
    if (options.limit !== undefined && (!Number.isInteger(options.limit) || options.limit < 1)) {
      throw new TypeError("limit must be a positive integer");
    }

    const search = new URLSearchParams();
    if (options.source !== undefined) {
      search.set("source", options.source);
    }
    if (options.type !== undefined) {
      search.set("type", options.type);
    }
    if (options.limit !== undefined) {
      search.set("limit", String(options.limit));
    }
    if (options.cursor !== undefined) {
      search.set("cursor", options.cursor);
    }
    const query = search.size === 0 ? "" : `?${search}`;

    return this.#request<WireListEventsResponse<Payload>>(
      `events${query}`,
      { signal: options.signal },
      true,
    ).then(({ events, next_cursor }) => {
      const page: ListEventsResponse<Payload> = { events };
      if (next_cursor !== undefined) {
        page.nextCursor = next_cursor;
      }
      return page;
    });
  }

  async *iterateEvents<Payload = JsonValue>(
    options: ListEventsOptions = {},
  ): AsyncGenerator<Event<Payload>, void, undefined> {
    let cursor = options.cursor;
    do {
      const page = await this.listEvents<Payload>({ ...options, cursor });
      yield* page.events;
      cursor = page.nextCursor;
    } while (cursor !== undefined);
  }

  health(options: RequestOptions = {}): Promise<StatusResponse> {
    return this.#request("health", { signal: options.signal }, false);
  }

  readiness(options: RequestOptions = {}): Promise<StatusResponse> {
    return this.#request("ready", { signal: options.signal }, false);
  }

  async #request<ResponseBody>(
    path: string,
    init: RequestInit,
    authenticated: boolean,
  ): Promise<ResponseBody> {
    const headers = new Headers(this.#headers);
    new Headers(init.headers).forEach((value, name) => headers.set(name, value));
    headers.set("Accept", "application/json");
    if (authenticated) headers.set("Authorization", `Bearer ${this.#apiKey}`);

    const response = await this.#fetch(new URL(path, this.#baseUrl), { ...init, headers });
    const text = await response.text();
    const body = text === "" ? undefined : parseResponseBody(text);
    if (!response.ok) {
      throw new IngstApiError(
        response.status,
        body,
        response.headers.get("X-Request-ID") ?? undefined,
      );
    }
    // SAFETY: Each SDK method selects ResponseBody from the documented schema of this endpoint.
    return body as ResponseBody;
  }
}

interface WireListEventsResponse<Payload> {
  events: Event<Payload>[];
  next_cursor?: string;
}

function ensureTrailingSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}

function parseResponseBody(value: string): JsonValue {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function getErrorMessage(body: ApiErrorBody): string | undefined {
  if (!(body instanceof Object) || Array.isArray(body) || !("error" in body)) {
    return undefined;
  }
  const error = body.error;
  return Object.prototype.toString.call(error) === "[object String]" ? String(error) : undefined;
}
