import { describe, expect, test } from "bun:test";
import { IngstApiError, IngstClient, type Event, type Fetch } from "./index";

const apiKey = "ingst_test_key";

describe("IngstClient", () => {
  test("creates an event with authentication and idempotency", async () => {
    const requests: Request[] = [];
    const client = createClient(async (input, init) => {
      const request = new Request(input, init);
      requests.push(request);
      return Response.json(await request.json(), { status: 201 });
    });

    const event = await client.createEvent(
      {
        source: "checkout",
        type: "order.created",
        timestamp: "2026-08-14T10:00:00Z",
        payload: {
          orderId: 42,
        },
      },
      { idempotencyKey: "order-42-created" },
    );

    expect(event.payload).toEqual({ orderId: 42 });
    expect(requests[0]?.headers.get("Authorization")).toBe(`Bearer ${apiKey}`);
    expect(requests[0]?.headers.get("Idempotency-Key")).toBe("order-42-created");
  });

  test("maps list parameters and pagination response", async () => {
    let request: Request | undefined;
    const client = createClient(async (input, init) => {
      request = new Request(input, init);
      return Response.json({ events: [], next_cursor: "next-page" });
    });

    const page = await client.listEvents({
      source: "checkout",
      type: "order.created",
      limit: 50,
      cursor: "current-page",
    });

    expect(request?.url).toBe(
      "https://ingst.example/v1/events?source=checkout&type=order.created&limit=50&cursor=current-page",
    );
    expect(page).toEqual({ events: [], nextCursor: "next-page" });
  });

  test("iterates through all pages", async () => {
    const pages = [
      { events: [event("first")], next_cursor: "page-2" },
      { events: [event("second")] },
    ];
    const client = createClient(async () => Response.json(pages.shift()));

    const events: Event[] = [];
    for await (const item of client.iterateEvents({ limit: 1 })) events.push(item);

    expect(events.map((item) => item.type)).toEqual(["first", "second"]);
  });

  test("does not authenticate health checks", async () => {
    let request: Request | undefined;
    const client = createClient(async (input, init) => {
      request = new Request(input, init);
      return Response.json({ status: "ok" });
    });

    await client.health();
    expect(request?.headers.has("Authorization")).toBe(false);
  });

  test("throws typed API errors", async () => {
    const client = createClient(async () =>
      Response.json(
        { error: "unauthorized" },
        { status: 401, headers: { "X-Request-ID": "request-1" } },
      ),
    );

    try {
      await client.listEvents();
      throw new Error("expected request to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(IngstApiError);
      expect(error).toMatchObject({
        message: "unauthorized",
        status: 401,
        requestId: "request-1",
      });
    }
  });
});

function createClient(fetch: Fetch): IngstClient {
  return new IngstClient({ baseUrl: "https://ingst.example/v1", apiKey, fetch });
}

function event(type: string): Event {
  return {
    source: "test",
    type,
    timestamp: "2026-08-14T10:00:00.000Z",
    payload: null,
  };
}
