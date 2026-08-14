import { IngstApiError, IngstClient } from "../src/index";

interface OrderCreated {
  orderId: number;
}

async function main() {
  const apiKey = process.env.INGST_API_KEY;
  if (!apiKey) throw new Error("INGST_API_KEY is required");

  const client = new IngstClient({
    baseUrl: process.env.INGST_URL ?? "http://localhost:3000",
    apiKey,
  });
  const signal = AbortSignal.timeout(10_000);
  const occurredAt = new Date();

  try {
    const event = await client.createEvent<OrderCreated>(
      {
        source: "checkout",
        type: "order.created",
        timestamp: occurredAt.toISOString(),
        payload: { orderId: 42 },
      },
      {
        idempotencyKey: `order-42-created-${occurredAt.getTime()}`,
        signal,
      },
    );

    console.log(`created order ${event.payload.orderId} at ${event.timestamp}`);

    for await (const current of client.iterateEvents<OrderCreated>({
      source: "checkout",
      type: "order.created",
      limit: 100,
      signal,
    })) {
      console.log(current.timestamp, current.type);
    }
  } catch (error) {
    if (error instanceof IngstApiError) {
      throw new Error(
        `ingst returned ${error.status} (request ${error.requestId ?? "unknown"}): ${error.message}`,
        { cause: error },
      );
    }
    throw error;
  }
}

await main();
