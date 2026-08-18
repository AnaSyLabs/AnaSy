import { Consumer, Kafka } from "kafkajs";
import { kafkaBrokers, kafkaTopics, type FatEvent } from "./env";

const MAX = 200;

type Buffer = {
  recent: FatEvent[];
  byId: Map<string, FatEvent>;
  ok: boolean;
  error?: string;
};

const g = globalThis as unknown as { __anasKafka?: { buffer: Buffer; started: boolean } };

function state() {
  if (!g.__anasKafka) {
    g.__anasKafka = {
      started: false,
      buffer: { recent: [], byId: new Map(), ok: false },
    };
  }
  return g.__anasKafka;
}

export function kafkaSnapshot() {
  return state().buffer;
}

export async function ensureKafkaConsumer() {
  const s = state();
  if (s.started) return;
  s.started = true;
  const kafka = new Kafka({ clientId: "anas-viewer", brokers: kafkaBrokers() });
  const consumer: Consumer = kafka.consumer({ groupId: "anas-viewer" });
  try {
    await consumer.connect();
    for (const topic of kafkaTopics()) {
      await consumer.subscribe({ topic, fromBeginning: true });
    }
    s.buffer.ok = true;
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        const event_id = message.key?.toString() || "";
        const payload = message.value?.toString() || "";
        const row: FatEvent = {
          event_id,
          topic,
          kafka_offset: Number(message.offset),
          payload,
          source: "kafka",
        };
        if (event_id) s.buffer.byId.set(event_id, row);
        s.buffer.recent = [row, ...s.buffer.recent].slice(0, MAX);
      },
    });
  } catch (e) {
    s.buffer.ok = false;
    s.buffer.error = e instanceof Error ? e.message : String(e);
    s.started = false;
  }
}
