import { createOrionDB } from "../src/client/index.js";

const db = createOrionDB({
  dbLocation: "./scratch/mydb",
  logLevel: "info",
  schema: {
    User: {
      id: { type: "string", primary: true, default: () => crypto.randomUUID() },
      name: { type: "string", required: true },
      email: { type: "string", unique: true },
      age: { type: "number" },
      posts: { type: "relation", model: "Post", foreignKey: "authorId", relation: "one-to-many" },
    },
    Post: {
      id: { type: "string", primary: true, default: () => crypto.randomUUID() },
      title: { type: "string", required: true },
      content: { type: "string" },
      authorId: { type: "string", required: true },
      author: { type: "relation", model: "User", foreignKey: "authorId", relation: "many-to-one" },
    },
  },
});

await db.$connect();
await db.$disconnect();
console.log("--- Disconnected ---");

// Reconnect with same dbLocation — no inline schema needed if _schema.json exists
// For now re-pass the schema
const db2 = createOrionDB({
  dbLocation: "./scratch/mydb",
  logLevel: "info",
});

await db2.$connect();
console.log("--- Reconnected ---");
// Alice should still be findable after full restart
const aliceAfterRestart = await db2.user.findUnique({
  where: { email: "alice@example.com" },
});
console.log("Alice survived restart:", aliceAfterRestart?.name === "Alice" ? "✅" : "❌");
console.log("Alice age after update survived:", aliceAfterRestart?.age === 31 ? "✅" : "❌");

// Bob should still be gone
const bobAfterRestart = await db2.user.findUnique({
  where: { email: "bob@example.com" },
});
console.log("Bob still gone after restart:", bobAfterRestart === null ? "✅" : "❌");

await db2.$disconnect();
