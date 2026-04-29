import { createOrionDB } from "../src/client/index.js";

const db = createOrionDB({
  dbLocation: "./scratch/mydb",
  logLevel: "debug",
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
console.log("✅ Connected");

// CREATE a user
const alice = await db.user.create({
  data: {
    name: "Alice",
    email: "alice@example.com",
    age: 30,
  },
});

console.log("Created user:", alice);
console.log("Has id?", typeof alice.id === "string");
console.log("No _deleted?", !("_deleted" in alice));
console.log("No _createdAt?", !("_createdAt" in alice));

await db.$disconnect();
console.log("✅ Disconnected");
