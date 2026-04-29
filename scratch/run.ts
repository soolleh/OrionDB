import { createOrionDB } from "./src/client/index.js";

const db = createOrionDB({
  dbLocation: "./scratch/mydb",
  logLevel: "debug",
  schema: {
    models: {
      User: {
        fields: {
          id: { type: "string", primary: true, default: () => crypto.randomUUID() },
          name: { type: "string", required: true },
          email: { type: "string", unique: true },
          age: { type: "number" },
        },
        relations: {
          posts: { type: "one-to-many", model: "Post", foreignKey: "authorId" },
        },
      },
      Post: {
        fields: {
          id: { type: "string", primary: true, default: () => crypto.randomUUID() },
          title: { type: "string", required: true },
          content: { type: "string" },
          authorId: { type: "string", required: true },
        },
        relations: {
          author: { type: "many-to-one", model: "User", foreignKey: "authorId" },
        },
      },
    },
  },
});

await db.$connect();
console.log("✅ Connected");

await db.$disconnect();
console.log("✅ Disconnected");
