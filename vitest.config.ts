import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Test environment
    environment: "node",

    // Globals like describe, it, expect
    globals: true,

    // Test file patterns
    include: ["tests/unit/**/*.test.ts", "tests/integration/**/*.test.ts", "tests/smoke/**/*.test.ts"],

    // Coverage configuration
    coverage: {
      include: ["src/**/*.ts"],
      provider: "v8",
      reporter: ["text", "json"],
      exclude: ["node_modules/", "dist/", "build/", "lib/", "**/*.d.ts", "**/*.config.*"],
      thresholds: {
        lines: 80,
        functions: 80,
        branches: 80,
        statements: 80,
      },
    },

    // Clear mocks between tests
    clearMocks: true,
  },
});
