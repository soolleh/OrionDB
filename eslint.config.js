import js from "@eslint/js";
import tseslint from "typescript-eslint";
import prettier from "eslint-config-prettier";

export default [
  // Ignore patterns
  {
    ignores: ["dist/**", "node_modules/**", "coverage/**"],
  },
  // Base JS recommended rules
  js.configs.recommended,

  // TypeScript recommended configs
  ...tseslint.configs.recommendedTypeChecked,

  // Custom rules
  {
    files: ["**/*.ts", "**/*.tsx"],
    languageOptions: {
      parser: tseslint.parser,
      parserOptions: {
        project: ["./tsconfig.src.json", "./tsconfig.test.json"],
      },
    },
    rules: {
      // General
      "no-console": "warn",
      "@typescript-eslint/no-explicit-any": "warn",

      // TypeScript specific
      "@typescript-eslint/no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
      "@typescript-eslint/explicit-function-return-type": "off",
    },
  },

  // Disable ESLint rules that conflict with Prettier
  prettier,
];
