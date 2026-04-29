import { defineConfig } from "tsdown";

export default defineConfig({
  tsconfig: "./tsconfig.src.json",
  entry: ["src/index.ts"],
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  minify: false,
  outExtensions: () => ({ js: ".js", dts: ".d.ts" }),
});
