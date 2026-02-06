import { defineConfig } from "vitest/config";

// Environment variables are loaded via env-cmd in package.json scripts
// Set environment variables for testcontainers
if (process.env.TEST_DOCKER_HOST) {
  process.env.DOCKER_HOST = process.env.TEST_DOCKER_HOST;
}
if (process.env.TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE) {
  process.env.TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE =
    process.env.TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE;
}

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["src/**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}"],
    exclude: ["node_modules", "dist", ".idea", ".git", ".cache"],
    // Run tests sequentially to avoid database connection conflicts
    sequence: {
      shuffle: false,
      concurrent: false,
    },
    // Increase hook timeout for testcontainer startup and migrations
    hookTimeout: 60000,
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: [
        "coverage/**",
        "dist/**",
        "**/[.]**",
        "packages/*/test?(s)/**",
        "**/*.d.ts",
        "**/virtual:*",
        "**/__x00__*",
        "**/\x00*",
        "cypress/**",
        "test?(s)/**",
        "test?(-*).?(c|m)[jt]s?(x)",
        "**/*{.,-}{test,spec}.?(c|m)[jt]s?(x)",
        "**/__tests__/**",
        "**/{karma,rollup,webpack,vite,vitest,jest,ava,babel,nyc,cypress,tsup,build}.config.*",
        "**/vitest.{workspace,projects}.[jt]s?(on)",
        "**/.{eslint,mocha,prettier}rc.{?(c|m)js,yml}",
      ],
    },
  },
});
