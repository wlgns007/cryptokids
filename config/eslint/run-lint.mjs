#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const args = process.argv.slice(2);

function tryRun(command, commandArgs) {
  const result = spawnSync(command, commandArgs, {
    stdio: "inherit",
    env: process.env
  });

  if (result.error) {
    if (result.error.code === "ENOENT") {
      return null;
    }
    throw result.error;
  }

  return result.status ?? 1;
}

let exitCode = null;

try {
  const eslintBin = require.resolve("eslint/bin/eslint.js");
  exitCode = tryRun(process.execPath, [eslintBin, ...args]);
} catch (error) {
  if (error.code !== "MODULE_NOT_FOUND") {
    console.error("Failed to load the local ESLint binary.\n", error);
    process.exit(1);
  }
}

if (exitCode === null) {
  exitCode = null; // prefer fallback lint when local ESLint isn't available
}

if (exitCode === null) {
  const { fallbackLint } = await import("./fallback-lint.mjs");
  exitCode = await fallbackLint({ args });
}

if (exitCode === null) {
  console.error(
    "Unable to find an ESLint executable. Install it locally or make sure it is available on PATH."
  );
  process.exit(1);
}

process.exit(exitCode);
