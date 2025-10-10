import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

const MODULE_EXTENSIONS = new Set([".js", ".mjs"]);
const SCRIPT_EXTENSIONS = new Set([".cjs"]);

function globToRegExp(pattern) {
  let regex = "^";

  for (let i = 0; i < pattern.length; i += 1) {
    const char = pattern[i];

    if (char === "*") {
      if (pattern[i + 1] === "*") {
        regex += ".*";
        i += 1;
        continue;
      }

      regex += "[^/]*";
      continue;
    }

    if (char === "?") {
      regex += ".";
      continue;
    }

    if (/[.+^${}()|[\]\\]/.test(char)) {
      regex += `\\${char}`;
      continue;
    }

    regex += char;
  }

  regex += "$";
  return new RegExp(regex);
}

function buildIgnoreMatchers(ignorePatterns = []) {
  return ignorePatterns
    .filter(Boolean)
    .map((pattern) => globToRegExp(pattern.replace(/^[.\/]+/, "")));
}

function shouldIgnore(relativePath, matchers) {
  const normalized = relativePath.replace(/\\/g, "/");
  return matchers.some((regex) => regex.test(normalized));
}

function collectFiles(startPaths, matchers, fileList = []) {
  for (const start of startPaths) {
    const resolved = path.resolve(process.cwd(), start);
    if (!fs.existsSync(resolved)) continue;

    const stats = fs.statSync(resolved);
    if (stats.isDirectory()) {
      for (const entry of fs.readdirSync(resolved)) {
        collectFiles([path.join(resolved, entry)], matchers, fileList);
      }
      continue;
    }

    const relativePath = path.relative(process.cwd(), resolved);
    if (shouldIgnore(relativePath, matchers)) continue;

    const ext = path.extname(resolved);
    if (!MODULE_EXTENSIONS.has(ext) && !SCRIPT_EXTENSIONS.has(ext)) continue;

    fileList.push({ filePath: resolved, relativePath, ext });
  }

  return fileList;
}

function checkSyntax({ filePath }) {
  const result = spawnSync(process.execPath, ["--check", filePath], { encoding: "utf8" });
  if ((result.status ?? 1) === 0) {
    return null;
  }

  const message = (result.stderr || result.stdout || "").trim();
  return new Error(message || `node --check exited with code ${result.status}`);
}

function parseArgs(rawArgs) {
  const args = [...rawArgs];
  const targets = [];
  let configPath;
  let fixRequested = false;

  while (args.length > 0) {
    const current = args.shift();
    if (current === "--config") {
      configPath = args.shift();
      continue;
    }

    if (current === "--fix") {
      fixRequested = true;
      continue;
    }

    targets.push(current);
  }

  return { configPath, fixRequested, targets: targets.length > 0 ? targets : ["."] };
}

async function loadIgnorePatterns(configPath) {
  if (!configPath) return [];

  const absolutePath = path.resolve(process.cwd(), configPath);
  if (!fs.existsSync(absolutePath)) return [];

  const moduleUrl = pathToFileURL(absolutePath).href;
  const configModule = await import(moduleUrl);
  const config = configModule.default ?? configModule;

  if (!Array.isArray(config) || config.length === 0) return [];

  const rootConfig = config[0];
  if (rootConfig && Array.isArray(rootConfig.ignores)) {
    return rootConfig.ignores;
  }

  return [];
}

export async function fallbackLint({ args }) {
  const { configPath, fixRequested, targets } = parseArgs(args);

  if (fixRequested) {
    console.warn("Fallback lint does not support --fix. Skipping automatic fixes.");
  }

  const ignorePatterns = await loadIgnorePatterns(configPath);
  const ignoreMatchers = buildIgnoreMatchers(ignorePatterns);

  const files = collectFiles(targets, ignoreMatchers);
  const errors = [];

  for (const file of files) {
    const error = checkSyntax({ filePath: file.filePath, ext: file.ext });
    if (error) {
      errors.push({ file, error });
    }
  }

  if (errors.length === 0) {
    if (files.length === 0) {
      console.info("Fallback lint: no matching files to check.");
    }
    return 0;
  }

  for (const { file, error } of errors) {
    const message = error && error.message ? error.message : String(error);
    console.error(`${file.relativePath}: ${message}`);
  }

  console.error(`Fallback lint found ${errors.length} syntax error(s).`);
  return 1;
}

if (import.meta.url === pathToFileURL(process.argv[1]).href) {
  fallbackLint({ args: process.argv.slice(2) }).then(
    (code) => process.exit(code ?? 1),
    (error) => {
      console.error("Fallback lint execution failed.", error);
      process.exit(1);
    }
  );
}
