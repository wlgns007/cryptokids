// eslint.config.mjs — ESLint configuration without external npm downloads
import recommendedConfig from "./config/eslint/recommended.js";
import { browserGlobals, nodeGlobals } from "./config/eslint/globals.js";

const jsModule = await import("@eslint/js").catch((error) => {
  if (error && error.code !== "ERR_MODULE_NOT_FOUND") {
    console.warn("Failed to load @eslint/js. Using fallback recommended config.", error);
  }
  return { default: { configs: { recommended: recommendedConfig } } };
});

const globalsModule = await import("globals").catch((error) => {
  if (error && error.code !== "ERR_MODULE_NOT_FOUND") {
    console.warn("Failed to load globals package. Using fallback node globals.", error);
  }
  return { default: { node: {} } };
});

const jsConfigs = jsModule?.default ?? jsModule;
const globalsPackage = globalsModule?.default ?? globalsModule;
const nodeGlobalSet = { ...globalsPackage.node, ...nodeGlobals };
const recommendedBase = jsConfigs?.configs?.recommended ?? recommendedConfig;

export default [
  {
    ignores: [
      "node_modules/**",
      "dist/**",
      "build/**",
      "artifacts/**",
      "cache/**",
      "legacy/**",                // ← ignore everything we archived
      "server/public/qrcode*.js", // 3rd-party/minified
      "eslint.config.*"
    ]
  },

  recommendedBase,

  {
    files: ["**/*.js", "**/*.mjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: nodeGlobalSet,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module"
      }
    },
    rules: {}
  },

  {
    files: ["**/*.cjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "commonjs",
      globals: nodeGlobalSet
    },
    rules: {}
  },

  // Server code (ESM)
  {
    files: ["server/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: nodeGlobals,
      parserOptions: {
        ecmaVersion: "latest",
        sourceType: "module"
      }
    },
    rules: {
      "no-empty": "off",
      "no-undef": "error",
      "no-unused-vars": "warn"
    }
  },

  // Browser code
  {
    files: ["server/public/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: browserGlobals
    },
    rules: {
      "no-undef": "off",
      "no-unused-vars": "off"
    }
  }
];
