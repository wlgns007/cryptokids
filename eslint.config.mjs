// eslint.config.mjs — ESLint configuration without external npm downloads
import recommendedConfig from "./config/eslint/recommended.js";
import { browserGlobals, nodeGlobals } from "./config/eslint/globals.js";

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

  recommendedConfig,

  // Server code (ESM)
  {
    files: ["server/**/*.js", "**/*.mjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: nodeGlobals
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
