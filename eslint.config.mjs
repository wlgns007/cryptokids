// eslint.config.mjs — ESLint v9 flat config
import js from "@eslint/js";
import globals from "globals";

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

  js.configs.recommended,

  // Server code (ESM)
  {
    files: ["server/**/*.js", "**/*.mjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: { ...globals.node }
    },
    rules: {
      "no-empty": "off",
      "no-unused-vars": "warn"
    }
  },

  // Browser code
  {
    files: ["server/public/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: { ...globals.browser }
    },
    rules: {
      "no-undef": "off",
      "no-unused-vars": "off"
    }
  }
];
