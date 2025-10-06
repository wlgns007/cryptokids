import js from "@eslint/js";
import globals from "globals";

export default [
  js.configs.recommended,

  // Node (server)
  {
    files: ["server/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: globals.node
    },
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_" }]
    }
  },

  // Browser (public)
  {
    files: ["server/public/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: globals.browser
    }
  }
];
