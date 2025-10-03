// contracts/eslint.config.mjs — ESLint v9 flat config
import js from '@eslint/js';
import globals from 'globals';

export default [
  { ignores: [
      'node_modules/**',
      'artifacts/**',
      'cache/**',
      'dist/**',
      'build/**',
      'scripts/deploy_oya.mjs',
      'scripts/deploy_reward_only.mjs',
      'scripts/deploy_plain.mjs',
      'scripts/interact_oya.mjs',
      'server/public/Backup/**',
      'server/public/qrcode.js'
  ]},

  js.configs.recommended,

  // Server code (ESM)
  {
    files: ['server/**/*.js', '**/*.mjs'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: { ...globals.node }
    },
    rules: { 'no-empty': 'off' }
  },

  // Browser code
  {
    files: ['server/public/**/*.js'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      globals: { ...globals.browser }
    },
    rules: { 'no-undef': 'off', 'no-unused-vars': 'off' }
  }
];