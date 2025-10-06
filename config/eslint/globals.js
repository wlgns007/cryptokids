export const sharedTimerGlobals = {
  setTimeout: "readonly",
  clearTimeout: "readonly",
  setInterval: "readonly",
  clearInterval: "readonly"
};

export const nodeGlobals = {
  ...sharedTimerGlobals,
  __dirname: "readonly",
  __filename: "readonly",
  Buffer: "readonly",
  console: "readonly",
  exports: "readonly",
  global: "readonly",
  module: "readonly",
  process: "readonly",
  require: "readonly",
  setImmediate: "readonly",
  clearImmediate: "readonly",
  TextDecoder: "readonly",
  TextEncoder: "readonly",
  URL: "readonly",
  URLSearchParams: "readonly"
};

export const browserGlobals = {
  ...sharedTimerGlobals,
  console: "readonly",
  document: "readonly",
  fetch: "readonly",
  FormData: "readonly",
  Headers: "readonly",
  HTMLElement: "readonly",
  Image: "readonly",
  localStorage: "readonly",
  location: "readonly",
  navigator: "readonly",
  Request: "readonly",
  Response: "readonly",
  sessionStorage: "readonly",
  window: "readonly",
  URL: "readonly",
  URLSearchParams: "readonly",
  crypto: "readonly"
};
