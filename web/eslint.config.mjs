// Package-local ESLint config. The shared ruleset is eslint.config.base.mjs (synced
// from cplieger/ci; never edit it here); this file imports it and adds the ignores.
import baseConfig from "./eslint.config.base.mjs";

export default [
  ...baseConfig,
  {
    ignores: ["**/reports/**", "**/.stryker-tmp/**", "**/coverage/**"],
  },
];
