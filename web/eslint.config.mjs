// The shared ruleset is ./eslint.config.base.mjs, synced from cplieger/ci;
// this file adds the ignores.
import baseConfig from "./eslint.config.base.mjs";

export default [
  ...baseConfig,
  {
    ignores: ["**/reports/**", "**/.stryker-tmp/**", "**/coverage/**"],
  },
];
