// Package-local ESLint config. The shared ruleset is ./eslint.config.base.mjs;
// this file imports it and adds the ignores.
//
// That base is a HAND-MAINTAINED copy of cplieger/ci's canonical config, not a
// synced one: sync.yaml writes to <repo>/eslint.config.base.mjs, the ROOT, which
// nothing reads (the package-dir dest was tried in ci #371 and reverted by #372).
// So a canonical improvement lands in the unread root file and does NOT reach
// this one -- when the root copy changes, copy it here too. Nothing detects drift.
import baseConfig from "./eslint.config.base.mjs";

export default [
  ...baseConfig,
  {
    ignores: ["**/reports/**", "**/.stryker-tmp/**", "**/coverage/**"],
  },
];
