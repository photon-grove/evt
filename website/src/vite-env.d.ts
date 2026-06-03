/// <reference types="vite/client" />

interface ImportMetaEnv {
  // Photon Grove homepage URL. Production builds use the production default;
  // dev builds override it (see website/.env.development).
  readonly VITE_PHOTON_GROVE_URL?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}
