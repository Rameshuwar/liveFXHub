/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_WS_URL?: string;
  readonly VITE_API_URL?: string;
  readonly VITE_BACKEND_TARGET?: string;
  readonly VITE_WS_TARGET?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
