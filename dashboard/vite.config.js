import { defineConfig } from "vite";
export default defineConfig({
  server: {
    host: "127.0.0.1",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:9100",
        changeOrigin: true,
        configure(proxy) {
          proxy.on("proxyReq", (request) => {
            request.setHeader("Origin", "http://127.0.0.1:9100");
          });
        },
      },
    },
  },
  build: { outDir: "dist" },
});
