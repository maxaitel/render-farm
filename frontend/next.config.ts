import path from "node:path";
import type { NextConfig } from "next";

const backendOrigin = process.env.BACKEND_ORIGIN ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  output: "standalone",
  outputFileTracingRoot: path.join(__dirname, ".."),
  experimental: {
    middlewareClientMaxBodySize: "2gb",
  },
  async rewrites() {
    return [
      {
        source: "/backend/:path*",
        destination: `${backendOrigin}/:path*`,
      },
    ];
  },
};

export default nextConfig;
