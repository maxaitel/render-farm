import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        paper: "var(--paper)",
        ink: "var(--ink)",
        steel: "var(--steel)",
        sand: "var(--sand)",
        ember: "var(--ember)",
        line: "var(--line)",
        mist: "var(--mist)",
      },
      boxShadow: {
        panel: "0 20px 80px rgba(28, 31, 38, 0.08)",
      },
      borderRadius: {
        xl: "1.25rem",
      },
    },
  },
  plugins: [],
};

export default config;

