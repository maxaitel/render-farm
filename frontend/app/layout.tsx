import type { Metadata } from "next";
import localFont from "next/font/local";

import "./globals.css";

const widescreen = localFont({
  src: "../public/fonts/WidescreenUEx_Trial_Blk.ttf",
  variable: "--font-display",
  display: "swap",
});

const alteHaas = localFont({
  src: "../public/fonts/Alte Haas Grotesk Bold.ttf",
  variable: "--font-subheading",
  display: "swap",
});

export const metadata: Metadata = {
  title: "Render Farm",
  description: "GPU-backed Blender queue for the DGX Spark.",
};

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${widescreen.variable} ${alteHaas.variable}`}>
        {children}
      </body>
    </html>
  );
}
