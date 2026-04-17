import type { Metadata } from "next";

import "./globals.css";

export const metadata: Metadata = {
  title: "ML-MAPO · graph editor",
  description: "LiteGraph editor for ML-MAPO pipeline blueprints",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
