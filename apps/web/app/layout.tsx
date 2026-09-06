import type { Metadata } from 'next';
import './globals.css';

export const metadata: Metadata = {
  title: 'Forge V2 — Distributed CI/CD Engine Shell',
  description:
    'Production-oriented foundation for Forge V2 distributed CI/CD orchestration engine.',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
