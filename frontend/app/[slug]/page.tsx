import { headers } from "next/headers";
import { notFound } from "next/navigation";

import { RenderDashboard } from "@/components/render-dashboard";

function isPrivateNetworkAddress(value: string | null) {
  if (!value) {
    return false;
  }
  if (value === "127.0.0.1" || value === "::1" || value === "localhost") {
    return true;
  }
  if (value.startsWith("10.") || value.startsWith("192.168.")) {
    return true;
  }
  if (value.startsWith("172.")) {
    const secondOctet = Number(value.split(".")[1] ?? "");
    return secondOctet >= 16 && secondOctet <= 31;
  }
  if (value.startsWith("fc") || value.startsWith("fd") || value.startsWith("fe80:")) {
    return true;
  }
  return false;
}

function isPrivateNetworkHostname(value: string | null) {
  if (!value) {
    return false;
  }
  if (isPrivateNetworkAddress(value)) {
    return true;
  }
  return value.endsWith(".local");
}

export default async function HiddenAdminPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const configuredPath =
    process.env.NEXT_PUBLIC_ADMIN_PANEL_PATH ??
    process.env.ADMIN_PANEL_PATH ??
    "control-tower";

  if (slug !== configuredPath) {
    notFound();
  }

  const headerStore = await headers();
  const forwardedFor = headerStore.get("x-forwarded-for");
  const realIp = headerStore.get("x-real-ip");
  const host = headerStore.get("host")?.split(":")[0] ?? null;
  const clientIp = forwardedFor?.split(",")[0]?.trim() ?? realIp ?? null;

  if (!isPrivateNetworkAddress(clientIp) && !isPrivateNetworkHostname(host)) {
    notFound();
  }

  return <RenderDashboard view="admin" />;
}
