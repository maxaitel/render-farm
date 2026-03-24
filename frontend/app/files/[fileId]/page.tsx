import { RenderDashboard } from "@/components/render-dashboard";

export default async function FilePage({
  params,
}: {
  params: Promise<{ fileId: string }>;
}) {
  const { fileId } = await params;
  return <RenderDashboard fileId={fileId} view="detail" />;
}
