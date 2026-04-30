"use client";

import Image from "next/image";
import Link from "next/link";
import type { ChangeEvent, FormEvent } from "react";
import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  Ban,
  ChevronRight,
  Cpu,
  Download,
  FolderOpen,
  LoaderCircle,
  LogOut,
  Radar,
  RefreshCcw,
  Shield,
  Upload,
} from "lucide-react";

import {
  adminCancelJob,
  adminRetryJob,
  cancelJob,
  createRun,
  fetchAdminActivity,
  fetchAdminFiles,
  fetchAdminOverview,
  fetchAdminRuns,
  fetchAdminUsers,
  fetchFiles,
  fetchSession,
  fetchSystemStatus,
  inspectStoredFile,
  retryJob,
  signIn,
  signOut,
  signUp,
  updateAdminUserStatus,
  type UploadProgressStats,
  uploadFileWithProgress,
} from "@/lib/api";
import type {
  ActivityRecord,
  AdminOverview,
  AuthSession,
  BlendInspection,
  RenderJob,
  RenderMode,
  RenderSettings,
  SystemStatus,
  UserAccount,
  UserFile,
  UserStatus,
} from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import logoMark from "../public/logo.png";

type RenderDashboardProps = {
  view?: "library" | "detail" | "admin";
  fileId?: string;
};

type UploadSourceMode = "file" | "folder";
type AuthMode = "sign-in" | "sign-up";

type JobFormState = {
  renderMode: RenderMode;
  frame: number;
  startFrame: number;
  endFrame: number;
  outputFormat: "PNG" | "JPEG" | "OPEN_EXR";
  renderEngine: string;
  samples: number;
  useDenoising: boolean;
  resolutionX: number;
  resolutionY: number;
  resolutionPercentage: number;
  frameStep: number;
  fps: number;
  fpsBase: number;
  frameRate: number;
  filmTransparent: boolean;
  viewTransform: string;
  look: string;
  exposure: number;
  gamma: number;
  imageQuality: number;
  compression: number;
  useMotionBlur: boolean;
  useSimplify: boolean;
  simplifySubdivision: number;
  simplifyChildParticles: number;
  simplifyVolumes: number;
  seed: number;
};

const INITIAL_FORM: JobFormState = {
  renderMode: "still",
  frame: 1,
  startFrame: 1,
  endFrame: 24,
  outputFormat: "PNG",
  renderEngine: "CYCLES",
  samples: 128,
  useDenoising: false,
  resolutionX: 1920,
  resolutionY: 1080,
  resolutionPercentage: 100,
  frameStep: 1,
  fps: 24,
  fpsBase: 1,
  frameRate: 24,
  filmTransparent: false,
  viewTransform: "Filmic",
  look: "Medium High Contrast",
  exposure: 0,
  gamma: 1,
  imageQuality: 90,
  compression: 15,
  useMotionBlur: false,
  useSimplify: false,
  simplifySubdivision: 6,
  simplifyChildParticles: 1,
  simplifyVolumes: 1,
  seed: 0,
};

const SCENE_DIRECTORY_NAMES = new Set([
  "scene",
  "scenes",
  "shot",
  "shots",
  "render",
  "renders",
]);

const AUXILIARY_DIRECTORY_NAMES = new Set([
  "asset",
  "assets",
  "lib",
  "libs",
  "library",
  "libraries",
  "link",
  "linked",
  "texture",
  "textures",
  "cache",
  "caches",
]);

function formatTimestamp(value: string | null) {
  if (!value) {
    return "Pending";
  }
  return new Date(value).toLocaleString();
}

function formatBytes(bytes: number) {
  if (bytes < 1024 * 1024) {
    return `${Math.max(1, Math.round(bytes / 1024))} KB`;
  }
  if (bytes < 1024 * 1024 * 1024) {
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatDuration(seconds: number | null | undefined) {
  if (seconds === null || seconds === undefined || !Number.isFinite(seconds)) {
    return "Pending";
  }
  if (seconds < 60) {
    return `${Math.max(1, Math.round(seconds))}s`;
  }
  const minutes = Math.floor(seconds / 60);
  const remaining = Math.round(seconds % 60);
  if (minutes < 60) {
    return `${minutes}m ${remaining}s`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatRate(bytesPerSecond: number | null | undefined) {
  if (!bytesPerSecond || !Number.isFinite(bytesPerSecond)) {
    return "Pending";
  }
  return `${formatBytes(bytesPerSecond)}/s`;
}

function outputUrl(job: RenderJob, outputPath: string) {
  return `/backend/api/jobs/${job.id}/outputs/${outputPath
    .split("/")
    .map(encodeURIComponent)
    .join("/")}`;
}

function outputArchiveUrl(job: RenderJob) {
  return `/backend/api/jobs/${job.id}/download`;
}

function videoArchiveUrl(job: RenderJob) {
  return `/backend/api/jobs/${job.id}/download/videos`;
}

function canDownloadOutputs(job: RenderJob) {
  return Boolean(job.archive_path || job.outputs.length);
}

function canDownloadVideos(job: RenderJob) {
  return (
    canDownloadOutputs(job) &&
    job.render_mode === "animation" &&
    job.phase === "completed" &&
    job.total_frames > 1
  );
}

function isPreviewableOutput(outputPath: string) {
  return /\.(png|jpe?g|webp)$/i.test(outputPath);
}

function outputCameraName(outputPath: string) {
  return outputPath.split("/")[0] || "Default camera";
}

function outputCounts(job: RenderJob) {
  const completed = Math.max(job.completed_frames, job.outputs.length);
  return {
    completed,
    expected: Math.max(job.total_outputs_expected, completed),
  };
}

function outputProgressLabel(job: RenderJob) {
  const { completed, expected } = outputCounts(job);
  if (job.phase === "completed") {
    return `${completed} output${completed === 1 ? "" : "s"}`;
  }
  return `${completed} / ${expected} outputs`;
}

function frameProgressLabel(job: RenderJob) {
  if (job.phase === "completed") {
    return `${job.total_frames} frame${job.total_frames === 1 ? "" : "s"} complete`;
  }
  if (job.current_frame !== null) {
    return `${job.current_frame} / ${job.total_frames}`;
  }
  return `0 / ${job.total_frames}`;
}

function timingSecondaryLabel(job: RenderJob) {
  if (job.phase === "completed") {
    return "Done";
  }
  if (job.phase === "failed" || job.phase === "cancelled") {
    return job.phase;
  }
  return `ETA ${formatDuration(job.estimated_seconds_remaining)}`;
}

function averageFrameLabel(job: RenderJob) {
  if (job.average_seconds_per_frame === null) {
    return "Avg unavailable";
  }
  return `Avg ${formatDuration(job.average_seconds_per_frame)} / frame`;
}

function hasRenderSettingValues(settings: RenderSettings) {
  return Object.values(settings).some(
    (value) => value !== null && value !== undefined && value !== "",
  );
}

function mergeRenderSettingsIntoForm(
  current: JobFormState,
  settings: RenderSettings,
): JobFormState {
  return {
    ...current,
    outputFormat:
      settings.output_format === "JPEG" || settings.output_format === "OPEN_EXR"
        ? settings.output_format
        : settings.output_format === "PNG"
          ? "PNG"
          : current.outputFormat,
    renderEngine: settings.render_engine || current.renderEngine,
    samples: settings.samples ?? current.samples,
    useDenoising: settings.use_denoising ?? current.useDenoising,
    resolutionX: settings.resolution_x ?? current.resolutionX,
    resolutionY: settings.resolution_y ?? current.resolutionY,
    resolutionPercentage:
      settings.resolution_percentage ?? current.resolutionPercentage,
    frameStep: settings.frame_step ?? current.frameStep,
    fps: settings.fps ?? current.fps,
    fpsBase: settings.fps_base ?? current.fpsBase,
    frameRate: settings.frame_rate ?? current.frameRate,
    filmTransparent: settings.film_transparent ?? current.filmTransparent,
    viewTransform: settings.view_transform || current.viewTransform,
    look: settings.look || current.look,
    exposure: settings.exposure ?? current.exposure,
    gamma: settings.gamma ?? current.gamma,
    imageQuality: settings.image_quality ?? current.imageQuality,
    compression: settings.compression ?? current.compression,
    useMotionBlur: settings.use_motion_blur ?? current.useMotionBlur,
    useSimplify: settings.use_simplify ?? current.useSimplify,
    simplifySubdivision:
      settings.simplify_subdivision ?? current.simplifySubdivision,
    simplifyChildParticles:
      settings.simplify_child_particles ?? current.simplifyChildParticles,
    simplifyVolumes: settings.simplify_volumes ?? current.simplifyVolumes,
    seed: settings.seed ?? current.seed,
  };
}

function fileLabel(file: File) {
  return file.webkitRelativePath || file.name;
}

function folderProjectUploadEntries(
  blendFile: File,
  projectFiles: File[],
): {
  blendPath: string;
  projectEntries: { file: File; path: string }[];
} {
  const blendPath = fileLabel(blendFile);
  return {
    blendPath,
    projectEntries: projectFiles.flatMap((projectFile) => {
      const path = fileLabel(projectFile);
      if (path === blendPath) {
        return [];
      }
      return [{ file: projectFile, path }];
    }),
  };
}

function folderRenderTargets(projectFiles: File[]) {
  const blendFiles = projectFiles.filter((file) =>
    file.name.toLowerCase().endsWith(".blend"),
  );
  if (!blendFiles.length) {
    return [];
  }

  const rankedFiles = blendFiles.map((file) => {
    const directories = fileLabel(file).split("/").slice(0, -1);
    const normalizedDirectories = directories.map((part) => part.toLowerCase());
    const auxiliaryScore = directories.filter((part) =>
      AUXILIARY_DIRECTORY_NAMES.has(part.toLowerCase()),
    ).length;
    const sceneScore = normalizedDirectories.some((part) =>
      SCENE_DIRECTORY_NAMES.has(part),
    )
      ? 0
      : 1;

    return {
      file,
      auxiliaryScore,
      sceneScore,
      depth: directories.length,
    };
  });

  const bestAuxiliaryScore = Math.min(
    ...rankedFiles.map(({ auxiliaryScore }) => auxiliaryScore),
  );
  const bestSceneScore = Math.min(
    ...rankedFiles
      .filter(({ auxiliaryScore }) => auxiliaryScore === bestAuxiliaryScore)
      .map(({ sceneScore }) => sceneScore),
  );
  const bestDepth = Math.min(
    ...rankedFiles
      .filter(
        ({ auxiliaryScore, sceneScore }) =>
          auxiliaryScore === bestAuxiliaryScore &&
          sceneScore === bestSceneScore,
      )
      .map(({ depth }) => depth),
  );

  const primaryCandidates = rankedFiles
    .filter(
      ({ auxiliaryScore, sceneScore, depth }) =>
        auxiliaryScore === bestAuxiliaryScore &&
        sceneScore === bestSceneScore &&
        depth === bestDepth,
    )
    .sort((left, right) => fileLabel(left.file).localeCompare(fileLabel(right.file)));

  return primaryCandidates.length ? [primaryCandidates[0].file] : [];
}

function activePhase(job: RenderJob) {
  return (
    job.phase === "queued" ||
    job.phase === "running" ||
    job.phase === "packaging" ||
    job.phase === "stalled"
  );
}

function cancelablePhase(job: RenderJob) {
  return job.phase === "queued" || job.phase === "running" || job.phase === "stalled";
}

function frameLabel(job: RenderJob) {
  if (job.render_mode === "still") {
    return `Frame ${job.frame ?? 1}`;
  }
  return `Frames ${job.start_frame ?? 1}-${job.end_frame ?? job.start_frame ?? 1}`;
}

function cameraLabel(job: RenderJob) {
  if (job.camera_names.length > 1) {
    return `${job.camera_names.length} cameras`;
  }
  if (job.camera_names.length === 1) {
    return job.camera_names[0];
  }
  if (job.camera_name) {
    return job.camera_name;
  }
  return "Default camera";
}

function liveDetail(job: RenderJob) {
  const cameraPrefix = job.current_camera_name
    ? `${job.current_camera_name}${job.current_camera_index ? ` ${job.current_camera_index}/${job.total_cameras}` : ""} • `
    : "";
  if (job.current_frame !== null) {
    return `${cameraPrefix}Frame ${job.current_frame} of ${job.total_frames}`;
  }
  if (job.current_sample !== null && job.total_samples) {
    return `${cameraPrefix}Sample ${job.current_sample} of ${job.total_samples}`;
  }
  if (job.phase === "queued" && job.queue_position) {
    return `Queued at position ${job.queue_position}`;
  }
  if (job.phase === "packaging") {
    return "Packaging outputs";
  }
  return job.resolved_device
    ? `${cameraPrefix}${job.resolved_device}`
    : "Queued";
}

function deviceSummary(system: SystemStatus | null) {
  if (!system) {
    return "Loading";
  }

  return [
    system.cycles_devices.cuda.length
      ? `CUDA ${system.cycles_devices.cuda.length}`
      : null,
    system.cycles_devices.optix.length
      ? `OptiX ${system.cycles_devices.optix.length}`
      : null,
    system.cycles_devices.hip.length
      ? `HIP ${system.cycles_devices.hip.length}`
      : null,
    system.cycles_devices.cpu.length
      ? `CPU ${system.cycles_devices.cpu.length}`
      : null,
  ]
    .filter(Boolean)
    .join(" • ");
}

function runBadgeVariant(job: RenderJob): "secondary" | "success" | "destructive" {
  if (job.phase === "completed") {
    return "success";
  }
  if (job.phase === "failed" || job.phase === "cancelled" || job.phase === "stalled") {
    return "destructive";
  }
  return "secondary";
}

function userStatusVariant(status: UserStatus): "secondary" | "success" | "destructive" {
  if (status === "approved") {
    return "success";
  }
  if (status === "suspended") {
    return "destructive";
  }
  return "secondary";
}

function upsertRun(files: UserFile[], nextRun: RenderJob) {
  return files
    .map((file) => {
      if (file.id !== nextRun.file_id) {
        return file;
      }
      const jobs = [nextRun, ...file.jobs.filter((job) => job.id !== nextRun.id)].sort(
        (left, right) =>
          new Date(right.created_at).getTime() -
          new Date(left.created_at).getTime(),
      );
      return {
        ...file,
        render_settings: nextRun.render_settings,
        jobs,
        latest_job: jobs[0] ?? null,
        updated_at: nextRun.created_at,
      };
    })
    .sort(
      (left, right) =>
        new Date(right.updated_at).getTime() - new Date(left.updated_at).getTime(),
    );
}

function TopBar({
  adminPath,
  session,
  system,
  view,
  onSignOut,
}: {
  adminPath: string | null;
  session: AuthSession;
  system: SystemStatus | null;
  view: "library" | "detail" | "admin";
  onSignOut: () => void;
}) {
  const adminToggleHref = view === "admin" ? "/" : adminPath;
  const AdminToggleIcon = view === "admin" ? FolderOpen : Shield;
  const adminToggleLabel = view === "admin" ? "Workspace" : "Admin";

  return (
    <Card className="subtle-panel rounded-2xl shadow-none">
      <CardContent className="flex flex-col gap-4 p-4 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-4">
          <Image
            src={logoMark}
            alt="Render Farm"
            className="h-auto w-[112px]"
            priority
          />
          <div>
            <p className="text-sm font-medium">
              {view === "admin"
                ? "Admin"
                : view === "detail"
                  ? "Scene detail"
                  : "Library"}
            </p>
            <p className="text-sm text-muted-foreground">
              {view === "library"
                ? "Upload a scene, then open it to run renders."
                : view === "detail"
                  ? "Run renders and review history for one scene."
                  : "Approve users and watch activity."}
            </p>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Badge variant="outline">{session.user.username}</Badge>
          <Badge variant="outline">{deviceSummary(system)}</Badge>
          {adminToggleHref ? (
            <Button asChild size="sm" variant="outline">
              <Link href={adminToggleHref}>
                <AdminToggleIcon />
                {adminToggleLabel}
              </Link>
            </Button>
          ) : null}
          <Button onClick={onSignOut} size="sm" variant="ghost">
            <LogOut />
            Sign out
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

function AuthScreen({
  authBusy,
  authMode,
  authPassword,
  authUsername,
  error,
  onSubmit,
  setAuthMode,
  setAuthPassword,
  setAuthUsername,
}: {
  authBusy: boolean;
  authMode: AuthMode;
  authPassword: string;
  authUsername: string;
  error: string | null;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  setAuthMode: (value: AuthMode | ((current: AuthMode) => AuthMode)) => void;
  setAuthPassword: (value: string) => void;
  setAuthUsername: (value: string) => void;
}) {
  return (
    <div className="app-shell">
      <div className="page-frame flex min-h-screen items-center justify-center">
        <Card className="subtle-panel w-full max-w-md rounded-2xl shadow-none">
          <CardHeader className="space-y-4">
            <Image
              src={logoMark}
              alt="Render Farm"
              className="h-auto w-[120px]"
              priority
            />
            <div>
              <CardTitle className="text-2xl font-semibold">
                {authMode === "sign-in" ? "Sign in" : "Create account"}
              </CardTitle>
              <CardDescription className="mt-1">
                {authMode === "sign-in"
                  ? "Use your approved account to enter the workspace."
                  : "New accounts remain pending until an admin approves them."}
              </CardDescription>
            </div>
          </CardHeader>
          <CardContent>
            <form className="space-y-4" onSubmit={onSubmit}>
              <div className="space-y-2">
                <label className="text-sm font-medium">Username</label>
                <Input
                  value={authUsername}
                  onChange={(event) => setAuthUsername(event.target.value)}
                  autoComplete="username"
                />
              </div>
              <div className="space-y-2">
                <label className="text-sm font-medium">Password</label>
                <Input
                  type="password"
                  value={authPassword}
                  onChange={(event) => setAuthPassword(event.target.value)}
                  minLength={authMode === "sign-up" ? 12 : undefined}
                  autoComplete={
                    authMode === "sign-in" ? "current-password" : "new-password"
                  }
                />
              </div>
              <Button className="w-full">
                {authBusy ? (
                  <>
                    <LoaderCircle className="animate-spin" />
                    Working
                  </>
                ) : authMode === "sign-in" ? (
                  "Sign in"
                ) : (
                  "Create pending account"
                )}
              </Button>
            </form>
            <Button
              className="mt-4 px-0"
              onClick={() =>
                setAuthMode((current) =>
                  current === "sign-in" ? "sign-up" : "sign-in",
                )
              }
              type="button"
              variant="link"
            >
              {authMode === "sign-in"
                ? "Need access? Create an account."
                : "Already approved? Sign in."}
            </Button>
            {error ? (
              <div className="mt-4 rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
                {error}
              </div>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function PendingScreen({
  error,
  onSignOut,
  status,
}: {
  error: string | null;
  onSignOut: () => void;
  status: UserStatus;
}) {
  return (
    <div className="app-shell">
      <div className="page-frame flex min-h-screen items-center justify-center">
        <Card className="subtle-panel w-full max-w-lg rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-2xl font-semibold">
              {status === "pending" ? "Waiting for approval" : "Account suspended"}
            </CardTitle>
            <CardDescription>
              {status === "pending"
                ? "An admin must approve this account before you can render."
                : "This account was suspended."}
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <Button onClick={onSignOut} variant="outline">
              <LogOut />
              Sign out
            </Button>
            {error ? (
              <div className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
                {error}
              </div>
            ) : null}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function UploadCard({
  onFileChange,
  onFolderChange,
  onCancelUpload,
  onModeChange,
  onSubmit,
  selectedBlendFile,
  selectionMessage,
  uploadProgress,
  uploadStats,
  uploadSourceMode,
  uploading,
}: {
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onFolderChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onCancelUpload: () => void;
  onModeChange: (mode: UploadSourceMode) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  selectedBlendFile: File | null;
  selectionMessage: string | null;
  uploadProgress: number;
  uploadStats: UploadProgressStats | null;
  uploadSourceMode: UploadSourceMode;
  uploading: boolean;
}) {
  return (
    <Card className="subtle-panel rounded-2xl shadow-none">
      <CardHeader>
        <CardTitle className="text-lg font-semibold">Add scene</CardTitle>
        <CardDescription>
          Upload a blend file or a project folder. The scene will appear in the grid and get its own detail page.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form className="space-y-4" onSubmit={onSubmit}>
          <Tabs
            onValueChange={(value) => onModeChange(value as UploadSourceMode)}
            value={uploadSourceMode}
          >
            <TabsList className="grid w-full max-w-[26rem] grid-cols-2">
              <TabsTrigger disabled={uploading} value="file">
                Single blend
              </TabsTrigger>
              <TabsTrigger disabled={uploading} value="folder">
                Project folder
              </TabsTrigger>
            </TabsList>
            <TabsContent value="file" className="space-y-4">
              <Input
                accept=".blend"
                disabled={uploading}
                onChange={onFileChange}
                type="file"
              />
            </TabsContent>
            <TabsContent value="folder" className="space-y-4">
              <Input
                disabled={uploading}
                onChange={onFolderChange}
                type="file"
                {...({ webkitdirectory: "", directory: "" } as Record<string, string>)}
              />
            </TabsContent>
          </Tabs>
          <div className="rounded-lg border border-dashed bg-muted/40 px-4 py-3 text-sm text-muted-foreground">
            {selectedBlendFile
              ? selectedBlendFile.name
              : selectionMessage || "Choose a source to add it to the library."}
          </div>
          {selectionMessage && selectedBlendFile ? (
            <p className="text-sm text-muted-foreground">{selectionMessage}</p>
          ) : null}
          {uploading ? (
            <div className="space-y-2">
              <Progress value={uploadProgress} />
              <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-4">
                <span>{uploadStats ? `${uploadProgress.toFixed(0)}%` : "Starting"}</span>
                <span>
                  {uploadStats
                    ? `${formatBytes(uploadStats.loaded)} / ${formatBytes(uploadStats.total)}`
                    : "Measuring size"}
                </span>
                <span>{formatRate(uploadStats?.bytesPerSecond)}</span>
                <span>
                  ETA {formatDuration(uploadStats?.estimatedSecondsRemaining)}
                </span>
              </div>
            </div>
          ) : null}
          <div className="flex flex-wrap gap-2">
            <Button className="flex-1" disabled={uploading || !selectedBlendFile}>
              {uploading ? (
                <>
                  <LoaderCircle className="animate-spin" />
                  Uploading
                </>
              ) : (
                <>
                  <Upload />
                  Add to library
                </>
              )}
            </Button>
            {uploading ? (
              <Button
                onClick={onCancelUpload}
                type="button"
                variant="destructive"
              >
                <Ban />
                Cancel
              </Button>
            ) : null}
          </div>
        </form>
      </CardContent>
    </Card>
  );
}

function FileGrid({ files }: { files: UserFile[] }) {
  if (!files.length) {
    return (
      <Card className="subtle-panel rounded-2xl border-dashed shadow-none">
        <CardContent className="flex min-h-[280px] flex-col items-center justify-center gap-3 py-16 text-center">
          <FolderOpen className="size-8 text-muted-foreground" />
          <div>
            <p className="text-lg font-medium">No scenes uploaded yet</p>
            <p className="mt-1 text-sm text-muted-foreground">
              Upload a file to start building the library.
            </p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
      {files.map((file) => (
        <Card className="subtle-panel rounded-2xl shadow-none" key={file.id}>
          <CardHeader className="space-y-3">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <CardTitle className="truncate text-base font-semibold">
                  {file.source_filename}
                </CardTitle>
                <CardDescription className="mt-1">
                  Updated {formatTimestamp(file.updated_at)}
                </CardDescription>
              </div>
              {file.latest_job ? (
                <Badge variant={runBadgeVariant(file.latest_job)}>
                  {file.latest_job.phase}
                </Badge>
              ) : (
                <Badge variant="outline">idle</Badge>
              )}
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex items-center justify-between text-sm text-muted-foreground">
              <span>{formatBytes(file.original_size_bytes)}</span>
              <span>{file.jobs.length} run{file.jobs.length === 1 ? "" : "s"}</span>
            </div>
            <Button asChild className="w-full" variant="outline">
              <Link href={`/files/${file.id}`}>
                Open scene
                <ChevronRight />
              </Link>
            </Button>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

function LibraryView({
  files,
  loadingData,
  onFileChange,
  onFolderChange,
  onCancelUpload,
  onModeChange,
  onUpload,
  selectedBlendFile,
  selectionMessage,
  uploadProgress,
  uploadStats,
  uploadSourceMode,
  uploading,
}: {
  files: UserFile[];
  loadingData: boolean;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onFolderChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onCancelUpload: () => void;
  onModeChange: (mode: UploadSourceMode) => void;
  onUpload: (event: FormEvent<HTMLFormElement>) => void;
  selectedBlendFile: File | null;
  selectionMessage: string | null;
  uploadProgress: number;
  uploadStats: UploadProgressStats | null;
  uploadSourceMode: UploadSourceMode;
  uploading: boolean;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[320px_minmax(0,1fr)]">
      <UploadCard
        onFileChange={onFileChange}
        onFolderChange={onFolderChange}
        onCancelUpload={onCancelUpload}
        onModeChange={onModeChange}
        onSubmit={onUpload}
        selectedBlendFile={selectedBlendFile}
        selectionMessage={selectionMessage}
        uploadProgress={uploadProgress}
        uploadStats={uploadStats}
        uploadSourceMode={uploadSourceMode}
        uploading={uploading}
      />
      <Card className="subtle-panel rounded-2xl shadow-none">
        <CardHeader className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <CardTitle className="text-lg font-semibold">Library</CardTitle>
            <CardDescription>
              Open a scene to run renders and review history.
            </CardDescription>
          </div>
          <Badge variant="outline">{loadingData ? "Refreshing" : `${files.length} files`}</Badge>
        </CardHeader>
        <CardContent>
          <FileGrid files={files} />
        </CardContent>
      </Card>
    </div>
  );
}

function JobProgressPanel({
  job,
  onCancelJob,
  onRetryJob,
  cancelling,
  retrying,
}: {
  job: RenderJob;
  onCancelJob: (job: RenderJob) => void;
  onRetryJob: (job: RenderJob) => void;
  cancelling: boolean;
  retrying: boolean;
}) {
  const visibleOutputs = job.outputs.filter(isPreviewableOutput);
  const latestOutput = visibleOutputs[visibleOutputs.length - 1] ?? null;
  const cameraPosition =
    job.current_camera_index && job.total_cameras
      ? `${job.current_camera_index} / ${job.total_cameras}`
      : `${job.total_cameras}`;
  const framePosition = frameProgressLabel(job);

  return (
    <div className="space-y-4">
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-lg border bg-background p-3">
          <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
            Camera
          </p>
          <p className="mt-1 truncate text-sm font-medium">
            {job.current_camera_name || cameraLabel(job)}
          </p>
          <p className="text-xs text-muted-foreground">{cameraPosition}</p>
        </div>
        <div className="rounded-lg border bg-background p-3">
          <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
            Frame
          </p>
          <p className="mt-1 text-sm font-medium">{framePosition}</p>
          <p className="text-xs text-muted-foreground">
            {outputProgressLabel(job)}
          </p>
        </div>
        <div className="rounded-lg border bg-background p-3">
          <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
            Timing
          </p>
          <p className="mt-1 text-sm font-medium">
            {formatDuration(job.elapsed_seconds)}
          </p>
          <p className="text-xs text-muted-foreground">
            {timingSecondaryLabel(job)}
          </p>
        </div>
        <div className="rounded-lg border bg-background p-3">
          <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
            Worker
          </p>
          <p className="mt-1 truncate text-sm font-medium">
            {job.worker_assigned || "Unassigned"}
          </p>
          <p className="text-xs text-muted-foreground">
            {averageFrameLabel(job)}
          </p>
        </div>
      </div>

      <div className="space-y-2">
        <div className="flex flex-wrap items-center justify-between gap-2 text-sm">
          <span>{job.status_message}</span>
          <span className="text-muted-foreground">
            {job.progress.toFixed(0)}%
          </span>
        </div>
        <Progress value={job.progress} />
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
          {job.current_output ? <span>Current output: {job.current_output}</span> : null}
          {job.queue_position ? <span>Queue position: {job.queue_position}</span> : null}
          {job.last_progress_at ? (
            <span>Last progress: {formatTimestamp(job.last_progress_at)}</span>
          ) : null}
        </div>
      </div>

      {latestOutput ? (
        <div className="grid gap-3 md:grid-cols-[180px_minmax(0,1fr)]">
          <div className="overflow-hidden rounded-lg border bg-muted">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              alt={latestOutput}
              className="aspect-video h-full w-full object-cover"
              src={outputUrl(job, latestOutput)}
            />
          </div>
          <div className="grid grid-cols-4 gap-2 sm:grid-cols-6 lg:grid-cols-8">
            {visibleOutputs.slice(-16).map((output) => (
              <a
                className="group overflow-hidden rounded-md border bg-muted"
                href={outputUrl(job, output)}
                key={output}
                target="_blank"
              >
                {/* eslint-disable-next-line @next/next/no-img-element */}
                <img
                  alt={output}
                  className="aspect-video w-full object-cover transition-transform group-hover:scale-105"
                  src={outputUrl(job, output)}
                />
              </a>
            ))}
          </div>
        </div>
      ) : null}

      <div className="flex flex-wrap gap-2">
        {canDownloadOutputs(job) ? (
          <Button asChild size="sm" variant="outline">
            <a href={outputArchiveUrl(job)}>
              <Download />
              {job.phase === "completed" ? "Full zip" : "Partial zip"}
            </a>
          </Button>
        ) : null}
        {canDownloadVideos(job) ? (
          <Button asChild size="sm" variant="outline">
            <a href={videoArchiveUrl(job)}>
              <Download />
              Videos zip
            </a>
          </Button>
        ) : null}
        {cancelablePhase(job) ? (
          <Button
            disabled={cancelling}
            onClick={() => onCancelJob(job)}
            size="sm"
            type="button"
            variant="destructive"
          >
            {cancelling ? (
              <>
                <LoaderCircle className="animate-spin" />
                Cancelling
              </>
            ) : (
              <>
                <Ban />
                Cancel
              </>
            )}
          </Button>
        ) : null}
        {!activePhase(job) && job.phase !== "completed" ? (
          <Button
            disabled={retrying}
            onClick={() => onRetryJob(job)}
            size="sm"
            type="button"
            variant="outline"
          >
            {retrying ? (
              <>
                <LoaderCircle className="animate-spin" />
                Retrying
              </>
            ) : (
              <>
                <RefreshCcw />
                Retry
              </>
            )}
          </Button>
        ) : null}
      </div>
    </div>
  );
}

function AdminJobInspector({
  job,
  ownerName,
}: {
  job: RenderJob;
  ownerName: string;
}) {
  const failedFrames = job.frame_statuses.filter((frame) => frame.status === "failed");
  const recentFrames = job.frame_statuses
    .filter((frame) => frame.status !== "pending")
    .slice(-12);

  return (
    <details className="rounded-lg border bg-muted/20 p-3">
      <summary className="cursor-pointer text-sm font-medium">
        Inspect job details
      </summary>
      <div className="mt-4 grid gap-4 lg:grid-cols-2">
        <div className="space-y-3 text-sm">
          <div className="grid grid-cols-2 gap-3">
            {[
              ["Job", job.id],
              ["Owner", ownerName],
              ["Phase", job.phase],
              ["Worker", job.worker_assigned || "Unassigned"],
              ["Device", job.resolved_device || job.requested_device],
              ["Priority", String(job.priority)],
              ["Started", formatTimestamp(job.started_at)],
              ["Last progress", formatTimestamp(job.last_progress_at)],
              ["Expected", `${outputCounts(job).expected} outputs`],
              ["Completed", `${outputCounts(job).completed} outputs`],
            ].map(([label, value]) => (
              <div className="rounded-md border bg-background p-3" key={label}>
                <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
                  {label}
                </p>
                <p className="mt-1 break-words">{value}</p>
              </div>
            ))}
          </div>
          {job.current_output ? (
            <p className="rounded-md border bg-background p-3 text-muted-foreground">
              Current output: <span className="text-foreground">{job.current_output}</span>
            </p>
          ) : null}
          {failedFrames.length ? (
            <div className="rounded-md border border-rose-200 bg-rose-50 p-3 text-rose-800">
              <p className="font-medium">{failedFrames.length} failed frame records</p>
              {failedFrames.slice(0, 4).map((frame) => (
                <p className="mt-1 text-xs" key={`${frame.camera_name}-${frame.frame}`}>
                  {frame.camera_name || "Default camera"} frame {frame.frame}:{" "}
                  {frame.error || "failed"}
                </p>
              ))}
            </div>
          ) : null}
        </div>

        <div className="space-y-3">
          <details className="rounded-md border bg-background p-3" open>
            <summary className="cursor-pointer text-sm font-medium">Render settings</summary>
            <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap text-xs text-muted-foreground">
              {JSON.stringify(job.render_settings, null, 2)}
            </pre>
          </details>
          <details className="rounded-md border bg-background p-3">
            <summary className="cursor-pointer text-sm font-medium">Command and environment</summary>
            <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap text-xs text-muted-foreground">
              {[
                job.command.length ? job.command.join(" ") : "Command not recorded.",
                "",
                JSON.stringify(job.environment_info, null, 2),
              ].join("\n")}
            </pre>
          </details>
          <details className="rounded-md border bg-background p-3">
            <summary className="cursor-pointer text-sm font-medium">Recent frame records</summary>
            <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap text-xs text-muted-foreground">
              {JSON.stringify(recentFrames, null, 2)}
            </pre>
          </details>
          {job.logs_tail.length ? (
            <details className="rounded-md border bg-background p-3">
              <summary className="cursor-pointer text-sm font-medium">Log tail</summary>
              <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap text-xs text-muted-foreground">
                {job.logs_tail.join("\n")}
              </pre>
            </details>
          ) : null}
        </div>
      </div>
    </details>
  );
}

function FileDetailView({
  cancellingJobIds,
  cameraInspection,
  file,
  form,
  inspecting,
  onCancelJob,
  onInspect,
  onRetryJob,
  onRun,
  retryingJobIds,
  running,
  selectedCameraNames,
  setForm,
  setSelectedCameraNames,
}: {
  cancellingJobIds: string[];
  cameraInspection: BlendInspection | null;
  file: UserFile;
  form: JobFormState;
  inspecting: boolean;
  onCancelJob: (job: RenderJob) => void;
  onInspect: () => void;
  onRetryJob: (job: RenderJob) => void;
  onRun: (event: FormEvent<HTMLFormElement>) => void;
  retryingJobIds: string[];
  running: boolean;
  selectedCameraNames: string[];
  setForm: (updater: (current: JobFormState) => JobFormState) => void;
  setSelectedCameraNames: (
    value: string[] | ((current: string[]) => string[]),
  ) => void;
}) {
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Link href="/" className="inline-flex items-center gap-2 hover:text-foreground">
          <ArrowLeft className="size-4" />
          Library
        </Link>
        <ChevronRight className="size-4" />
        <span className="truncate">{file.source_filename}</span>
      </div>

      <div className="grid gap-6 xl:grid-cols-[360px_minmax(0,1fr)]">
        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">Scene</CardTitle>
            <CardDescription>{file.source_filename}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Size</span>
              <span>{formatBytes(file.original_size_bytes)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Updated</span>
              <span>{formatTimestamp(file.updated_at)}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-muted-foreground">Runs</span>
              <span>{file.jobs.length}</span>
            </div>
            {cameraInspection ? (
              <>
                <Separator />
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <p className="text-muted-foreground">Cameras</p>
                    <p>{cameraInspection.cameras.length}</p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Frames</p>
                    <p>
                      {cameraInspection.frame_start}-{cameraInspection.frame_end}
                    </p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Original resolution</p>
                    <p>
                      {cameraInspection.resolution.x}x{cameraInspection.resolution.y}
                    </p>
                  </div>
                  <div>
                    <p className="text-muted-foreground">Engine</p>
                    <p>{cameraInspection.render_engine}</p>
                  </div>
                </div>
                {cameraInspection.asset_warnings.length ? (
                  <div className="rounded-md border border-amber-200 bg-amber-50 p-3 text-amber-800">
                    {cameraInspection.asset_warnings.slice(0, 3).map((warning) => (
                      <p key={warning}>{warning}</p>
                    ))}
                  </div>
                ) : null}
              </>
            ) : null}
          </CardContent>
        </Card>

        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">New render</CardTitle>
            <CardDescription>
              Run this scene again with different cameras or frame settings.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <form className="space-y-5" onSubmit={onRun}>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                <label className="space-y-2 text-sm font-medium">
                  <span>Mode</span>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                    value={form.renderMode}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        renderMode: event.target.value as RenderMode,
                      }))
                    }
                  >
                    <option value="still">Still</option>
                    <option value="animation">Animation</option>
                  </select>
                </label>
                <label className="space-y-2 text-sm font-medium">
                  <span>Format</span>
                  <select
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                    value={form.outputFormat}
                    onChange={(event) =>
                      setForm((current) => ({
                        ...current,
                        outputFormat: event.target.value as JobFormState["outputFormat"],
                      }))
                    }
                  >
                    <option value="PNG">PNG</option>
                    <option value="JPEG">JPEG</option>
                    <option value="OPEN_EXR">OpenEXR</option>
                  </select>
                </label>
                {form.renderMode === "still" ? (
                  <label className="space-y-2 text-sm font-medium">
                    <span>Frame</span>
                    <Input
                      min={1}
                      type="number"
                      value={form.frame}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          frame: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                ) : (
                  <>
                    <label className="space-y-2 text-sm font-medium">
                      <span>Start</span>
                      <Input
                        min={0}
                        type="number"
                        value={form.startFrame}
                        onChange={(event) =>
                          setForm((current) => ({
                            ...current,
                            startFrame: Number(event.target.value || 1),
                          }))
                        }
                      />
                    </label>
                    <label className="space-y-2 text-sm font-medium">
                      <span>End</span>
                      <Input
                        min={form.startFrame}
                        type="number"
                        value={form.endFrame}
                        onChange={(event) =>
                          setForm((current) => ({
                            ...current,
                            endFrame: Number(event.target.value || current.startFrame),
                          }))
                        }
                      />
                    </label>
                  </>
                )}
              </div>

              <Separator />

              <div className="space-y-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <p className="text-sm font-medium">Cameras</p>
                    <p className="text-sm text-muted-foreground">
                      Scan this scene and select optional cameras for the run.
                    </p>
                  </div>
                  <Button onClick={onInspect} type="button" variant="outline">
                    {inspecting ? (
                      <>
                        <LoaderCircle className="animate-spin" />
                        Scanning
                      </>
                    ) : (
                      <>
                        <Radar />
                        Scan cameras
                      </>
                    )}
                  </Button>
                </div>
                {cameraInspection?.cameras.length ? (
                  <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
                    {cameraInspection.cameras.map((camera) => {
                      const checked = selectedCameraNames.includes(camera.name);
                      return (
                        <button
                          className={`rounded-md border px-3 py-2 text-left text-sm transition-colors ${
                            checked
                              ? "border-foreground bg-secondary text-foreground"
                              : "bg-background hover:bg-muted"
                          }`}
                          key={camera.name}
                          onClick={() =>
                            setSelectedCameraNames((current) =>
                              checked
                                ? current.filter((item) => item !== camera.name)
                                : [...current, camera.name],
                            )
                          }
                          type="button"
                        >
                          {camera.name}
                        </button>
                      );
                    })}
                  </div>
                ) : null}
              </div>

              <Separator />

              <div className="space-y-3">
                <div>
                  <p className="text-sm font-medium">Render settings</p>
                  <p className="text-sm text-muted-foreground">
                    The first scan seeds these from the blend. Your changes are saved as this scene's defaults after each run.
                  </p>
                </div>
                <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                  <label className="space-y-2 text-sm font-medium">
                    <span>Engine</span>
                    <select
                      className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                      value={form.renderEngine}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          renderEngine: event.target.value,
                        }))
                      }
                    >
                      <option value="CYCLES">Cycles</option>
                      <option value="BLENDER_EEVEE_NEXT">Eevee</option>
                      <option value="BLENDER_WORKBENCH">Workbench</option>
                    </select>
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Samples</span>
                    <Input
                      min={1}
                      type="number"
                      value={form.samples}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          samples: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Resolution X</span>
                    <Input
                      min={1}
                      type="number"
                      value={form.resolutionX}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          resolutionX: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Resolution Y</span>
                    <Input
                      min={1}
                      type="number"
                      value={form.resolutionY}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          resolutionY: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Scale %</span>
                    <Input
                      max={100}
                      min={1}
                      type="number"
                      value={form.resolutionPercentage}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          resolutionPercentage: Number(event.target.value || 100),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Frame step</span>
                    <Input
                      min={1}
                      type="number"
                      value={form.frameStep}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          frameStep: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Image quality</span>
                    <Input
                      max={100}
                      min={1}
                      type="number"
                      value={form.imageQuality}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          imageQuality: Number(event.target.value || 90),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Compression</span>
                    <Input
                      max={100}
                      min={0}
                      type="number"
                      value={form.compression}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          compression: Number(event.target.value || 0),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>View transform</span>
                    <Input
                      value={form.viewTransform}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          viewTransform: event.target.value,
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Look</span>
                    <Input
                      value={form.look}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          look: event.target.value,
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Exposure</span>
                    <Input
                      step="0.1"
                      type="number"
                      value={form.exposure}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          exposure: Number(event.target.value || 0),
                        }))
                      }
                    />
                  </label>
                  <label className="space-y-2 text-sm font-medium">
                    <span>Gamma</span>
                    <Input
                      min={0.01}
                      step="0.01"
                      type="number"
                      value={form.gamma}
                      onChange={(event) =>
                        setForm((current) => ({
                          ...current,
                          gamma: Number(event.target.value || 1),
                        }))
                      }
                    />
                  </label>
                </div>
                <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                  {[
                    ["Denoising", "useDenoising"],
                    ["Transparent", "filmTransparent"],
                    ["Motion blur", "useMotionBlur"],
                    ["Simplify", "useSimplify"],
                  ].map(([label, key]) => (
                    <label
                      className="flex items-center gap-2 rounded-md border bg-background px-3 py-2 text-sm"
                      key={key}
                    >
                      <input
                        checked={Boolean(form[key as keyof JobFormState])}
                        onChange={(event) =>
                          setForm((current) => ({
                            ...current,
                            [key]: event.target.checked,
                          }))
                        }
                        type="checkbox"
                      />
                      <span>{label}</span>
                    </label>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <Button disabled={running}>
                  {running ? (
                    <>
                      <LoaderCircle className="animate-spin" />
                      Queueing render
                    </>
                  ) : (
                    <>
                      <Cpu />
                      Start render
                    </>
                  )}
                </Button>
                <p className="text-sm text-muted-foreground">
                  Device assignment is handled by the admin scheduler.
                </p>
              </div>
            </form>
          </CardContent>
        </Card>
      </div>

      <Card className="subtle-panel rounded-2xl shadow-none">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">Run history</CardTitle>
          <CardDescription>
            Previous renders for this scene.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {file.jobs.length ? (
            file.jobs.map((job) => (
              <Card className="rounded-xl shadow-none" key={job.id}>
                <CardContent className="space-y-4 p-4">
                  <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                    <div className="space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                        <span className="text-sm">{cameraLabel(job)}</span>
                        <span className="text-sm text-muted-foreground">{frameLabel(job)}</span>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        {formatTimestamp(job.created_at)}
                      </p>
                      <p className="text-sm text-foreground/80">
                        {activePhase(job)
                          ? liveDetail(job)
                          : job.error || job.status_message}
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-2 md:justify-end">
                      {canDownloadOutputs(job) ? (
                        <Button asChild size="sm" variant="outline">
                          <a href={outputArchiveUrl(job)}>
                            <Download />
                            {job.phase === "completed" ? "Full zip" : "Partial zip"}
                          </a>
                        </Button>
                      ) : null}
                      {canDownloadVideos(job) ? (
                        <Button asChild size="sm" variant="outline">
                          <a href={videoArchiveUrl(job)}>
                            <Download />
                            Videos zip
                          </a>
                        </Button>
                      ) : null}
                      {cancelablePhase(job) ? (
                        <Button
                          disabled={cancellingJobIds.includes(job.id)}
                          onClick={() => onCancelJob(job)}
                          size="sm"
                          type="button"
                          variant="destructive"
                        >
                          {cancellingJobIds.includes(job.id) ? (
                            <>
                              <LoaderCircle className="animate-spin" />
                              Cancelling
                            </>
                          ) : (
                            <>
                              <Ban />
                              Cancel
                            </>
                          )}
                        </Button>
                      ) : null}
                    </div>
                  </div>
                  <JobProgressPanel
                    cancelling={cancellingJobIds.includes(job.id)}
                    job={job}
                    onCancelJob={onCancelJob}
                    onRetryJob={onRetryJob}
                    retrying={retryingJobIds.includes(job.id)}
                  />
                  {job.logs_tail.length ? (
                    <details className="rounded-md border bg-muted/30 p-3">
                      <summary className="cursor-pointer text-sm font-medium">
                        Log tail
                      </summary>
                      <pre className="mt-3 overflow-x-auto whitespace-pre-wrap text-xs text-muted-foreground">
                        {job.logs_tail.join("\n")}
                      </pre>
                    </details>
                  ) : null}
                </CardContent>
              </Card>
            ))
          ) : (
            <div className="rounded-lg border border-dashed px-4 py-10 text-center text-sm text-muted-foreground">
              No runs yet for this scene.
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function AdminView({
  adminActivity,
  adminFiles,
  adminOverview,
  adminRuns,
  adminUsers,
  onCancelJob,
  onRetryJob,
  onStatusChange,
}: {
  adminActivity: ActivityRecord[];
  adminFiles: UserFile[];
  adminOverview: AdminOverview | null;
  adminRuns: RenderJob[];
  adminUsers: UserAccount[];
  onCancelJob: (job: RenderJob) => void;
  onRetryJob: (job: RenderJob) => void;
  onStatusChange: (userId: number, status: UserStatus) => void;
}) {
  const [jobSearch, setJobSearch] = useState("");
  const [userSearch, setUserSearch] = useState("");
  const normalizedJobSearch = jobSearch.trim().toLowerCase();
  const normalizedUserSearch = userSearch.trim().toLowerCase();
  const userNameById = new Map(
    adminUsers.map((user) => [user.id, user.username] as const),
  );
  const ownerLabel = (userId: number) => userNameById.get(userId) || `User ${userId}`;
  const visibleUsers = adminUsers.filter((user) =>
    user.username.toLowerCase().includes(normalizedUserSearch),
  );
  const visibleRuns = adminRuns.filter((job) => {
    if (!normalizedJobSearch) {
      return true;
    }
    return [
      job.source_filename,
      job.phase,
      job.id,
      job.camera_names.join(" "),
      String(job.user_id),
      ownerLabel(job.user_id),
    ]
      .join(" ")
      .toLowerCase()
      .includes(normalizedJobSearch);
  });
  const visibleFiles = adminFiles.filter((file) => {
    if (!normalizedJobSearch) {
      return true;
    }
    return [file.source_filename, file.id, String(file.user_id), ownerLabel(file.user_id)]
      .join(" ")
      .toLowerCase()
      .includes(normalizedJobSearch);
  });
  const currentJobs = adminRuns.filter(activePhase);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-3 xl:grid-cols-6">
        {[
          ["Pending", adminOverview?.pending_users ?? 0],
          ["Approved", adminOverview?.approved_users ?? 0],
          ["Suspended", adminOverview?.suspended_users ?? 0],
          ["Files", adminOverview?.total_files ?? 0],
          ["Runs", adminOverview?.total_runs ?? 0],
          ["Active", adminOverview?.active_runs ?? 0],
        ].map(([label, value]) => (
          <Card className="subtle-panel rounded-2xl shadow-none" key={label}>
            <CardContent className="p-4">
              <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">
                {label}
              </p>
              <p className="mt-2 text-2xl font-semibold">{value}</p>
            </CardContent>
          </Card>
        ))}
      </div>

      <Card className="subtle-panel rounded-2xl shadow-none">
        <CardHeader className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <CardTitle className="text-lg font-semibold">Current jobs</CardTitle>
            <CardDescription>Queued, rendering, packaging, and stalled jobs.</CardDescription>
          </div>
          <Badge variant="outline">{currentJobs.length} active</Badge>
        </CardHeader>
        <CardContent className="space-y-4">
          {currentJobs.length ? (
            currentJobs.map((job) => (
              <div className="space-y-4 rounded-lg border p-4" key={job.id}>
                <div className="flex flex-col gap-2 md:flex-row md:items-start md:justify-between">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-sm font-medium">{job.source_filename}</p>
                      <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                    </div>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {ownerLabel(job.user_id)} • {cameraLabel(job)} • {frameLabel(job)}
                    </p>
                  </div>
                </div>
                <JobProgressPanel
                  cancelling={false}
                  job={job}
                  onCancelJob={onCancelJob}
                  onRetryJob={onRetryJob}
                  retrying={false}
                />
                <AdminJobInspector job={job} ownerName={ownerLabel(job.user_id)} />
              </div>
            ))
          ) : (
            <div className="rounded-lg border border-dashed px-4 py-8 text-center text-sm text-muted-foreground">
              No active jobs right now.
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">Users</CardTitle>
            <CardDescription>Approve or suspend access.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <Input
              placeholder="Search users"
              value={userSearch}
              onChange={(event) => setUserSearch(event.target.value)}
            />
            {visibleUsers.map((user) => (
              <div
                className="flex flex-col gap-4 rounded-lg border p-4 md:flex-row md:items-center md:justify-between"
                key={user.id}
              >
                <div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="font-medium">{user.username}</span>
                    <Badge variant={userStatusVariant(user.status)}>{user.status}</Badge>
                    <Badge variant="outline">{user.role}</Badge>
                  </div>
                  <p className="mt-2 text-sm text-muted-foreground">
                    {user.render_file_count} files • {user.run_count} runs
                  </p>
                </div>
                <div className="flex gap-2">
                  <Button onClick={() => onStatusChange(user.id, "approved")} size="sm" variant="outline">
                    Approve
                  </Button>
                  <Button onClick={() => onStatusChange(user.id, "suspended")} size="sm" variant="outline">
                    Suspend
                  </Button>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card className="subtle-panel rounded-2xl shadow-none">
            <CardHeader>
              <CardTitle className="text-lg font-semibold">Activity</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {adminActivity.slice(0, 8).map((item) => (
                <div className="rounded-lg border p-4" key={item.id}>
                  <p className="text-sm">{item.description}</p>
                  <p className="mt-2 text-xs text-muted-foreground">
                    {item.event_type} • {formatTimestamp(item.created_at)}
                  </p>
                </div>
              ))}
            </CardContent>
          </Card>
          <Card className="subtle-panel rounded-2xl shadow-none">
            <CardHeader>
              <CardTitle className="text-lg font-semibold">Recent runs</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Input
                placeholder="Search jobs, files, status, user id"
                value={jobSearch}
                onChange={(event) => setJobSearch(event.target.value)}
              />
              {visibleRuns.slice(0, 8).map((job) => (
                <div className="rounded-lg border p-4" key={job.id}>
                  <div className="flex items-center justify-between gap-3">
                    <p className="truncate text-sm font-medium">{job.source_filename}</p>
                    <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                  </div>
                  <p className="mt-2 text-xs text-muted-foreground">
                    {ownerLabel(job.user_id)} • {cameraLabel(job)} • {frameLabel(job)}
                  </p>
                  <p className="mt-2 text-xs text-muted-foreground">
                    {outputProgressLabel(job)} • Last progress{" "}
                    {formatTimestamp(job.last_progress_at)}
                  </p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {canDownloadOutputs(job) ? (
                      <Button asChild size="sm" variant="outline">
                        <a href={outputArchiveUrl(job)}>
                          <Download />
                          {job.phase === "completed" ? "Full zip" : "Partial zip"}
                        </a>
                      </Button>
                    ) : null}
                    {canDownloadVideos(job) ? (
                      <Button asChild size="sm" variant="outline">
                        <a href={videoArchiveUrl(job)}>
                          <Download />
                          Videos zip
                        </a>
                      </Button>
                    ) : null}
                    {job.log_path ? (
                      <Button asChild size="sm" variant="outline">
                        <a href={`/backend/api/jobs/${job.id}/logs`}>
                          <RefreshCcw />
                          Logs
                        </a>
                      </Button>
                    ) : null}
                    {cancelablePhase(job) ? (
                      <Button
                        onClick={() => onCancelJob(job)}
                        size="sm"
                        type="button"
                        variant="destructive"
                      >
                        <Ban />
                        Cancel
                      </Button>
                    ) : null}
                    {!activePhase(job) && job.phase !== "completed" ? (
                      <Button
                        onClick={() => onRetryJob(job)}
                        size="sm"
                        type="button"
                        variant="outline"
                      >
                        <RefreshCcw />
                        Retry
                      </Button>
                    ) : null}
                  </div>
                  {job.outputs.filter(isPreviewableOutput).length ? (
                    <div className="mt-3 grid grid-cols-4 gap-2">
                      {job.outputs.filter(isPreviewableOutput).slice(-4).map((output) => (
                        <a
                          className="overflow-hidden rounded-md border bg-muted"
                          href={outputUrl(job, output)}
                          key={output}
                          target="_blank"
                        >
                          {/* eslint-disable-next-line @next/next/no-img-element */}
                          <img
                            alt={output}
                            className="aspect-video w-full object-cover"
                            src={outputUrl(job, output)}
                          />
                        </a>
                      ))}
                    </div>
                  ) : null}
                  <div className="mt-3">
                    <AdminJobInspector job={job} ownerName={ownerLabel(job.user_id)} />
                  </div>
                </div>
              ))}
            </CardContent>
          </Card>
        </div>
      </div>

      <Card className="subtle-panel rounded-2xl shadow-none">
        <CardHeader>
          <CardTitle className="text-lg font-semibold">Uploaded files</CardTitle>
          <CardDescription>Inspect original blend files and user ownership.</CardDescription>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {visibleFiles.slice(0, 12).map((file) => (
            <div className="rounded-lg border p-4" key={file.id}>
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium">{file.source_filename}</p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {ownerLabel(file.user_id)} • {formatBytes(file.original_size_bytes)}
                  </p>
                </div>
                <Badge variant="outline">{file.jobs.length} jobs</Badge>
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <Button asChild size="sm" variant="outline">
                  <a href={`/backend/api/admin/files/${file.id}/download`}>
                    <Download />
                    Original
                  </a>
                </Button>
              </div>
            </div>
          ))}
        </CardContent>
      </Card>
    </div>
  );
}

export function RenderDashboard({
  view = "library",
  fileId,
}: RenderDashboardProps) {
  const [authMode, setAuthMode] = useState<AuthMode>("sign-in");
  const [session, setSession] = useState<AuthSession | null>(null);
  const [system, setSystem] = useState<SystemStatus | null>(null);
  const [files, setFiles] = useState<UserFile[]>([]);
  const [form, setForm] = useState<JobFormState>(INITIAL_FORM);
  const [cameraInspection, setCameraInspection] =
    useState<BlendInspection | null>(null);
  const [selectedCameraNames, setSelectedCameraNames] = useState<string[]>([]);
  const [uploadSourceMode, setUploadSourceMode] =
    useState<UploadSourceMode>("file");
  const [uploadBlendFile, setUploadBlendFile] = useState<File | null>(null);
  const [uploadProjectFiles, setUploadProjectFiles] = useState<File[]>([]);
  const [authUsername, setAuthUsername] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStats, setUploadStats] = useState<UploadProgressStats | null>(null);
  const [running, setRunning] = useState(false);
  const [inspecting, setInspecting] = useState(false);
  const [cancellingJobIds, setCancellingJobIds] = useState<string[]>([]);
  const [retryingJobIds, setRetryingJobIds] = useState<string[]>([]);
  const [booting, setBooting] = useState(true);
  const [loadingData, setLoadingData] = useState(false);
  const [selectionMessage, setSelectionMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [adminOverview, setAdminOverview] = useState<AdminOverview | null>(null);
  const [adminUsers, setAdminUsers] = useState<UserAccount[]>([]);
  const [adminActivity, setAdminActivity] = useState<ActivityRecord[]>([]);
  const [adminRuns, setAdminRuns] = useState<RenderJob[]>([]);
  const [adminFiles, setAdminFiles] = useState<UserFile[]>([]);
  const sourcesRef = useRef<Map<string, EventSource>>(new Map());
  const uploadAbortControllerRef = useRef<AbortController | null>(null);
  const activeJobIds = Array.from(
    new Set(files.flatMap((file) => file.jobs).filter(activePhase).map((job) => job.id)),
  ).sort();
  const activeJobIdsKey = JSON.stringify(activeJobIds);
  const activeJobSessionKey =
    session && session.user.status === "approved" ? `${session.user.id}:approved` : "closed";

  const selectedFile = fileId
    ? files.find((item) => item.id === fileId) ?? null
    : null;
  const adminPath = session?.admin_panel_path ? `/${session.admin_panel_path}` : null;

  useEffect(() => {
    let cancelled = false;

    void fetchSession()
      .then((payload) => {
        if (!cancelled) {
          setSession(payload);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setSession(null);
        }
      })
      .finally(() => {
        if (!cancelled) {
          setBooting(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  async function refreshCoreData(currentSession = session) {
    if (!currentSession || currentSession.user.status !== "approved") {
      return;
    }
    setLoadingData(true);
    try {
      const [systemPayload, filesPayload] = await Promise.all([
        fetchSystemStatus(),
        fetchFiles(),
      ]);
      setSystem(systemPayload);
      setFiles(
        filesPayload.sort(
          (left, right) =>
            new Date(right.updated_at).getTime() -
            new Date(left.updated_at).getTime(),
        ),
      );
      setError(null);
    } catch (loadError) {
      setError(
        loadError instanceof Error ? loadError.message : "Failed to load workspace.",
      );
    } finally {
      setLoadingData(false);
    }
  }

  async function refreshAdminData(currentSession = session) {
    if (
      !currentSession ||
      currentSession.user.role !== "admin" ||
      !currentSession.lan_admin_access ||
      view !== "admin"
    ) {
      return;
    }
    try {
      const [overviewPayload, usersPayload, activityPayload, runsPayload, filesPayload] =
        await Promise.all([
          fetchAdminOverview(),
          fetchAdminUsers(),
          fetchAdminActivity(),
          fetchAdminRuns(),
          fetchAdminFiles(),
        ]);
      setAdminOverview(overviewPayload);
      setAdminUsers(usersPayload);
      setAdminActivity(activityPayload);
      setAdminRuns(runsPayload);
      setAdminFiles(filesPayload);
    } catch (loadError) {
      setError(
        loadError instanceof Error ? loadError.message : "Failed to load admin panel.",
      );
    }
  }

  useEffect(() => {
    if (!session) {
      setFiles([]);
      setSystem(null);
      return;
    }
    if (session.user.status === "approved") {
      void refreshCoreData(session);
      if (view === "admin") {
        void refreshAdminData(session);
      }
      const intervalId = window.setInterval(() => {
        void refreshCoreData(session);
        if (view === "admin") {
          void refreshAdminData(session);
        }
      }, 15000);
      return () => {
        window.clearInterval(intervalId);
      };
    }
    const intervalId = window.setInterval(() => {
      void fetchSession()
        .then(setSession)
        .catch(() => undefined);
    }, 15000);
    return () => {
      window.clearInterval(intervalId);
    };
  }, [session, view]);

  useEffect(() => {
    if (!session || session.user.status !== "approved") {
      sourcesRef.current.forEach((source) => source.close());
      sourcesRef.current.clear();
      return;
    }

    const activeIds = new Set(JSON.parse(activeJobIdsKey) as string[]);

    sourcesRef.current.forEach((source, jobId) => {
      if (!activeIds.has(jobId)) {
        source.close();
        sourcesRef.current.delete(jobId);
      }
    });

    activeIds.forEach((jobId) => {
      if (sourcesRef.current.has(jobId)) {
        return;
      }
      const source = new EventSource(`/backend/api/jobs/${jobId}/events`);
      source.onmessage = (event) => {
        const payload = JSON.parse(event.data) as RenderJob;
        setFiles((current) => upsertRun(current, payload));
      };
      source.onerror = () => {
        source.close();
        sourcesRef.current.delete(jobId);
      };
      sourcesRef.current.set(jobId, source);
    });
  }, [activeJobIdsKey, activeJobSessionKey]);

  useEffect(() => {
    return () => {
      uploadAbortControllerRef.current?.abort();
      sourcesRef.current.forEach((source) => source.close());
      sourcesRef.current.clear();
    };
  }, []);

  useEffect(() => {
    if (!selectedFile || !hasRenderSettingValues(selectedFile.render_settings)) {
      return;
    }
    setForm((current) =>
      mergeRenderSettingsIntoForm(current, selectedFile.render_settings),
    );
  }, [selectedFile?.id]);

  async function handleAuthSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAuthBusy(true);
    try {
      const payload =
        authMode === "sign-in"
          ? await signIn(authUsername, authPassword)
          : await signUp(authUsername, authPassword);
      setSession(payload);
      setAuthPassword("");
      setError(
        authMode === "sign-up"
          ? "Account created. An admin must approve it before use."
          : null,
      );
    } catch (authError) {
      setError(
        authError instanceof Error ? authError.message : "Authentication failed.",
      );
    } finally {
      setAuthBusy(false);
    }
  }

  async function handleSignOut() {
    await signOut().catch(() => undefined);
    setSession(null);
    setFiles([]);
    setSystem(null);
    setAdminOverview(null);
    setAdminUsers([]);
    setAdminActivity([]);
    setAdminRuns([]);
    setAdminFiles([]);
  }

  function handleSingleFileChange(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0] ?? null;
    setUploadSourceMode("file");
    setUploadBlendFile(file);
    setUploadProjectFiles([]);
    setSelectionMessage(file ? `${file.name} selected.` : null);
  }

  function handleFolderChange(event: ChangeEvent<HTMLInputElement>) {
    const allFiles = Array.from(event.target.files ?? []);
    setUploadSourceMode("folder");
    setUploadProjectFiles(allFiles);
    const targets = folderRenderTargets(allFiles);
    const blendFile = targets[0] ?? null;
    setUploadBlendFile(blendFile);
    if (!blendFile) {
      setSelectionMessage("No .blend file was found in that folder.");
      return;
    }
    setSelectionMessage(
      `${blendFile.webkitRelativePath || blendFile.name} will be used as the source scene.`,
    );
  }

  async function handleUpload(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!uploadBlendFile) {
      setError("Choose a file or folder first.");
      return;
    }

    const formData = new FormData();
    formData.set("blend_file", uploadBlendFile, uploadBlendFile.name);
    if (uploadSourceMode === "folder") {
      const payload = folderProjectUploadEntries(uploadBlendFile, uploadProjectFiles);
      formData.set("blend_file_path", payload.blendPath);
      payload.projectEntries.forEach(({ file, path }) => {
        formData.append("project_files", file, file.name);
        formData.append("project_paths", path);
      });
    }

    setUploading(true);
    setUploadProgress(0);
    setUploadStats(null);
    const abortController = new AbortController();
    uploadAbortControllerRef.current = abortController;
    try {
      await uploadFileWithProgress(formData, (stats) => {
        setUploadProgress(stats.progress);
        setUploadStats(stats);
      }, abortController.signal);
      setUploadBlendFile(null);
      setUploadProjectFiles([]);
      setSelectionMessage("Scene added to the library.");
      setError(null);
      void refreshCoreData();
    } catch (uploadError) {
      const wasCancelled =
        uploadError instanceof Error && uploadError.message === "Upload cancelled.";
      if (wasCancelled) {
        setUploadProgress(0);
        setUploadStats(null);
        setSelectionMessage("Upload cancelled.");
        setError(null);
      } else {
        setError(uploadError instanceof Error ? uploadError.message : "Upload failed.");
      }
    } finally {
      if (uploadAbortControllerRef.current === abortController) {
        uploadAbortControllerRef.current = null;
      }
      setUploading(false);
    }
  }

  function handleCancelUpload() {
    uploadAbortControllerRef.current?.abort();
  }

  async function handleInspect() {
    if (!selectedFile) {
      return;
    }
    setInspecting(true);
    try {
      const scanFrame = form.renderMode === "still" ? form.frame : form.startFrame;
      const payload = await inspectStoredFile(selectedFile.id, scanFrame);
      setCameraInspection(payload);
      const defaultSelection =
        payload.default_camera && payload.cameras.some((camera) => camera.name === payload.default_camera)
          ? [payload.default_camera]
          : payload.cameras[0]
            ? [payload.cameras[0].name]
            : [];
      setSelectedCameraNames(defaultSelection);
      setForm((current) =>
        mergeRenderSettingsIntoForm(
          {
            ...current,
            startFrame: payload.frame_start,
            endFrame: payload.frame_end,
            resolutionX: payload.resolution.x,
            resolutionY: payload.resolution.y,
            resolutionPercentage: payload.resolution.percentage,
            frameStep: payload.frame_step,
            imageQuality: payload.image_settings.quality ?? current.imageQuality,
            compression: payload.image_settings.compression ?? current.compression,
          },
          payload.render_settings,
        ),
      );
      setError(null);
    } catch (inspectError) {
      setError(
        inspectError instanceof Error ? inspectError.message : "Camera scan failed.",
      );
    } finally {
      setInspecting(false);
    }
  }

  async function handleCreateRun(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedFile) {
      return;
    }

    const formData = new FormData();
    formData.set("render_mode", form.renderMode);
    formData.set("output_format", form.outputFormat);
    if (form.renderMode === "still") {
      formData.set("frame", String(form.frame));
    } else {
      formData.set("start_frame", String(form.startFrame));
      formData.set("end_frame", String(form.endFrame));
    }
    formData.set("render_engine", form.renderEngine);
    formData.set("samples", String(form.samples));
    formData.set("use_denoising", String(form.useDenoising));
    formData.set("resolution_x", String(form.resolutionX));
    formData.set("resolution_y", String(form.resolutionY));
    formData.set("resolution_percentage", String(form.resolutionPercentage));
    formData.set("frame_step", String(form.frameStep));
    formData.set("fps", String(form.fps));
    formData.set("fps_base", String(form.fpsBase));
    formData.set("frame_rate", String(form.frameRate));
    formData.set("film_transparent", String(form.filmTransparent));
    formData.set("view_transform", form.viewTransform);
    formData.set("look", form.look);
    formData.set("exposure", String(form.exposure));
    formData.set("gamma", String(form.gamma));
    formData.set("image_quality", String(form.imageQuality));
    formData.set("compression", String(form.compression));
    formData.set("use_motion_blur", String(form.useMotionBlur));
    formData.set("use_simplify", String(form.useSimplify));
    formData.set("simplify_subdivision", String(form.simplifySubdivision));
    formData.set("simplify_child_particles", String(form.simplifyChildParticles));
    formData.set("simplify_volumes", String(form.simplifyVolumes));
    formData.set("seed", String(form.seed));
    selectedCameraNames.forEach((cameraName) => {
      formData.append("camera_names", cameraName);
    });

    setRunning(true);
    try {
      const run = await createRun(selectedFile.id, formData);
      setFiles((current) => upsertRun(current, run));
      setError(null);
      void refreshCoreData();
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to queue render.");
    } finally {
      setRunning(false);
    }
  }

  async function handleCancelJob(job: RenderJob) {
    if (!window.confirm(`Cancel render ${job.id}? This stops the job immediately.`)) {
      return;
    }

    setCancellingJobIds((current) =>
      current.includes(job.id) ? current : [...current, job.id],
    );
    try {
      const snapshot = await cancelJob(job.id);
      setFiles((current) => upsertRun(current, snapshot));
      setError(null);
      void refreshCoreData();
    } catch (cancelError) {
      setError(
        cancelError instanceof Error ? cancelError.message : "Failed to cancel render.",
      );
    } finally {
      setCancellingJobIds((current) => current.filter((item) => item !== job.id));
    }
  }

  async function handleRetryJob(job: RenderJob) {
    setRetryingJobIds((current) =>
      current.includes(job.id) ? current : [...current, job.id],
    );
    try {
      const snapshot = await retryJob(job.id);
      setFiles((current) => upsertRun(current, snapshot));
      setError(null);
      void refreshCoreData();
    } catch (retryError) {
      setError(retryError instanceof Error ? retryError.message : "Failed to retry render.");
    } finally {
      setRetryingJobIds((current) => current.filter((item) => item !== job.id));
    }
  }

  async function handleAdminStatus(userId: number, status: UserStatus) {
    try {
      const updated = await updateAdminUserStatus(userId, status);
      setAdminUsers((current) =>
        current.map((item) => (item.id === updated.id ? updated : item)),
      );
      void refreshAdminData();
    } catch (adminError) {
      setError(
        adminError instanceof Error ? adminError.message : "Failed to update user.",
      );
    }
  }

  async function handleAdminCancelJob(job: RenderJob) {
    if (!window.confirm(`Cancel render ${job.id}?`)) {
      return;
    }
    try {
      const snapshot = await adminCancelJob(job.id);
      setAdminRuns((current) =>
        current.map((item) => (item.id === snapshot.id ? snapshot : item)),
      );
      void refreshAdminData();
      void refreshCoreData();
    } catch (adminError) {
      setError(
        adminError instanceof Error ? adminError.message : "Failed to cancel render.",
      );
    }
  }

  async function handleAdminRetryJob(job: RenderJob) {
    try {
      const snapshot = await adminRetryJob(job.id);
      setAdminRuns((current) => [snapshot, ...current]);
      void refreshAdminData();
      void refreshCoreData();
    } catch (adminError) {
      setError(
        adminError instanceof Error ? adminError.message : "Failed to retry render.",
      );
    }
  }

  if (booting) {
    return (
      <div className="app-shell">
        <div className="page-frame flex min-h-screen items-center justify-center">
          <LoaderCircle className="size-5 animate-spin text-muted-foreground" />
        </div>
      </div>
    );
  }

  if (!session) {
    return (
      <AuthScreen
        authBusy={authBusy}
        authMode={authMode}
        authPassword={authPassword}
        authUsername={authUsername}
        error={error}
        onSubmit={handleAuthSubmit}
        setAuthMode={setAuthMode}
        setAuthPassword={setAuthPassword}
        setAuthUsername={setAuthUsername}
      />
    );
  }

  if (session.user.status !== "approved") {
    return (
      <PendingScreen
        error={error}
        onSignOut={() => void handleSignOut()}
        status={session.user.status}
      />
    );
  }

  return (
    <div className="app-shell">
      <div className="page-frame space-y-6">
        <TopBar
          adminPath={adminPath}
          onSignOut={() => void handleSignOut()}
          session={session}
          system={system}
          view={view}
        />

        {error ? (
          <div className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
            {error}
          </div>
        ) : null}

        {view === "admin" ? (
          <AdminView
            adminActivity={adminActivity}
            adminFiles={adminFiles}
            adminOverview={adminOverview}
            adminRuns={adminRuns}
            adminUsers={adminUsers}
            onCancelJob={(job) => void handleAdminCancelJob(job)}
            onRetryJob={(job) => void handleAdminRetryJob(job)}
            onStatusChange={(userId, status) => void handleAdminStatus(userId, status)}
          />
        ) : view === "detail" ? (
          selectedFile ? (
            <FileDetailView
              cancellingJobIds={cancellingJobIds}
              cameraInspection={cameraInspection}
              file={selectedFile}
              form={form}
              inspecting={inspecting}
              onCancelJob={(job) => void handleCancelJob(job)}
              onInspect={() => void handleInspect()}
              onRetryJob={(job) => void handleRetryJob(job)}
              onRun={handleCreateRun}
              retryingJobIds={retryingJobIds}
              running={running}
              selectedCameraNames={selectedCameraNames}
              setForm={(updater) => setForm((current) => updater(current))}
              setSelectedCameraNames={setSelectedCameraNames}
            />
          ) : (
            <Card className="subtle-panel rounded-2xl shadow-none">
              <CardHeader>
                <CardTitle className="text-lg font-semibold">Scene not found</CardTitle>
                <CardDescription>
                  That file id is not in your library.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Button asChild variant="outline">
                  <Link href="/">
                    <ArrowLeft />
                    Back to library
                  </Link>
                </Button>
              </CardContent>
            </Card>
          )
        ) : (
          <LibraryView
            files={files}
            loadingData={loadingData}
            onFileChange={handleSingleFileChange}
            onFolderChange={handleFolderChange}
            onCancelUpload={handleCancelUpload}
            onModeChange={setUploadSourceMode}
            onUpload={handleUpload}
            selectedBlendFile={uploadBlendFile}
            selectionMessage={selectionMessage}
            uploadProgress={uploadProgress}
            uploadStats={uploadStats}
            uploadSourceMode={uploadSourceMode}
            uploading={uploading}
          />
        )}
      </div>
    </div>
  );
}
