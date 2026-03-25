"use client";

import Image from "next/image";
import Link from "next/link";
import type { ChangeEvent, FormEvent } from "react";
import { useEffect, useRef, useState } from "react";
import {
  ArrowLeft,
  ChevronRight,
  Cpu,
  Download,
  FolderOpen,
  LoaderCircle,
  LogOut,
  Radar,
  Shield,
  Upload,
} from "lucide-react";

import {
  createRun,
  fetchAdminActivity,
  fetchAdminOverview,
  fetchAdminRuns,
  fetchAdminUsers,
  fetchFiles,
  fetchSession,
  fetchSystemStatus,
  inspectStoredFile,
  signIn,
  signOut,
  signUp,
  updateAdminUserStatus,
  uploadFileWithProgress,
} from "@/lib/api";
import type {
  ActivityRecord,
  AdminOverview,
  AuthSession,
  BlendInspection,
  RenderJob,
  RenderMode,
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
};

const INITIAL_FORM: JobFormState = {
  renderMode: "still",
  frame: 1,
  startFrame: 0,
  endFrame: 24,
  outputFormat: "PNG",
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
  return job.phase === "queued" || job.phase === "running";
}

function frameLabel(job: RenderJob) {
  if (job.render_mode === "still") {
    return `Frame ${job.frame ?? 1}`;
  }
  return `Frames ${job.start_frame ?? 1}-${job.end_frame ?? job.start_frame ?? 1}`;
}

function formatDuration(seconds: number) {
  const totalSeconds = Math.max(1, Math.round(seconds));
  if (totalSeconds < 60) {
    return `${totalSeconds}s`;
  }

  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, "0")}m`;
  }
  return `${minutes}m ${String(remainingSeconds).padStart(2, "0")}s`;
}

function currentFrameValue(job: RenderJob) {
  if (job.current_frame !== null) {
    return job.current_frame;
  }
  if (job.phase !== "running") {
    return null;
  }
  if (job.render_mode === "still") {
    return job.frame;
  }
  return job.start_frame;
}

function currentFrameText(job: RenderJob) {
  const frame = currentFrameValue(job);
  if (frame === null) {
    return null;
  }
  if (job.render_mode === "still") {
    return `Current frame ${frame}`;
  }
  const start = job.start_frame ?? frame;
  const end = job.end_frame ?? start;
  return `Current frame ${frame} of ${start}-${end}`;
}

function timePerFrameText(job: RenderJob) {
  const current =
    job.phase === "running" ? job.current_frame_elapsed_seconds : null;
  if (current !== null) {
    return `Time / frame ${formatDuration(current)} so far`;
  }
  if (job.average_frame_duration_seconds !== null) {
    return `Time / frame ${formatDuration(job.average_frame_duration_seconds)} avg`;
  }
  if (job.last_frame_duration_seconds !== null) {
    return `Time / frame ${formatDuration(job.last_frame_duration_seconds)}`;
  }
  return null;
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
    ? `${job.current_camera_name} • `
    : "";
  if (job.render_mode === "animation" && job.current_frame !== null) {
    const start = job.start_frame ?? job.current_frame;
    const end = job.end_frame ?? start;
    return `${cameraPrefix}Frame ${job.current_frame} of ${start}-${end}`;
  }
  if (job.current_sample !== null && job.total_samples) {
    return `${cameraPrefix}Sample ${job.current_sample} of ${job.total_samples}`;
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
  if (job.phase === "failed") {
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

function upsertAdminRun(jobs: RenderJob[], nextRun: RenderJob) {
  return [nextRun, ...jobs.filter((job) => job.id !== nextRun.id)].sort(
    (left, right) =>
      new Date(right.created_at).getTime() - new Date(left.created_at).getTime(),
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
  onModeChange,
  onSubmit,
  selectedBlendFile,
  selectionMessage,
  uploadProgress,
  uploadSourceMode,
  uploading,
}: {
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onFolderChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onModeChange: (mode: UploadSourceMode) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  selectedBlendFile: File | null;
  selectionMessage: string | null;
  uploadProgress: number;
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
              <TabsTrigger value="file">Single blend</TabsTrigger>
              <TabsTrigger value="folder">Project folder</TabsTrigger>
            </TabsList>
            <TabsContent value="file" className="space-y-4">
              <Input accept=".blend" onChange={onFileChange} type="file" />
            </TabsContent>
            <TabsContent value="folder" className="space-y-4">
              <Input
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
          {uploading ? <Progress value={uploadProgress} /> : null}
          <Button className="w-full" disabled={uploading || !selectedBlendFile}>
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
            {file.latest_job ? (
              <div className="space-y-2 rounded-lg border bg-muted/20 p-3 text-sm">
                <p className="text-muted-foreground">
                  {cameraLabel(file.latest_job)} • {frameLabel(file.latest_job)}
                </p>
                <p className="text-foreground/80">
                  {activePhase(file.latest_job)
                    ? liveDetail(file.latest_job)
                    : file.latest_job.error || file.latest_job.status_message}
                </p>
                {currentFrameText(file.latest_job) || timePerFrameText(file.latest_job) ? (
                  <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                    {currentFrameText(file.latest_job) ? (
                      <span>{currentFrameText(file.latest_job)}</span>
                    ) : null}
                    {timePerFrameText(file.latest_job) ? (
                      <span>{timePerFrameText(file.latest_job)}</span>
                    ) : null}
                  </div>
                ) : null}
                {activePhase(file.latest_job) ? <Progress value={file.latest_job.progress} /> : null}
              </div>
            ) : null}
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
  onModeChange,
  onUpload,
  selectedBlendFile,
  selectionMessage,
  uploadProgress,
  uploadSourceMode,
  uploading,
}: {
  files: UserFile[];
  loadingData: boolean;
  onFileChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onFolderChange: (event: ChangeEvent<HTMLInputElement>) => void;
  onModeChange: (mode: UploadSourceMode) => void;
  onUpload: (event: FormEvent<HTMLFormElement>) => void;
  selectedBlendFile: File | null;
  selectionMessage: string | null;
  uploadProgress: number;
  uploadSourceMode: UploadSourceMode;
  uploading: boolean;
}) {
  return (
    <div className="grid gap-6 lg:grid-cols-[320px_minmax(0,1fr)]">
      <UploadCard
        onFileChange={onFileChange}
        onFolderChange={onFolderChange}
        onModeChange={onModeChange}
        onSubmit={onUpload}
        selectedBlendFile={selectedBlendFile}
        selectionMessage={selectionMessage}
        uploadProgress={uploadProgress}
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

function FileDetailView({
  cameraInspection,
  file,
  form,
  inspecting,
  onInspect,
  onRun,
  running,
  selectedCameraNames,
  setForm,
  setSelectedCameraNames,
}: {
  cameraInspection: BlendInspection | null;
  file: UserFile;
  form: JobFormState;
  inspecting: boolean;
  onInspect: () => void;
  onRun: (event: FormEvent<HTMLFormElement>) => void;
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
                            startFrame: Number(event.target.value || 0),
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
                      {currentFrameText(job) || timePerFrameText(job) ? (
                        <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm text-muted-foreground">
                          {currentFrameText(job) ? <span>{currentFrameText(job)}</span> : null}
                          {timePerFrameText(job) ? <span>{timePerFrameText(job)}</span> : null}
                        </div>
                      ) : null}
                    </div>
                    {job.archive_path ? (
                      <Button asChild size="sm" variant="outline">
                        <a href={`/backend/api/jobs/${job.id}/download`}>
                          <Download />
                          Download
                        </a>
                      </Button>
                    ) : null}
                  </div>
                  {activePhase(job) ? <Progress value={job.progress} /> : null}
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
  adminOverview,
  adminRuns,
  adminUsers,
  onStatusChange,
}: {
  adminActivity: ActivityRecord[];
  adminOverview: AdminOverview | null;
  adminRuns: RenderJob[];
  adminUsers: UserAccount[];
  onStatusChange: (userId: number, status: UserStatus) => void;
}) {
  const runningRuns = adminRuns
    .filter((job) => job.phase === "running")
    .sort(
      (left, right) =>
        new Date(left.started_at ?? left.created_at).getTime() -
        new Date(right.started_at ?? right.created_at).getTime(),
    );
  const queuedRuns = adminRuns
    .filter((job) => job.phase === "queued")
    .sort(
      (left, right) =>
        new Date(left.created_at).getTime() - new Date(right.created_at).getTime(),
    );
  const recentFinishedRuns = adminRuns.filter(
    (job) => job.phase === "completed" || job.phase === "failed",
  );

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

      <div className="grid gap-6 xl:grid-cols-2">
        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">Running jobs</CardTitle>
            <CardDescription>
              Live renders with progress, frame state, and log tails.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {runningRuns.length ? (
              runningRuns.map((job) => (
                <Card className="rounded-xl shadow-none" key={job.id}>
                  <CardContent className="space-y-4 p-4">
                    <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                      <div className="space-y-2">
                        <div className="flex flex-wrap items-center gap-2">
                          <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                          <span className="text-sm font-medium">{job.source_filename}</span>
                        </div>
                        <p className="text-sm text-muted-foreground">
                          User {job.user_id} • {cameraLabel(job)} • {frameLabel(job)}
                        </p>
                        <p className="text-sm text-muted-foreground">
                          Queued {formatTimestamp(job.created_at)}
                        </p>
                        <p className="text-sm text-foreground/80">{liveDetail(job)}</p>
                        {currentFrameText(job) || timePerFrameText(job) ? (
                          <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm text-muted-foreground">
                            {currentFrameText(job) ? <span>{currentFrameText(job)}</span> : null}
                            {timePerFrameText(job) ? <span>{timePerFrameText(job)}</span> : null}
                          </div>
                        ) : null}
                      </div>
                      {job.archive_path ? (
                        <Button asChild size="sm" variant="outline">
                          <a href={`/backend/api/jobs/${job.id}/download`}>
                            <Download />
                            Download
                          </a>
                        </Button>
                      ) : null}
                    </div>
                    <Progress value={job.progress} />
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
                No renders are running right now.
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">Queue</CardTitle>
            <CardDescription>
              Jobs waiting for a worker slot.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {queuedRuns.length ? (
              queuedRuns.map((job) => (
                <Card className="rounded-xl shadow-none" key={job.id}>
                  <CardContent className="space-y-4 p-4">
                    <div className="space-y-2">
                      <div className="flex flex-wrap items-center gap-2">
                        <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                        <span className="text-sm font-medium">{job.source_filename}</span>
                      </div>
                      <p className="text-sm text-muted-foreground">
                        User {job.user_id} • {cameraLabel(job)} • {frameLabel(job)}
                      </p>
                      <p className="text-sm text-muted-foreground">
                        Queued {formatTimestamp(job.created_at)}
                      </p>
                      <p className="text-sm text-foreground/80">
                        {job.error || job.status_message}
                      </p>
                    </div>
                    <Progress value={job.progress} />
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
                The queue is empty.
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 xl:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
        <Card className="subtle-panel rounded-2xl shadow-none">
          <CardHeader>
            <CardTitle className="text-lg font-semibold">Users</CardTitle>
            <CardDescription>Approve or suspend access.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            {adminUsers.map((user) => (
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
              <CardTitle className="text-lg font-semibold">Recent finished runs</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {recentFinishedRuns.slice(0, 8).map((job) => (
                <div className="rounded-lg border p-4" key={job.id}>
                  <div className="flex items-center justify-between gap-3">
                    <p className="truncate text-sm font-medium">{job.source_filename}</p>
                    <Badge variant={runBadgeVariant(job)}>{job.phase}</Badge>
                  </div>
                  <p className="mt-2 text-xs text-muted-foreground">
                    User {job.user_id} • {cameraLabel(job)} • {frameLabel(job)}
                  </p>
                  <p className="mt-2 text-xs text-muted-foreground">
                    {job.error || job.status_message}
                  </p>
                  {timePerFrameText(job) ? (
                    <p className="mt-2 text-xs text-muted-foreground">
                      {timePerFrameText(job)}
                    </p>
                  ) : null}
                </div>
              ))}
              {!recentFinishedRuns.length ? (
                <div className="rounded-lg border border-dashed px-4 py-10 text-center text-sm text-muted-foreground">
                  No completed or failed jobs yet.
                </div>
              ) : null}
            </CardContent>
          </Card>
        </div>
      </div>
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
  const [running, setRunning] = useState(false);
  const [inspecting, setInspecting] = useState(false);
  const [booting, setBooting] = useState(true);
  const [loadingData, setLoadingData] = useState(false);
  const [selectionMessage, setSelectionMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [adminOverview, setAdminOverview] = useState<AdminOverview | null>(null);
  const [adminUsers, setAdminUsers] = useState<UserAccount[]>([]);
  const [adminActivity, setAdminActivity] = useState<ActivityRecord[]>([]);
  const [adminRuns, setAdminRuns] = useState<RenderJob[]>([]);
  const sourcesRef = useRef<Map<string, EventSource>>(new Map());
  const activeJobIds = Array.from(
    new Set(
      (view === "admin" ? adminRuns : files.flatMap((file) => file.jobs))
        .filter(activePhase)
        .map((job) => job.id),
    ),
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
      const [overviewPayload, usersPayload, activityPayload, runsPayload] =
        await Promise.all([
          fetchAdminOverview(),
          fetchAdminUsers(),
          fetchAdminActivity(),
          fetchAdminRuns(),
        ]);
      setAdminOverview(overviewPayload);
      setAdminUsers(usersPayload);
      setAdminActivity(activityPayload);
      setAdminRuns(runsPayload);
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
        if (view === "admin") {
          setAdminRuns((current) => upsertAdminRun(current, payload));
        }
      };
      source.onerror = () => {
        source.close();
        sourcesRef.current.delete(jobId);
      };
      sourcesRef.current.set(jobId, source);
    });
  }, [activeJobIdsKey, activeJobSessionKey, view]);

  useEffect(() => {
    return () => {
      sourcesRef.current.forEach((source) => source.close());
      sourcesRef.current.clear();
    };
  }, []);

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
    try {
      await uploadFileWithProgress(formData, setUploadProgress);
      setUploadBlendFile(null);
      setUploadProjectFiles([]);
      setSelectionMessage("Scene added to the library.");
      setError(null);
      void refreshCoreData();
    } catch (uploadError) {
      setError(uploadError instanceof Error ? uploadError.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
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
            adminOverview={adminOverview}
            adminRuns={adminRuns}
            adminUsers={adminUsers}
            onStatusChange={(userId, status) => void handleAdminStatus(userId, status)}
          />
        ) : view === "detail" ? (
          selectedFile ? (
            <FileDetailView
              cameraInspection={cameraInspection}
              file={selectedFile}
              form={form}
              inspecting={inspecting}
              onInspect={() => void handleInspect()}
              onRun={handleCreateRun}
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
            onModeChange={setUploadSourceMode}
            onUpload={handleUpload}
            selectedBlendFile={uploadBlendFile}
            selectionMessage={selectionMessage}
            uploadProgress={uploadProgress}
            uploadSourceMode={uploadSourceMode}
            uploading={uploading}
          />
        )}
      </div>
    </div>
  );
}
