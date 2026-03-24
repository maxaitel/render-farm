"use client";

import Image from "next/image";
import type { FormEvent, ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  Cpu,
  Download,
  LoaderCircle,
  Server,
  SquareTerminal,
} from "lucide-react";

import {
  fetchJobs,
  releaseBlendInspection,
  fetchSystemStatus,
  inspectBlendFile,
  type ProjectUploadEntry,
  submitJobWithProgress,
  submitJobsWithProgress,
  touchBlendInspection,
} from "@/lib/api";
import type {
  BlendInspection,
  RenderJob,
  RenderMode,
  SystemStatus,
} from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import logoMark from "../public/logo.png";

type JobFormState = {
  renderMode: RenderMode;
  frame: number;
  startFrame: number;
  endFrame: number;
  outputFormat: "PNG" | "JPEG" | "OPEN_EXR";
  devicePreference: "AUTO" | "CUDA" | "OPTIX" | "CPU";
};

type UploadSourceMode = "files" | "folder";

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

const INITIAL_FORM: JobFormState = {
  renderMode: "still",
  frame: 1,
  startFrame: 1,
  endFrame: 24,
  outputFormat: "PNG",
  devicePreference: "AUTO",
};

function upsertJob(list: RenderJob[], nextJob: RenderJob) {
  const without = list.filter((job) => job.id !== nextJob.id);
  return [nextJob, ...without].sort(
    (left, right) =>
      new Date(right.created_at).getTime() -
      new Date(left.created_at).getTime(),
  );
}

function formatTimestamp(value: string | null) {
  if (!value) {
    return "Pending";
  }
  return new Date(value).toLocaleString();
}

function frameLabel(job: RenderJob) {
  if (job.render_mode === "still") {
    return `Frame ${job.frame ?? 1}`;
  }
  return `Frames ${job.start_frame ?? 1}-${job.end_frame ?? job.start_frame ?? 1}`;
}

function outputLabel(job: RenderJob) {
  if (!job.outputs.length) {
    return "No outputs yet";
  }
  return `${job.outputs.length} file${job.outputs.length === 1 ? "" : "s"} ready`;
}

function cameraLabel(job: RenderJob) {
  if (job.camera_names.length > 1) {
    return `${job.camera_names.length} cameras`;
  }
  if (job.camera_names.length === 1) {
    return `Camera ${job.camera_names[0]}`;
  }
  if (job.camera_name) {
    return `Camera ${job.camera_name}`;
  }
  return null;
}

function activePhase(job: RenderJob) {
  return job.phase === "queued" || job.phase === "running";
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
  projectEntries: ProjectUploadEntry[];
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

function totalFileBytes(files: File[]) {
  return files.reduce((total, file) => total + file.size, 0);
}

function projectFilesFingerprint(files: File[]) {
  return files
    .map((file) =>
      [
        file.webkitRelativePath || file.name,
        file.size,
        file.lastModified,
      ].join(":"),
    )
    .sort()
    .join("|");
}

function inspectionUploadKey(file: File | null, projectFiles: File[]) {
  if (!file) {
    return "";
  }
  return [
    file.webkitRelativePath || "",
    file.name,
    file.size,
    file.lastModified,
    projectFilesFingerprint(projectFiles),
  ].join(":");
}

function cameraScanRequestKey(
  file: File | null,
  projectFiles: File[],
  renderMode: RenderMode,
  scanFrame: number,
) {
  if (!file) {
    return "";
  }
  return [
    file.webkitRelativePath || "",
    file.name,
    file.size,
    file.lastModified,
    projectFilesFingerprint(projectFiles),
    renderMode,
    scanFrame,
  ].join(":");
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

function liveDetail(job: RenderJob) {
  const cameraPrefix = job.current_camera_name
    ? `${job.current_camera_name} • `
    : "";
  if (job.render_mode === "animation" && job.current_frame) {
    return `${cameraPrefix}Frame ${job.current_frame} of ${job.total_frames}`;
  }

  if (job.current_sample !== null && job.total_samples) {
    return `${cameraPrefix}Sample ${job.current_sample} of ${job.total_samples}`;
  }

  return job.resolved_device
    ? `${cameraPrefix}Running on ${job.resolved_device}`
    : "Waiting for worker";
}

function formatElapsedDuration(milliseconds: number) {
  const totalSeconds = milliseconds / 1000;
  if (totalSeconds < 60) {
    return `${totalSeconds.toFixed(1)}s`;
  }

  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}m ${seconds.toFixed(1)}s`;
}

const INSPECTION_TOUCH_INTERVAL_MS = 5 * 60 * 1000;

export function RenderDashboard() {
  const [jobs, setJobs] = useState<RenderJob[]>([]);
  const [system, setSystem] = useState<SystemStatus | null>(null);
  const [form, setForm] = useState<JobFormState>(INITIAL_FORM);
  const [uploadSourceMode, setUploadSourceMode] =
    useState<UploadSourceMode>("files");
  const [selectedFiles, setSelectedFiles] = useState<File[]>([]);
  const [selectedProjectFiles, setSelectedProjectFiles] = useState<File[]>([]);
  const [cameraInspection, setCameraInspection] =
    useState<BlendInspection | null>(null);
  const [selectedCameraNames, setSelectedCameraNames] = useState<string[]>([]);
  const [inspectingCameras, setInspectingCameras] = useState(false);
  const [cameraScanProgress, setCameraScanProgress] = useState(0);
  const [cameraScanPhase, setCameraScanPhase] = useState<
    "uploading" | "processing"
  >("uploading");
  const [cameraScanStartedAt, setCameraScanStartedAt] = useState<number | null>(
    null,
  );
  const [cameraScanElapsedMs, setCameraScanElapsedMs] = useState<number | null>(
    null,
  );
  const [submitting, setSubmitting] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [activeUploadName, setActiveUploadName] = useState<string | null>(null);
  const [activeUploadIndex, setActiveUploadIndex] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const sourcesRef = useRef<Map<string, EventSource>>(new Map());
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const cameraScanRequestRef = useRef(0);
  const cameraScanRequestKeyRef = useRef("");
  const submittingInspectionTokenRef = useRef<string | null>(null);

  const primarySelectedFile = selectedFiles[0] ?? null;
  const cameraScanAvailable = selectedFiles.length === 1;
  const scanFrame = form.renderMode === "still" ? form.frame : form.startFrame;
  const activeInspectionUploadKey = inspectionUploadKey(
    primarySelectedFile,
    selectedProjectFiles,
  );
  const activeCameraScanRequestKey = cameraScanRequestKey(
    primarySelectedFile,
    selectedProjectFiles,
    form.renderMode,
    scanFrame,
  );

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        const [systemPayload, jobsPayload] = await Promise.all([
          fetchSystemStatus(),
          fetchJobs(),
        ]);
        if (cancelled) {
          return;
        }
        setSystem(systemPayload);
        setJobs(jobsPayload);
        setError(null);
      } catch (loadError) {
        if (!cancelled) {
          setError(
            loadError instanceof Error
              ? loadError.message
              : "Failed to load dashboard.",
          );
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }

    void load();
    const intervalId = window.setInterval(() => {
      void fetchSystemStatus()
        .then(setSystem)
        .catch(() => undefined);
      void fetchJobs()
        .then(setJobs)
        .catch(() => undefined);
    }, 15000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
      sourcesRef.current.forEach((source) => source.close());
      sourcesRef.current.clear();
    };
  }, []);

  useEffect(() => {
    const inspectionToken = cameraInspection?.inspection_token;
    return () => {
      if (!inspectionToken) {
        return;
      }
      if (submittingInspectionTokenRef.current === inspectionToken) {
        return;
      }
      void releaseBlendInspection(inspectionToken);
    };
  }, [cameraInspection?.inspection_token]);

  useEffect(() => {
    if (!inspectingCameras || cameraScanStartedAt === null) {
      return;
    }

    setCameraScanElapsedMs(Date.now() - cameraScanStartedAt);
    const intervalId = window.setInterval(() => {
      setCameraScanElapsedMs(Date.now() - cameraScanStartedAt);
    }, 200);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [inspectingCameras, cameraScanStartedAt]);

  useEffect(() => {
    const inspectionToken = cameraInspection?.inspection_token;
    if (!inspectionToken) {
      return;
    }
    if (submittingInspectionTokenRef.current === inspectionToken) {
      return;
    }

    const intervalId = window.setInterval(() => {
      if (submittingInspectionTokenRef.current === inspectionToken) {
        return;
      }
      void touchBlendInspection(inspectionToken)
        .then((stillAvailable) => {
          if (stillAvailable) {
            return;
          }
          let clearedInspection = false;
          setCameraInspection((current) => {
            if (current?.inspection_token !== inspectionToken) {
              return current;
            }
            clearedInspection = true;
            return null;
          });
          if (!clearedInspection) {
            return;
          }
          setSelectedCameraNames([]);
          setCameraScanProgress(0);
          setCameraScanPhase("uploading");
          setCameraScanStartedAt(null);
          setCameraScanElapsedMs(null);
          setError((current) =>
            current ?? "Saved camera scan expired. Rescan cameras or queue the render to upload the scene again.",
          );
        })
        .catch(() => undefined);
    }, INSPECTION_TOUCH_INTERVAL_MS);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [cameraInspection?.inspection_token]);

  useEffect(() => {
    const activeIds = new Set(jobs.filter(activePhase).map((job) => job.id));

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
      const source = new EventSource(`backend/api/jobs/${jobId}/events`);
      source.onmessage = (event) => {
        const payload = JSON.parse(event.data) as RenderJob;
        setJobs((current) => upsertJob(current, payload));
      };
      source.onerror = () => {
        source.close();
        sourcesRef.current.delete(jobId);
      };
      sourcesRef.current.set(jobId, source);
    });
  }, [jobs]);

  useEffect(() => {
    const input = fileInputRef.current;
    if (!input) {
      return;
    }

    input.multiple = true;
    if (uploadSourceMode === "folder") {
      input.setAttribute("webkitdirectory", "");
      input.setAttribute("directory", "");
      input.removeAttribute("accept");
    } else {
      input.removeAttribute("webkitdirectory");
      input.removeAttribute("directory");
      input.setAttribute("accept", ".blend");
    }

    input.value = "";
    setSelectedFiles([]);
    setSelectedProjectFiles([]);
    setCameraInspection(null);
    setSelectedCameraNames([]);
    setCameraScanProgress(0);
    setCameraScanPhase("uploading");
    setCameraScanStartedAt(null);
    setCameraScanElapsedMs(null);
    setError(null);
  }, [uploadSourceMode]);

  useEffect(() => {
    cameraScanRequestRef.current += 1;
    cameraScanRequestKeyRef.current = activeCameraScanRequestKey;
    setInspectingCameras(false);
    setCameraScanProgress(0);
    setCameraScanPhase("uploading");
    setCameraScanStartedAt(null);
    setCameraScanElapsedMs(null);
  }, [activeCameraScanRequestKey]);

  useEffect(() => {
    setInspectingCameras(false);
    setCameraInspection(null);
    setSelectedCameraNames([]);
    setCameraScanProgress(0);
    setCameraScanPhase("uploading");
    setCameraScanStartedAt(null);
    setCameraScanElapsedMs(null);
  }, [activeInspectionUploadKey]);

  const stats = useMemo(() => {
    const running = jobs.filter((job) => job.phase === "running").length;
    const queued = jobs.filter((job) => job.phase === "queued").length;
    const completed = jobs.filter((job) => job.phase === "completed").length;
    return { running, queued, completed };
  }, [jobs]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (inspectingCameras) {
      setError("Wait for camera scanning to finish before queueing the render.");
      return;
    }
    if (!selectedFiles.length) {
      setError(
        uploadSourceMode === "folder"
          ? "Choose a folder with at least one .blend file."
          : "Choose one or more .blend files first.",
      );
      return;
    }

    setSubmitting(true);
    setUploadProgress(0);
    setActiveUploadIndex(1);
    setActiveUploadName(fileLabel(selectedFiles[0]));
    setError(null);
    let nextFileIndex = 0;
    let submittedInspectionToken: string | null = null;
    try {
      for (const [index, file] of selectedFiles.entries()) {
        const payload = new FormData();
        const canReuseInspectionUpload =
          selectedFiles.length === 1 &&
          Boolean(cameraInspection?.inspection_token);
        if (canReuseInspectionUpload && cameraInspection) {
          submittingInspectionTokenRef.current = cameraInspection.inspection_token;
          submittedInspectionToken = cameraInspection.inspection_token;
          payload.set("inspect_token", cameraInspection.inspection_token);
        } else {
          payload.set("blend_file", file, file.name);
          if (uploadSourceMode === "folder") {
            const { blendPath, projectEntries } = folderProjectUploadEntries(
              file,
              selectedProjectFiles,
            );
            payload.set("blend_file_path", blendPath);
            projectEntries.forEach(({ file: projectFile, path }) => {
              payload.append("project_files", projectFile, projectFile.name);
              payload.append("project_paths", path);
            });
          }
        }
        payload.set("render_mode", form.renderMode);
        payload.set("output_format", form.outputFormat);
        payload.set("device_preference", form.devicePreference);
        const requestedCameraNames =
          selectedFiles.length === 1 ? selectedCameraNames : [];
        requestedCameraNames.forEach((cameraName) => {
          payload.append("camera_names", cameraName);
        });
        if (form.renderMode === "still") {
          payload.set("frame", String(form.frame));
        } else {
          payload.set("start_frame", String(form.startFrame));
          payload.set("end_frame", String(form.endFrame));
        }

        setActiveUploadIndex(index + 1);
        setActiveUploadName(fileLabel(file));
        const submittedJobs =
          requestedCameraNames.length > 0
            ? await submitJobsWithProgress(payload, (progress) => {
                const overallProgress =
                  ((index + progress / 100) / selectedFiles.length) * 100;
                setUploadProgress(overallProgress);
              })
            : [
                await submitJobWithProgress(payload, (progress) => {
                  const overallProgress =
                    ((index + progress / 100) / selectedFiles.length) * 100;
                  setUploadProgress(overallProgress);
                }),
              ];
        submittedJobs.forEach((job) => {
          setJobs((current) => upsertJob(current, job));
        });
        nextFileIndex = index + 1;
      }

      if (submittedInspectionToken) {
        await releaseBlendInspection(submittedInspectionToken);
      }
      setSelectedFiles([]);
      setSelectedProjectFiles([]);
      setCameraInspection(null);
      setSelectedCameraNames([]);
      setCameraScanProgress(0);
      setCameraScanPhase("uploading");
      setForm(INITIAL_FORM);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    } catch (submitError) {
      if (selectedFiles.length > 1 && nextFileIndex > 0) {
        setSelectedFiles(selectedFiles.slice(nextFileIndex));
      }
      setError(
        submitError instanceof Error
          ? submitError.message
          : "Failed to queue render.",
      );
    } finally {
      submittingInspectionTokenRef.current = null;
      setSubmitting(false);
      setUploadProgress(0);
      setActiveUploadIndex(0);
      setActiveUploadName(null);
    }
  }

  async function handleInspectCameras() {
    if (!cameraScanAvailable) {
      return;
    }

    setInspectingCameras(true);
    setCameraScanProgress(0);
    setCameraScanPhase("uploading");
    const scanStartedAt = Date.now();
    setCameraScanStartedAt(scanStartedAt);
    setCameraScanElapsedMs(0);
    setError(null);
    const requestId = cameraScanRequestRef.current;
    const expectedScanKey = activeCameraScanRequestKey;
    const previousInspection = cameraInspection;
    const previousSelectedCameraNames = selectedCameraNames;
    try {
      const folderUploadOptions =
        uploadSourceMode === "folder"
          ? folderProjectUploadEntries(selectedFiles[0], selectedProjectFiles)
          : undefined;
      const inspection = await inspectBlendFile(
        selectedFiles[0],
        scanFrame,
        (progress) => {
          if (
            requestId !== cameraScanRequestRef.current ||
            expectedScanKey !== cameraScanRequestKeyRef.current
          ) {
            return;
          }
          setCameraScanProgress(progress);
        },
        (phase) => {
          if (
            requestId !== cameraScanRequestRef.current ||
            expectedScanKey !== cameraScanRequestKeyRef.current
          ) {
            return;
          }
          setCameraScanPhase(phase);
        },
        folderUploadOptions
          ? {
              blendFilePath: folderUploadOptions.blendPath,
              projectFiles: folderUploadOptions.projectEntries,
            }
          : undefined,
      );
      if (
        requestId !== cameraScanRequestRef.current ||
        expectedScanKey !== cameraScanRequestKeyRef.current
      ) {
        void releaseBlendInspection(inspection.inspection_token);
        return;
      }
      setCameraInspection(inspection);
      setSelectedCameraNames([]);
      setCameraScanElapsedMs(Date.now() - scanStartedAt);
    } catch (inspectError) {
      if (
        requestId !== cameraScanRequestRef.current ||
        expectedScanKey !== cameraScanRequestKeyRef.current
      ) {
        return;
      }
      if (previousInspection) {
        setCameraInspection(previousInspection);
        setSelectedCameraNames(previousSelectedCameraNames);
      } else {
        setCameraInspection(null);
        setSelectedCameraNames([]);
      }
      setCameraScanProgress(0);
      setCameraScanPhase("uploading");
      setCameraScanStartedAt(null);
      setCameraScanElapsedMs(null);
      setError(
        inspectError instanceof Error
          ? inspectError.message
          : "Failed to inspect cameras.",
      );
    } finally {
      if (
        requestId === cameraScanRequestRef.current &&
        expectedScanKey === cameraScanRequestKeyRef.current
      ) {
        setInspectingCameras(false);
      }
    }
  }

  function toggleCamera(name: string) {
    setSelectedCameraNames((current) =>
      current.includes(name)
        ? current.filter((cameraName) => cameraName !== name)
        : [...current, name],
    );
  }

  const uploadStageLabel =
    selectedFiles.length > 1
      ? `Uploading ${activeUploadIndex} of ${selectedFiles.length}${activeUploadName ? `: ${activeUploadName}` : ""}`
      : uploadProgress >= 100
        ? "Upload complete. Registering render job."
        : "Uploading blend file to the render host.";

  const cameraScanLabel = inspectingCameras
    ? cameraScanPhase === "uploading"
      ? "Uploading the blend file for camera scanning."
      : "Upload complete. Blender is reading camera names from the scene."
    : cameraInspection?.cameras.length
      ? `${cameraInspection.cameras.length} camera${cameraInspection.cameras.length === 1 ? "" : "s"} found.`
      : "Scan the selected blend file to list cameras and choose one or more render angles.";
  const cameraScanElapsedLabel =
    cameraScanElapsedMs !== null
      ? formatElapsedDuration(cameraScanElapsedMs)
      : null;

  return (
    <main className="min-h-screen bg-paper text-ink">
      <div className="mx-auto max-w-7xl px-4 py-4 sm:px-6 lg:px-8 lg:py-8">
        <section className="dashboard-shell soft-surface overflow-hidden rounded-[2rem] border border-line p-4 shadow-panel md:p-6">
          <div className="grid gap-6 xl:grid-cols-[minmax(0,1.25fr)_380px]">
            <div className="space-y-6">
              <section className="overflow-hidden rounded-[1.85rem] border border-line bg-white text-ink">
                <div className="relative px-5 py-5 md:px-8 md:py-7">
                  <div className="absolute inset-y-0 right-0 w-full bg-[radial-gradient(circle_at_top_right,rgba(207,32,46,0.08),transparent_34%)]" />
                  <div className="relative z-10">
                    <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                      <div>
                        <Image
                          alt="University of Canterbury logo"
                          className="h-12 w-auto object-contain md:h-[3.4rem]"
                          priority
                          src={logoMark}
                          unoptimized
                          width={180}
                          height={56}
                        />
                        <p className="eyebrow mt-3 text-steel">
                          University of Canterbury
                        </p>
                      </div>
                    </div>
                    <div className="mt-3 grid gap-5 lg:grid-cols-[minmax(0,1fr)_17rem] lg:items-center">
                      <div className="max-w-[34rem]">
                        <h1 className="max-w-[8.5ch] font-display text-[2.15rem] leading-[0.95] tracking-[0] sm:text-[2.8rem] xl:text-[3.2rem]">
                          Render Farm
                        </h1>
                      </div>
                      <div className="rounded-[1.35rem] border border-line bg-mist p-5 lg:justify-self-end">
                        <p className="font-subheading text-[11px] uppercase tracking-[0.08em] text-steel">
                          Live Summary
                        </p>
                        <p className="mt-4 text-sm leading-6 text-ink">
                          {loading
                            ? "Syncing queue and system status."
                            : `${jobs.length} job${jobs.length === 1 ? "" : "s"} tracked across the queue.`}
                        </p>
                        <p className="mt-4 font-subheading text-[11px] uppercase tracking-[0.08em] text-steel">
                          {deviceSummary(system)}
                        </p>
                      </div>
                    </div>
                    <div className="mt-7 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                      <Metric label="Running" value={String(stats.running)} />
                      <Metric label="Queued" value={String(stats.queued)} />
                      <Metric label="Finished" value={String(stats.completed)} />
                      <Metric
                        label="Workers"
                        value={String(system?.active_jobs ?? 0)}
                      />
                    </div>
                  </div>
                </div>
              </section>

              <section className="space-y-4">
                <div className="flex items-end justify-between gap-4 px-1">
                  <div>
                    <p className="eyebrow">Queue</p>
                    <h2 className="mt-3 font-subheading text-[2rem] leading-[1.02] tracking-[-0.02em] sm:text-[2.35rem]">
                      Active Jobs
                    </h2>
                  </div>
                  <span className="font-subheading text-xs uppercase tracking-[0.08em] text-steel">
                    {loading ? "Syncing" : `${jobs.length} total`}
                  </span>
                </div>

                {jobs.length === 0 && !loading ? (
                  <Card className="bg-mist">
                    <p className="font-subheading text-[2rem] leading-[1.02] tracking-[-0.02em]">
                      Queue is empty.
                    </p>
                    <p className="mt-3 max-w-md text-sm leading-6 text-steel">
                      Submit a scene from the right-hand panel and it will show
                      up here with progress, logs, and download access.
                    </p>
                  </Card>
                ) : null}

                {jobs.map((job) => (
                  <Card className="space-y-5 overflow-hidden" key={job.id}>
                    <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-3">
                          <Badge phase={job.phase} />
                          <p className="min-w-0 truncate font-subheading text-lg tracking-[0.005em] text-ink md:text-xl">
                            {job.source_filename}
                          </p>
                        </div>
                        <div className="mt-3 flex flex-wrap gap-x-5 gap-y-2 text-sm text-steel">
                          <span>{frameLabel(job)}</span>
                          <span>{job.output_format}</span>
                          {cameraLabel(job) ? <span>{cameraLabel(job)}</span> : null}
                          <span>Requested {job.requested_device}</span>
                          <span>{liveDetail(job)}</span>
                        </div>
                      </div>

                      {job.archive_path ? (
                        <a href={`backend/api/jobs/${job.id}/download`}>
                          <Button variant="secondary">
                            <Download className="mr-2 h-4 w-4" />
                            Download
                          </Button>
                        </a>
                      ) : null}
                    </div>

                    <div>
                      <div className="mb-2 flex items-center justify-between gap-4 text-sm text-steel">
                        <span className="truncate">{job.status_message}</span>
                        <span className="font-subheading text-xs uppercase tracking-[0.08em] text-ink">
                          {Math.round(job.progress)}%
                        </span>
                      </div>
                      <Progress value={job.progress} />
                    </div>

                    <div className="grid gap-3 md:grid-cols-4">
                      <MetaBlock
                        label="Created"
                        value={formatTimestamp(job.created_at)}
                      />
                      <MetaBlock
                        label="Started"
                        value={formatTimestamp(job.started_at)}
                      />
                      <MetaBlock label="Outputs" value={outputLabel(job)} />
                      <MetaBlock
                        label="Device"
                        value={job.resolved_device ?? "Pending"}
                      />
                    </div>

                    {job.logs_tail.length ? (
                      <details className="group rounded-[1.25rem] border border-line bg-mist px-4 py-3">
                        <summary className="flex cursor-pointer items-center justify-between gap-3 font-subheading text-xs uppercase tracking-[0.08em] text-ink">
                          Recent log lines
                          <ChevronDown className="h-4 w-4 text-steel transition-transform duration-200 group-open:rotate-180" />
                        </summary>
                        <pre className="mt-3 max-h-40 overflow-auto whitespace-pre-wrap rounded-[1rem] bg-[#16181b] p-4 text-xs leading-6 text-[#dae4ea]">
                          {job.logs_tail.slice(-8).join("\n")}
                        </pre>
                      </details>
                    ) : null}

                    {job.error ? (
                      <div className="rounded-[1rem] border border-[#e7c6c6] bg-[#fff5f5] px-4 py-3 text-sm text-[#8e3535]">
                        {job.error}
                      </div>
                    ) : null}
                  </Card>
                ))}
              </section>
            </div>

            <aside className="space-y-4">
              <Card>
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="eyebrow">New Job</p>
                    <h2 className="mt-3 font-subheading text-[1.8rem] leading-[1.02] tracking-[-0.02em] sm:text-[2rem]">
                      Submit Render
                    </h2>
                  </div>
                </div>

                <form className="mt-8 space-y-5" onSubmit={handleSubmit}>
                  <div>
                    <span className="mb-2 block font-subheading text-[11px] uppercase tracking-[0.08em] text-ink/62">
                      Upload source
                    </span>
                    <div className="grid grid-cols-2 gap-2 rounded-[1.15rem] bg-black/[0.04] p-1">
                      <ModeButton
                        active={uploadSourceMode === "files"}
                        label="Files"
                        onClick={() => setUploadSourceMode("files")}
                      />
                      <ModeButton
                        active={uploadSourceMode === "folder"}
                        label="Folder"
                        onClick={() => setUploadSourceMode("folder")}
                      />
                    </div>
                  </div>

                  <Label
                    title={
                      uploadSourceMode === "folder"
                        ? "Blend folder"
                        : "Blend files"
                    }
                  >
                    <input
                      id="blend-file"
                      className="block w-full rounded-[1rem] border border-dashed border-line bg-white px-4 py-4 text-sm text-ink outline-none transition file:mr-4 file:rounded-full file:border-0 file:bg-ember file:px-4 file:py-2 file:text-sm file:font-semibold file:tracking-[0] file:text-white hover:border-ember/30 focus:border-ember"
                      multiple
                      onChange={(event) => {
                        const allFiles = Array.from(event.target.files ?? []);
                        const files =
                          uploadSourceMode === "folder"
                            ? folderRenderTargets(allFiles)
                            : allFiles.filter((file) =>
                                file.name.toLowerCase().endsWith(".blend"),
                              );
                        setSelectedFiles(files);
                        setSelectedProjectFiles(
                          uploadSourceMode === "folder" ? allFiles : files,
                        );
                        setCameraInspection(null);
                        setSelectedCameraNames([]);
                        setCameraScanProgress(0);
                        setCameraScanPhase("uploading");
                        if (!files.length && event.target.files?.length) {
                          setError("Only .blend files are accepted.");
                        } else {
                          setError(null);
                        }
                      }}
                      ref={fileInputRef}
                      type="file"
                    />
                  </Label>

                  {selectedFiles.length ? (
                    <div className="rounded-[1.2rem] border border-line bg-white px-4 py-4">
                      <div className="flex items-start justify-between gap-4">
                        <div className="min-w-0">
                          <p className="truncate font-subheading text-sm tracking-[0] text-ink">
                            {selectedFiles.length === 1
                              ? fileLabel(selectedFiles[0])
                              : `${selectedFiles.length} blend files selected`}
                          </p>
                          <p className="mt-1 text-sm text-steel">
                            {formatBytes(
                              totalFileBytes(
                                uploadSourceMode === "folder"
                                  ? selectedProjectFiles
                                  : selectedFiles,
                              ),
                            )}
                          </p>
                        </div>
                        <span className="rounded-full bg-sand px-3 py-1 font-subheading text-[11px] uppercase tracking-[0.08em] text-steel">
                          {submitting ? "Uploading" : "Ready"}
                        </span>
                      </div>

                      {selectedFiles.length > 1 ? (
                        <div className="mt-4 space-y-1 text-sm text-steel">
                          {selectedFiles.slice(0, 3).map((file) => (
                            <p className="truncate" key={fileLabel(file)}>
                              {fileLabel(file)}
                            </p>
                          ))}
                          {selectedFiles.length > 3 ? (
                            <p>
                              +{selectedFiles.length - 3} more file
                              {selectedFiles.length - 3 === 1 ? "" : "s"}
                            </p>
                          ) : null}
                        </div>
                      ) : null}

                      {submitting ? (
                        <div className="mt-4">
                          <div className="mb-2 flex items-center justify-between text-sm text-steel">
                            <span>{uploadStageLabel}</span>
                            <span>{Math.round(uploadProgress)}%</span>
                          </div>
                          <Progress value={uploadProgress} />
                        </div>
                      ) : (
                        <p className="mt-4 text-sm leading-6 text-steel">
                          {uploadSourceMode === "folder"
                            ? "Using the primary scene .blend from the selected folder, while sibling assets stay attached to the job."
                            : "Large scenes take a moment to transfer before they are visible in the queue."}
                        </p>
                      )}
                    </div>
                  ) : null}

                  {selectedFiles.length ? (
                    <div className="rounded-[1.2rem] border border-line bg-mist px-4 py-4">
                      <div className="flex items-start justify-between gap-4">
                        <div>
                          <p className="font-subheading text-sm text-ink">
                            Camera selection
                          </p>
                          <p className="mt-1 text-sm text-steel">
                            {cameraScanAvailable
                              ? cameraScanLabel
                              : "Camera scanning is available when one blend file is selected at a time."}
                          </p>
                          {cameraInspection ? (
                            <p className="mt-1 text-sm text-steel">
                              This scan upload will be reused when you queue the render.
                            </p>
                          ) : null}
                        </div>
                        {cameraScanAvailable ? (
                          <Button
                            disabled={inspectingCameras || submitting}
                            onClick={handleInspectCameras}
                            type="button"
                            variant="secondary"
                          >
                            {inspectingCameras
                              ? "Scanning cameras"
                              : cameraInspection
                                ? "Rescan cameras"
                                : "Scan cameras"}
                          </Button>
                        ) : null}
                      </div>

                      {inspectingCameras ? (
                        <div className="mt-4 rounded-[1rem] border border-line bg-white px-4 py-3">
                          <div className="flex items-center gap-3 text-sm text-ink">
                            <LoaderCircle className="h-4 w-4 animate-spin text-ember" />
                            <span>
                              {cameraScanPhase === "uploading"
                                ? "Uploading blend for camera scan."
                                : "Reading camera names in Blender."}
                            </span>
                          </div>
                          <div className="mt-3 flex items-center justify-between gap-3 text-sm text-steel">
                            <span>{cameraScanLabel}</span>
                            <div className="text-right">
                              {cameraScanElapsedLabel ? (
                                <p>{cameraScanElapsedLabel}</p>
                              ) : null}
                              <p>{Math.round(cameraScanProgress)}%</p>
                            </div>
                          </div>
                          <div className="mt-3">
                            <Progress value={cameraScanProgress} />
                          </div>
                        </div>
                      ) : null}

                      {cameraInspection?.cameras.length ? (
                        <div className="mt-4 space-y-3">
                          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                            <p className="text-sm text-steel">
                              Scan frame {cameraInspection.frame}. Selected{" "}
                              {selectedCameraNames.length || 0} camera
                              {selectedCameraNames.length === 1 ? "" : "s"}.
                              {cameraScanElapsedLabel
                                ? ` Scan took ${cameraScanElapsedLabel}.`
                                : ""}
                            </p>
                            <div className="flex gap-2">
                              <Button
                                disabled={!cameraInspection.cameras.length}
                                onClick={() =>
                                  setSelectedCameraNames(
                                    cameraInspection.cameras.map((camera) => camera.name),
                                  )
                                }
                                type="button"
                                variant="secondary"
                              >
                                Select all
                              </Button>
                              <Button
                                disabled={!selectedCameraNames.length}
                                onClick={() => setSelectedCameraNames([])}
                                type="button"
                                variant="secondary"
                              >
                                Clear
                              </Button>
                            </div>
                          </div>
                          <div className="grid gap-2 sm:grid-cols-2">
                            {cameraInspection.cameras.map((camera) => {
                              const active = selectedCameraNames.includes(
                                camera.name,
                              );
                              const isDefaultCamera =
                                cameraInspection.default_camera === camera.name;

                              return (
                                <label
                                  className={`flex cursor-pointer items-center justify-between gap-3 rounded-[1rem] border px-3 py-3 transition ${
                                    active
                                      ? "border-ember bg-white shadow-[0_10px_24px_rgba(207,32,46,0.08)]"
                                      : "border-line bg-white hover:border-ink/25"
                                  }`}
                                  key={camera.name}
                                >
                                  <div className="min-w-0">
                                    <p className="truncate font-subheading text-sm text-ink">
                                      {camera.name}
                                    </p>
                                    <p className="mt-1 text-xs text-steel">
                                      {isDefaultCamera
                                        ? "Default scene camera"
                                        : "Additional camera"}
                                    </p>
                                  </div>
                                  <input
                                    checked={active}
                                    className="h-4 w-4 accent-[#cf202e]"
                                    onChange={() => toggleCamera(camera.name)}
                                    type="checkbox"
                                  />
                                </label>
                              );
                            })}
                          </div>
                        </div>
                      ) : cameraInspection ? (
                        <p className="mt-4 text-sm text-steel">
                          No cameras were found in this blend file.
                          {cameraScanElapsedLabel
                            ? ` Scan took ${cameraScanElapsedLabel}.`
                            : ""}
                        </p>
                      ) : null}
                    </div>
                  ) : null}

                  <div>
                    <span className="mb-2 block font-subheading text-[11px] uppercase tracking-[0.08em] text-ink/62">
                      Render mode
                    </span>
                    <div className="grid grid-cols-2 gap-2 rounded-[1.15rem] bg-black/[0.04] p-1">
                      <ModeButton
                        active={form.renderMode === "still"}
                        label="Still frame"
                        onClick={() =>
                          setForm((current) => ({
                            ...current,
                            renderMode: "still",
                          }))
                        }
                      />
                      <ModeButton
                        active={form.renderMode === "animation"}
                        label="Animation"
                        onClick={() =>
                          setForm((current) => ({
                            ...current,
                            renderMode: "animation",
                          }))
                        }
                      />
                    </div>
                  </div>

                  <div className="grid gap-4 md:grid-cols-2">
                    <Label title="Output format">
                      <select
                        className="field"
                        value={form.outputFormat}
                        onChange={(event) =>
                          setForm((current) => ({
                            ...current,
                            outputFormat: event.target
                              .value as JobFormState["outputFormat"],
                          }))
                        }
                      >
                        <option value="PNG">PNG</option>
                        <option value="JPEG">JPEG</option>
                        <option value="OPEN_EXR">OpenEXR</option>
                      </select>
                    </Label>

                    <Label title="Device preference">
                      <select
                        className="field"
                        value={form.devicePreference}
                        onChange={(event) =>
                          setForm((current) => ({
                            ...current,
                            devicePreference: event.target
                              .value as JobFormState["devicePreference"],
                          }))
                        }
                      >
                        <option value="AUTO">Auto</option>
                        <option value="CUDA">CUDA</option>
                        <option value="OPTIX">OptiX</option>
                        <option value="CPU">CPU</option>
                      </select>
                    </Label>
                  </div>

                  {form.renderMode === "still" ? (
                    <Label title="Frame">
                      <input
                        className="field"
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
                    </Label>
                  ) : (
                    <div className="grid gap-4 md:grid-cols-2">
                      <Label title="Start frame">
                        <input
                          className="field"
                          min={1}
                          type="number"
                          value={form.startFrame}
                          onChange={(event) =>
                            setForm((current) => ({
                              ...current,
                              startFrame: Number(event.target.value || 1),
                            }))
                          }
                        />
                      </Label>

                      <Label title="End frame">
                        <input
                          className="field"
                          min={form.startFrame}
                          type="number"
                          value={form.endFrame}
                          onChange={(event) =>
                            setForm((current) => ({
                              ...current,
                              endFrame: Number(
                                event.target.value || current.startFrame,
                              ),
                            }))
                          }
                        />
                      </Label>
                    </div>
                  )}

                  {error ? (
                    <div className="rounded-[1rem] border border-[#e5c6c6] bg-[#fff7f7] px-4 py-3 text-sm text-[#8a2e2e]">
                      {error}
                    </div>
                  ) : null}

                  <Button
                    className="w-full"
                    disabled={submitting || loading || inspectingCameras}
                    type="submit"
                  >
                    {submitting
                      ? `Uploading ${Math.round(uploadProgress)}%`
                      : inspectingCameras
                        ? `Scanning ${Math.round(cameraScanProgress)}%`
                      : selectedCameraNames.length > 1 && selectedFiles.length === 1
                        ? `Queue render for ${selectedCameraNames.length} cameras`
                      : selectedFiles.length > 1
                        ? `Queue ${selectedFiles.length} renders`
                        : "Queue render"}
                  </Button>
                </form>
              </Card>

              <Card>
                <p className="eyebrow">System</p>
                <div className="mt-5 space-y-3">
                  <StatusRow
                    icon={<Server className="h-4 w-4" />}
                    label="GPU runtime"
                    value={system?.gpu ?? "Loading"}
                  />
                  <StatusRow
                    icon={<SquareTerminal className="h-4 w-4" />}
                    label="Blender"
                    value={system?.blender ?? "Loading"}
                  />
                  <StatusRow
                    icon={<Cpu className="h-4 w-4" />}
                    label="Device policy"
                    value={
                      system
                        ? `${system.device_policy.default} first, then ${system.device_policy.order.join(" / ")}`
                        : "Loading"
                    }
                  />
                  <StatusRow
                    icon={<Cpu className="h-4 w-4" />}
                    label="Detected devices"
                    value={deviceSummary(system)}
                  />
                </div>
              </Card>

              <Card className="bg-mist">
                <p className="font-subheading text-xs uppercase tracking-[0.08em] text-ink">
                  Notes
                </p>
                <p className="mt-3 text-sm leading-6 text-steel">
                  Jobs update automatically. Use the queue to watch progress,
                  inspect recent logs, and download archives when they finish.
                </p>
              </Card>
            </aside>
          </div>
        </section>
      </div>
    </main>
  );
}

function Label({ title, children }: { title: string; children: ReactNode }) {
  return (
    <label className="block">
      <span className="mb-2 block font-subheading text-[11px] uppercase tracking-[0.08em] text-ink/62">
        {title}
      </span>
      {children}
    </label>
  );
}

function ModeButton({
  active,
  label,
  onClick,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      className={
        active
          ? "rounded-[0.95rem] bg-ember px-3 py-3 font-subheading text-[11px] uppercase tracking-[0.08em] text-white transition"
          : "rounded-[0.95rem] px-3 py-3 font-subheading text-[11px] uppercase tracking-[0.08em] text-steel transition hover:bg-white"
      }
      onClick={onClick}
      type="button"
    >
      {label}
    </button>
  );
}

function Metric({
  icon,
  label,
  value,
}: {
  icon?: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[1.35rem] border border-line bg-mist p-5">
      {icon ? <div className="flex items-center gap-2 text-ember">{icon}</div> : null}
      <p
        className={`font-subheading text-[11px] uppercase tracking-[0.08em] text-steel ${icon ? "mt-5" : "mt-0"}`}
      >
        {label}
      </p>
      <p className="mt-3 font-subheading text-[1.9rem] leading-none tracking-[-0.03em] text-ink">
        {value}
      </p>
    </div>
  );
}

function StatusRow({
  icon,
  label,
  value,
}: {
  icon: ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="rounded-[1.2rem] border border-line bg-white px-4 py-3">
      <div className="flex items-center gap-3">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-[rgba(207,32,46,0.08)] text-ember">
          {icon}
        </div>
        <div className="min-w-0">
          <p className="font-subheading text-[11px] uppercase tracking-[0.08em] text-steel">
            {label}
          </p>
          <p className="truncate text-sm text-ink">{value}</p>
        </div>
      </div>
    </div>
  );
}

function MetaBlock({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[1.1rem] border border-line bg-mist px-4 py-3">
      <p className="font-subheading text-[11px] uppercase tracking-[0.08em] text-steel">
        {label}
      </p>
      <p className="mt-2 text-sm text-ink">{value}</p>
    </div>
  );
}
