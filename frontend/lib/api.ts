import type { BlendInspection, RenderJob, SystemStatus } from "@/lib/types";

export type ProjectUploadEntry = {
  file: File;
  path: string;
};

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ detail: "Request failed." }));
    throw new Error(payload.detail ?? "Request failed.");
  }
  return response.json() as Promise<T>;
}

export async function fetchSystemStatus(): Promise<SystemStatus> {
  const response = await fetch("backend/api/system", { cache: "no-store" });
  return parseResponse<SystemStatus>(response);
}

export async function fetchJobs(): Promise<RenderJob[]> {
  const response = await fetch("backend/api/jobs", { cache: "no-store" });
  return parseResponse<RenderJob[]>(response);
}

export async function submitJob(formData: FormData): Promise<RenderJob> {
  return new Promise<RenderJob>((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", "backend/api/jobs");
    request.responseType = "json";

    request.onload = () => {
      const payload =
        request.response ??
        (request.responseText ? (JSON.parse(request.responseText) as RenderJob | { detail?: string }) : null);

      if (request.status >= 200 && request.status < 300 && payload) {
        resolve(payload as RenderJob);
        return;
      }

      const detail = payload && "detail" in payload ? payload.detail : null;
      reject(new Error(detail ?? "Request failed."));
    };

    request.onerror = () => {
      reject(new Error("Upload failed."));
    };

    request.onabort = () => {
      reject(new Error("Upload cancelled."));
    };

    request.send(formData);
  });
}

export async function submitJobWithProgress(
  formData: FormData,
  onProgress: (progress: number) => void,
): Promise<RenderJob> {
  return submitWithProgress<RenderJob>("backend/api/jobs", formData, onProgress);
}

export async function submitJobsWithProgress(
  formData: FormData,
  onProgress: (progress: number) => void,
): Promise<RenderJob[]> {
  return submitWithProgress<RenderJob[]>("backend/api/jobs/batch", formData, onProgress);
}

export async function inspectBlendFile(
  file: File,
  frame: number | undefined,
  onProgress: (progress: number) => void,
  onPhaseChange: (phase: "uploading" | "processing") => void,
  options?: {
    blendFilePath?: string;
    projectFiles?: ProjectUploadEntry[];
  },
): Promise<BlendInspection> {
  return new Promise<BlendInspection>((resolve, reject) => {
    const formData = new FormData();
    formData.set("blend_file", file, file.name);
    if (options?.blendFilePath) {
      formData.set("blend_file_path", options.blendFilePath);
    }
    options?.projectFiles?.forEach(({ file: projectFile, path }) => {
      formData.append("project_files", projectFile, projectFile.name);
      formData.append("project_paths", path);
    });
    if (frame) {
      formData.set("frame", String(frame));
    }

    const request = new XMLHttpRequest();
    let processingTimer: number | null = null;
    let currentProgress = 0;

    const startProcessingProgress = () => {
      onPhaseChange("processing");
      currentProgress = Math.max(currentProgress, 90);
      onProgress(currentProgress);
      processingTimer = window.setInterval(() => {
        currentProgress = Math.min(98, currentProgress + 1);
        onProgress(currentProgress);
      }, 350);
    };

    const clearProcessingProgress = () => {
      if (processingTimer !== null) {
        window.clearInterval(processingTimer);
        processingTimer = null;
      }
    };

    request.open("POST", "backend/api/blend-inspect");
    request.responseType = "json";

    request.upload.onprogress = (event) => {
      if (!event.lengthComputable || event.total === 0) {
        return;
      }
      onPhaseChange("uploading");
      currentProgress = Math.min(89, (event.loaded / event.total) * 89);
      onProgress(currentProgress);
    };

    request.upload.onload = () => {
      startProcessingProgress();
    };

    request.onload = () => {
      clearProcessingProgress();
      const payload =
        request.response ??
        (request.responseText
          ? (JSON.parse(request.responseText) as BlendInspection | { detail?: string })
          : null);

      if (request.status >= 200 && request.status < 300 && payload) {
        onProgress(100);
        resolve(payload as BlendInspection);
        return;
      }

      const detail = payload && "detail" in payload ? payload.detail : null;
      reject(new Error(detail ?? "Request failed."));
    };

    request.onerror = () => {
      clearProcessingProgress();
      reject(new Error("Camera scan failed."));
    };

    request.onabort = () => {
      clearProcessingProgress();
      reject(new Error("Camera scan cancelled."));
    };

    request.send(formData);
  });
}

export async function releaseBlendInspection(
  inspectionToken: string,
): Promise<void> {
  await fetch(`backend/api/blend-inspect/${inspectionToken}`, {
    method: "DELETE",
    keepalive: true,
  }).catch(() => undefined);
}

export async function touchBlendInspection(
  inspectionToken: string,
): Promise<void> {
  await fetch(`backend/api/blend-inspect/${inspectionToken}/touch`, {
    method: "POST",
    keepalive: true,
  }).catch(() => undefined);
}

function submitWithProgress<T>(
  url: string,
  formData: FormData,
  onProgress: (progress: number) => void,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", url);
    request.responseType = "json";

    request.upload.onprogress = (event) => {
      if (!event.lengthComputable || event.total === 0) {
        return;
      }
      onProgress(Math.min(100, (event.loaded / event.total) * 100));
    };

    request.onload = () => {
      const payload =
        request.response ??
        (request.responseText ? (JSON.parse(request.responseText) as T | { detail?: string }) : null);

      if (request.status >= 200 && request.status < 300 && payload) {
        onProgress(100);
        resolve(payload as T);
        return;
      }

      const detail = payload && "detail" in payload ? payload.detail : null;
      reject(new Error(detail ?? "Request failed."));
    };

    request.onerror = () => {
      reject(new Error("Upload failed."));
    };

    request.onabort = () => {
      reject(new Error("Upload cancelled."));
    };

    request.send(formData);
  });
}
