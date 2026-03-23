import type { BlendInspection, RenderJob, SystemStatus } from "@/lib/types";

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
  frame?: number,
): Promise<BlendInspection> {
  const formData = new FormData();
  formData.set("blend_file", file, file.name);
  if (frame) {
    formData.set("frame", String(frame));
  }
  const response = await fetch("backend/api/blend-inspect", {
    method: "POST",
    body: formData,
  });
  return parseResponse<BlendInspection>(response);
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
