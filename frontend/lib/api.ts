import type {
  ActivityRecord,
  AdminOverview,
  AuthSession,
  BlendInspection,
  RenderJob,
  SystemStatus,
  UserAccount,
  UserFile,
  UserStatus,
} from "@/lib/types";

export type UploadProgressStats = {
  progress: number;
  loaded: number;
  total: number;
  elapsedSeconds: number;
  bytesPerSecond: number;
  estimatedSecondsRemaining: number | null;
};

export type ProjectUploadEntry = {
  file: File;
  path: string;
};

function xhrJsonPayload<T>(request: XMLHttpRequest): T | { detail?: string } | null {
  if (request.response !== null) {
    return request.response as T | { detail?: string };
  }
  return null;
}

function responseDetail(payload: unknown): string | null {
  if (!payload || typeof payload !== "object" || !("detail" in payload)) {
    return null;
  }
  const { detail } = payload as { detail?: unknown };
  if (typeof detail === "string") {
    return detail;
  }
  if (Array.isArray(detail)) {
    const messages = detail
      .map((item) => {
        if (!item || typeof item !== "object" || !("msg" in item)) {
          return null;
        }
        const { msg } = item as { msg?: unknown };
        return typeof msg === "string" ? msg : null;
      })
      .filter(Boolean);
    return messages.length ? messages.join(" ") : null;
  }
  return null;
}

async function parseResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const payload = await response.json().catch(() => ({ detail: "Request failed." }));
    throw new Error(responseDetail(payload) ?? "Request failed.");
  }
  return response.json() as Promise<T>;
}

export async function fetchSession(): Promise<AuthSession> {
  const response = await fetch("/backend/api/auth/session", { cache: "no-store" });
  return parseResponse<AuthSession>(response);
}

export async function signUp(username: string, password: string): Promise<AuthSession> {
  const response = await fetch("/backend/api/auth/sign-up", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  return parseResponse<AuthSession>(response);
}

export async function signIn(username: string, password: string): Promise<AuthSession> {
  const response = await fetch("/backend/api/auth/sign-in", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  return parseResponse<AuthSession>(response);
}

export async function signOut(): Promise<void> {
  const response = await fetch("/backend/api/auth/sign-out", {
    method: "POST",
  });
  await parseResponse<{ ok: boolean }>(response);
}

export async function fetchSystemStatus(): Promise<SystemStatus> {
  const response = await fetch("/backend/api/system", { cache: "no-store" });
  return parseResponse<SystemStatus>(response);
}

export async function fetchFiles(): Promise<UserFile[]> {
  const response = await fetch("/backend/api/files", { cache: "no-store" });
  return parseResponse<UserFile[]>(response);
}

export async function uploadFileWithProgress(
  formData: FormData,
  onProgress: (progress: UploadProgressStats) => void,
  signal?: AbortSignal,
): Promise<UserFile> {
  return submitWithProgress<UserFile>("/backend/api/files", formData, onProgress, signal);
}

export async function inspectStoredFile(
  fileId: string,
  frame: number | undefined,
): Promise<BlendInspection> {
  const formData = new FormData();
  if (typeof frame === "number") {
    formData.set("frame", String(frame));
  }
  const response = await fetch(`/backend/api/files/${fileId}/inspect`, {
    method: "POST",
    body: formData,
  });
  return parseResponse<BlendInspection>(response);
}

export async function createRun(
  fileId: string,
  formData: FormData,
): Promise<RenderJob> {
  const response = await fetch(`/backend/api/files/${fileId}/runs`, {
    method: "POST",
    body: formData,
  });
  return parseResponse<RenderJob>(response);
}

export async function cancelJob(jobId: string): Promise<RenderJob> {
  const response = await fetch(`/backend/api/jobs/${jobId}/cancel`, {
    method: "POST",
  });
  return parseResponse<RenderJob>(response);
}

export async function retryJob(jobId: string): Promise<RenderJob> {
  const response = await fetch(`/backend/api/jobs/${jobId}/retry`, {
    method: "POST",
  });
  return parseResponse<RenderJob>(response);
}

export async function adminCancelJob(jobId: string): Promise<RenderJob> {
  const response = await fetch(`/backend/api/admin/runs/${jobId}/cancel`, {
    method: "POST",
  });
  return parseResponse<RenderJob>(response);
}

export async function adminRetryJob(jobId: string): Promise<RenderJob> {
  const response = await fetch(`/backend/api/admin/runs/${jobId}/retry`, {
    method: "POST",
  });
  return parseResponse<RenderJob>(response);
}

export async function fetchAdminOverview(): Promise<AdminOverview> {
  const response = await fetch("/backend/api/admin/overview", { cache: "no-store" });
  return parseResponse<AdminOverview>(response);
}

export async function fetchAdminUsers(): Promise<UserAccount[]> {
  const response = await fetch("/backend/api/admin/users", { cache: "no-store" });
  return parseResponse<UserAccount[]>(response);
}

export async function updateAdminUserStatus(
  userId: number,
  status: UserStatus,
): Promise<UserAccount> {
  const response = await fetch(`/backend/api/admin/users/${userId}/status`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ status }),
  });
  return parseResponse<UserAccount>(response);
}

export async function fetchAdminActivity(): Promise<ActivityRecord[]> {
  const response = await fetch("/backend/api/admin/activity", { cache: "no-store" });
  return parseResponse<ActivityRecord[]>(response);
}

export async function fetchAdminRuns(): Promise<RenderJob[]> {
  const response = await fetch("/backend/api/admin/runs", { cache: "no-store" });
  return parseResponse<RenderJob[]>(response);
}

export async function fetchAdminFiles(): Promise<UserFile[]> {
  const response = await fetch("/backend/api/admin/files", { cache: "no-store" });
  return parseResponse<UserFile[]>(response);
}

function submitWithProgress<T>(
  url: string,
  formData: FormData,
  onProgress: (progress: UploadProgressStats) => void,
  signal?: AbortSignal,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    if (signal?.aborted) {
      reject(new Error("Upload cancelled."));
      return;
    }

    const request = new XMLHttpRequest();
    const startedAt = performance.now();
    const abortRequest = () => {
      request.abort();
    };
    const cleanup = () => {
      signal?.removeEventListener("abort", abortRequest);
    };

    request.open("POST", url);
    request.responseType = "json";
    signal?.addEventListener("abort", abortRequest, { once: true });

    request.upload.onprogress = (event) => {
      if (!event.lengthComputable || event.total === 0) {
        return;
      }
      const elapsedSeconds = Math.max(0.001, (performance.now() - startedAt) / 1000);
      const bytesPerSecond = event.loaded / elapsedSeconds;
      const remainingBytes = Math.max(0, event.total - event.loaded);
      onProgress({
        progress: Math.min(100, (event.loaded / event.total) * 100),
        loaded: event.loaded,
        total: event.total,
        elapsedSeconds,
        bytesPerSecond,
        estimatedSecondsRemaining:
          bytesPerSecond > 0 ? remainingBytes / bytesPerSecond : null,
      });
    };

    request.onload = () => {
      cleanup();
      const payload = xhrJsonPayload<T>(request);

      if (request.status >= 200 && request.status < 300 && payload) {
        resolve(payload as T);
        return;
      }

      reject(new Error(responseDetail(payload) ?? "Request failed."));
    };

    request.onerror = () => {
      cleanup();
      reject(new Error("Upload failed."));
    };

    request.onabort = () => {
      cleanup();
      reject(new Error("Upload cancelled."));
    };

    request.send(formData);
  });
}
