export type JobPhase = "queued" | "running" | "completed" | "failed";
export type RenderMode = "still" | "animation";
export type UserRole = "user" | "admin";
export type UserStatus = "pending" | "approved" | "suspended";

export interface RenderJob {
  id: string;
  user_id: number;
  file_id: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  phase: JobPhase;
  progress: number;
  status_message: string;
  source_filename: string;
  source_path: string;
  output_directory: string;
  archive_path: string | null;
  render_mode: RenderMode;
  output_format: string;
  requested_device: string;
  resolved_device: string | null;
  camera_name: string | null;
  camera_names: string[];
  current_camera_name: string | null;
  frame: number | null;
  start_frame: number | null;
  end_frame: number | null;
  current_frame: number | null;
  current_frame_started_at: string | null;
  current_frame_elapsed_seconds: number | null;
  total_frames: number;
  last_frame_duration_seconds: number | null;
  average_frame_duration_seconds: number | null;
  current_sample: number | null;
  total_samples: number | null;
  outputs: string[];
  logs_tail: string[];
  error: string | null;
}

export interface UserFile {
  id: string;
  user_id: number;
  created_at: string;
  updated_at: string;
  source_filename: string;
  source_path: string;
  source_root: string;
  original_size_bytes: number;
  latest_job: RenderJob | null;
  jobs: RenderJob[];
}

export interface UserAccount {
  id: number;
  username: string;
  role: UserRole;
  status: UserStatus;
  created_at: string;
  approved_at: string | null;
  approved_by_user_id: number | null;
  last_login_at: string | null;
  render_file_count: number;
  run_count: number;
}

export interface AuthSession {
  user: UserAccount;
  admin_panel_path: string | null;
  lan_admin_access: boolean;
}

export interface SystemStatus {
  blender: string;
  gpu: string;
  device_policy: {
    default: string;
    order: string[];
  };
  cycles_devices: {
    available_types: string[];
    cuda: string[];
    optix: string[];
    hip: string[];
    cpu: string[];
  };
  job_count: number;
  active_jobs: number;
}

export interface BlendCameraOption {
  name: string;
}

export interface BlendInspection {
  default_camera: string | null;
  frame: number;
  cameras: BlendCameraOption[];
}

export interface AdminOverview {
  pending_users: number;
  approved_users: number;
  suspended_users: number;
  total_files: number;
  total_runs: number;
  active_runs: number;
}

export interface ActivityRecord {
  id: number;
  created_at: string;
  event_type: string;
  description: string;
  actor_user_id: number | null;
  actor_username: string | null;
  subject_user_id: number | null;
  subject_username: string | null;
  file_id: string | null;
  job_id: string | null;
  metadata: Record<string, unknown>;
}
