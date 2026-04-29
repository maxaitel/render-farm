export type JobPhase =
  | "queued"
  | "running"
  | "packaging"
  | "completed"
  | "failed"
  | "stalled"
  | "cancelled";
export type RenderMode = "still" | "animation";
export type UserRole = "user" | "admin";
export type UserStatus = "pending" | "approved" | "suspended";
export type FramePhase =
  | "pending"
  | "rendering"
  | "complete"
  | "failed"
  | "retrying"
  | "skipped";

export interface RenderSettings {
  render_engine: string | null;
  output_format: string | null;
  samples: number | null;
  use_denoising: boolean | null;
  resolution_x: number | null;
  resolution_y: number | null;
  resolution_percentage: number | null;
  frame_step: number | null;
  film_transparent: boolean | null;
  view_transform: string | null;
  look: string | null;
  exposure: number | null;
  gamma: number | null;
  image_quality: number | null;
  compression: number | null;
  use_motion_blur: boolean | null;
  use_simplify: boolean | null;
  simplify_subdivision: number | null;
  simplify_child_particles: number | null;
  simplify_volumes: number | null;
  seed: number | null;
}

export interface FrameRenderRecord {
  camera_name: string | null;
  camera_index: number;
  frame: number;
  status: FramePhase;
  output_path: string | null;
  attempts: number;
  error: string | null;
  started_at: string | null;
  finished_at: string | null;
  seconds: number | null;
}

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
  render_settings: RenderSettings;
  requested_device: string;
  resolved_device: string | null;
  worker_assigned: string | null;
  queue_position: number | null;
  priority: number;
  camera_name: string | null;
  camera_names: string[];
  current_camera_name: string | null;
  current_camera_index: number | null;
  total_cameras: number;
  frame: number | null;
  start_frame: number | null;
  end_frame: number | null;
  current_frame: number | null;
  total_frames: number;
  total_outputs_expected: number;
  completed_frames: number;
  failed_frames: number;
  current_output: string | null;
  elapsed_seconds: number | null;
  estimated_seconds_remaining: number | null;
  average_seconds_per_frame: number | null;
  last_progress_at: string | null;
  current_sample: number | null;
  total_samples: number | null;
  outputs: string[];
  frame_statuses: FrameRenderRecord[];
  logs_tail: string[];
  log_path: string | null;
  command: string[];
  environment_info: Record<string, unknown>;
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
  render_settings: RenderSettings;
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
  frame_start: number;
  frame_end: number;
  frame_step: number;
  cameras: BlendCameraOption[];
  resolution: {
    x: number;
    y: number;
    percentage: number;
  };
  render_engine: string;
  samples: number | null;
  output_format: string;
  image_settings: {
    file_format: string;
    quality: number | null;
    compression: number | null;
  };
  render_settings: RenderSettings;
  blend_render_settings?: RenderSettings;
  render_settings_source: "blend" | "saved";
  estimated_output_files: number;
  scene_collections: string[];
  asset_warnings: string[];
  file_size_bytes: number;
  source_filename: string;
  processing_status: string;
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
