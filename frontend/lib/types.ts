export type JobPhase = "queued" | "running" | "completed" | "failed";
export type RenderMode = "still" | "animation";

export interface RenderJob {
  id: string;
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
  frame: number | null;
  start_frame: number | null;
  end_frame: number | null;
  current_frame: number | null;
  total_frames: number;
  current_sample: number | null;
  total_samples: number | null;
  outputs: string[];
  logs_tail: string[];
  error: string | null;
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

export interface BlendCameraPreview {
  name: string;
  preview_data_url?: string | null;
}

export interface BlendInspection {
  default_camera: string | null;
  frame: number;
  cameras: BlendCameraPreview[];
}
