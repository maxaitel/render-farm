FROM nvidia/cuda:12.9.1-base-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libatlas3-base \
    libavdevice60 \
    libavfilter9 \
    libblas3 \
    libblosc1 \
    libboost-iostreams1.83.0 \
    libbs2b0 \
    libepoxy0 \
    libfftw3-double3 \
    libfftw3-single3 \
    libflite1 \
    libgfortran5 \
    libimath-3-1-29t64 \
    libjack0 \
    libjemalloc2 \
    libdbus-1-3 \
    libegl1 \
    libfontconfig1 \
    libglib2.0-0 \
    libgl1 \
    liblapack3 \
    libopenal1 \
    libopenexr-3-1-30 \
    libosdcpu3.5.0t64 \
    libosdgpu3.5.0t64 \
    libplacebo338 \
    libpocketsphinx3 \
    libpotrace0 \
    libpugixml1v5 \
    libpystring0 \
    librubberband2 \
    libsdl2-2.0-0 \
    libserd-0-0 \
    libshaderc1 \
    libsm6 \
    libsndio7.0 \
    libsord-0-0 \
    libsphinxbase3t64 \
    libsratom-0-0 \
    libvidstab1.1 \
    libwayland-client0 \
    libwebpdemux2 \
    libyaml-cpp0.8 \
    libzimg2 \
    libzix-0-0 \
    libxfixes3 \
    libxi6 \
    libxkbcommon0 \
    libxrender1 \
    libxxf86vm1 \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY scripts/blender-wrapper.sh /usr/local/bin/blender-wrapper
RUN chmod +x /usr/local/bin/blender-wrapper

RUN useradd --create-home --shell /bin/bash render \
    && mkdir -p /data/jobs /data/tmp \
    && chown -R render:render /app /data

USER render

EXPOSE 8000

CMD ["python3", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
