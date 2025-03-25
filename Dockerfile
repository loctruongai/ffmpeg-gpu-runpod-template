FROM runpod/base:0.6.1-cuda12.2.0

# --- Optional: System dependencies ---
COPY builder/setup.sh /setup.sh
RUN /bin/bash /setup.sh && \
    rm /setup.sh

# Python dependencies
COPY builder/requirements.txt /requirements.txt
RUN python3.11 -m pip install --upgrade pip && \
    python3.11 -m pip install --upgrade -r /requirements.txt --no-cache-dir && \
    rm /requirements.txt

COPY bin/ffmpeg /ffmpeg
ADD bin/assets /assets

# Copy source code
WORKDIR /app
ADD src /app/src

# Entrypoint: Chạy FastAPI qua Uvicorn
CMD ["uvicorn", "src.handler:app", "--host", "0.0.0.0", "--port", "8000"]
