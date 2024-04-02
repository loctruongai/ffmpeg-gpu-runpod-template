""" Example handler file. """

import os
import boto3
import tempfile
import runpod
import subprocess

s3 = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url="https://storage.googleapis.com",
    aws_access_key_id=os.environ.get("HMAC_KEY"),
    aws_secret_access_key=os.environ.get("HMAC_SECRET"),
)


def encode_video(
    input_video: str,
    input_audio: str,
    subtitles: str,
    output_video: str,
    watermark_enabled: bool = True,
    subtitles_enabled: bool = True,
    show_deepsync_logo: bool = True,
    show_translation_notice: bool = True,
    matroska: bool = False,
):
    watermark_deepsync_dub = os.path.join("assets", "watermark-final.png")
    watermark_deepsync_dub_position = "main_w-overlay_w-5:main_h-overlay_h-5"
    watermark_deepsync_dub_scale = "iw*0.4:-1"

    watermark_warning = os.path.join("assets", "note.png")
    watermark_warning_position = "5:main_h-overlay_h-5"
    watermark_warning_scale = "iw*0.6:-1"

    cmd = ["/ffmpeg"]
    cmd += ["-hwaccel", "cuvid"]
    cmd += ["-hwaccel_output_format", "cuda"]
    cmd += ["-i", shlex.quote(input_video)]
    cmd += ["-i", shlex.quote(input_audio)]
    fc = []

    if watermark_enabled:
        if show_deepsync_logo:
            cmd += ["-i", shlex.quote(watermark_deepsync_dub)]
        if show_translation_notice:
            cmd += ["-i", shlex.quote(watermark_warning)]
        cmd += ["-filter_complex"]
        if show_deepsync_logo and show_translation_notice:
            fc += [f"[2:v]scale={watermark_deepsync_dub_scale}[wm1];"]
            fc += [f"[3:v]scale={watermark_warning_scale}[wm2];"]
            fc += [f"[0:v][wm1]overlay={watermark_deepsync_dub_position}[v];"]
            fc += [f"[v][wm2]overlay={watermark_warning_position}[v];"]
        else:
            current_scale = (
                watermark_deepsync_dub_scale
                if show_deepsync_logo
                else watermark_warning_scale
            )
            current_position = (
                watermark_deepsync_dub_position
                if show_deepsync_logo
                else watermark_warning_position
            )
            fc += [f"[2:v]scale={current_scale}[wm1];"]
            fc += [f"[0:v][wm1]overlay={current_position}[v];"]
        if not subtitles_enabled:
            fc[-1] = fc[-1].rstrip(";")

    if subtitles_enabled:
        if not watermark_enabled:
            cmd += ["-filter_complex"]
        fc += [
            f'[{"0:" if not watermark_enabled else ""}v]ass={subtitles}:fontsdir=/assets/[v]'
        ]

    if watermark_enabled or subtitles_enabled:
        fc = shlex.quote(" ".join(fc))
        cmd += [fc]
        cmd += ["-map", '"[v]"']
    else:
        cmd += ["-map", "0:v"]

    if matroska:
        cmd += ["-f", "matroska"]

    cmd += ["-map", "1:a"]
    cmd += ["-c:v", "h264_nvenc"]
    cmd += ["-c:a", "aac"]
    cmd += [shlex.quote(output_video)]

    cmd = " ".join(cmd)
    print("Complete command:")
    print(cmd)
    result = subprocess.run(cmd, shell=True)

    if result.returncode == 1 and matroska == False:
        subprocess.run(f"rm {shlex.quote(output_video)};", shell=True)
        encode_video(
            input_video,
            input_audio,
            subtitles,
            output_video,
            watermark_enabled,
            subtitles_enabled,
            show_deepsync_logo,
            show_translation_notice,
            matroska=True,
        )



def handler(job):
    """ Handler function that will be used to process jobs. """
    task = job['task']
    event = job['parameters']

    if task == "ENCODING":
        _id = event.get("id")
        language = event.get("language")
        watermark_enabled = event.get("watermark") != "false"
        subtitles_enabled = event.get("subtitles") != "false"
        show_deepsync_logo = event.get("deepsync_logo") != "false"
        show_translation_notice = event.get("translation_notice") != "false"
        name = event.get("name", "exported_video.mp4")
        input_video_name = event.get("input_video_name", "video.mp4")

        bucket = os.environ.get('BUCKET_NAME')
        video_key = f"{os.environ.get('BUCKET_PARENT_FOLDER')}/{_id}/{input_video_name}"
        audio_key = f"{os.environ.get('BUCKET_PARENT_FOLDER')}/{_id}/exported_with_music.wav"
        subtitles_key = f"{os.environ.get('BUCKET_PARENT_FOLDER')}/{_id}/subtitles_{language}.ass"

        exported_video_key = f"{os.environ.get('BUCKET_PARENT_FOLDER')}/{_id}/{name}"

        with tempfile.TemporaryDirectory() as tmpdirname:
            input_video = os.path.join(tmpdirname, "video.mp4")
            input_audio = os.path.join(tmpdirname, "exported_with_music.wav")
            subtitle_file = os.path.join(tmpdirname, f"subtitles_{language}.ass")
            output_video = os.path.join(tmpdirname, "exported_video.mp4")

            # Download video and audio files from source S3 bucket
            print(bucket, video_key, input_video)
            s3.download_file(Bucket=bucket, Key=video_key, Filename=input_video)
            s3.download_file(Bucket=bucket, Key=audio_key, Filename=input_audio)
            s3.download_file(Bucket=bucket, Key=subtitles_key, Filename=subtitle_file)

            # Encode audio, video and subtitles
            encode_video(
                input_video,
                input_audio,
                subtitle_file,
                output_video,
                watermark_enabled,
                subtitles_enabled,
                show_deepsync_logo,
                show_translation_notice,
            )

            if not os.path.exists(output_video):
                raise Exception("Video was unable to encode.")

            # Upload the resultant video to the destination S3 bucket
            s3.upload_file(Filename=output_video, Bucket=bucket, Key=exported_video_key)
            return {
                '_id': _id,
                'statusCode': 200,
                'body': 'Video re-encoding and upload completed!'
            }



runpod.serverless.start({"handler": handler})
