""" Example handler file. """

import os
import boto3
import shlex
import runpod
import tempfile
import subprocess

s3 = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url="https://storage.googleapis.com",
    aws_access_key_id=os.environ.get("HMAC_KEY"),
    aws_secret_access_key=os.environ.get("HMAC_SECRET"),
)


def get_bucket_key(uri):
    uri = uri.replace("gs://", "").replace("s3://", "")
    bucket, key = uri.split("/", maxsplit=1)
    filename = os.path.basename(key)
    return bucket, key, filename


def encode_video(
    input_video: str,
    input_audio: str,
    subtitles: str,
    output_video: str,
    subtitles_enabled: bool = True,
    matroska: bool = False,
):
    cmd = ["/ffmpeg"]
    cmd += ["-hwaccel", "cuvid"]
    cmd += ["-hwaccel_output_format", "cuda"]
    cmd += ["-i", shlex.quote(input_video)]
    cmd += ["-i", shlex.quote(input_audio)]
    fc = []

    if subtitles_enabled:
        cmd += ["-filter_complex"]
        fc += [
            f'[0:v]ass={subtitles}:fontsdir=/assets/[v]'
        ]

    if subtitles_enabled:
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
            subtitles_enabled,
            matroska=True,
        )


def downsample_video(
    input_video: str,
    output_video: str,
    resolution=240
):
    ratio = f"{int(resolution*16/9)}:{resolution}"
    cmd = ["/ffmpeg"]
    cmd += ["-hwaccel", "cuvid"]
    cmd += ["-hwaccel_output_format", "cuda"]
    cmd += ["-i", shlex.quote(input_video)]
    cmd += ["-vcodec", "h264_nvenc"]
    cmd += ["-vf", f'scale="{ratio}"']
    cmd += ["-crf", "28"]
    cmd += [shlex.quote(output_video)]

    cmd = " ".join(cmd)
    print("Complete command:")
    print(cmd)
    result = subprocess.run(cmd, shell=True)


def handler(job):
    """ Handler function that will be used to process jobs. """
    task = job['task']
    event = job['parameters']

    if task == "ENCODING":
        _id = event.get("id")
        language = event.get("language")
        subtitles_enabled = event.get("subtitles", False)
        name = event.get("name", "exported_video.mp4")
        input_video_name = event.get("input_video_name", "video.mp4")
        bucket = event.get("bucket")
        bucket_parent_folder = event.get("bucket_parent_folder")

        assert bucket is not None
        assert bucket_parent_folder is not None

        video_key = f"{bucket_parent_folder}/{_id}/{input_video_name}"
        audio_key = f"{bucket_parent_folder}/{_id}/exported_with_music.wav"
        subtitles_key = f"{bucket_parent_folder}/{_id}/subtitles_{language}.ass"

        exported_video_key = f"{bucket_parent_folder}/{_id}/{name}"

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
                subtitles_enabled,
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
    elif task == "DOWNSAMPLING":
        original_video_uri = event.get("original_video_uri")
        output_video_uri = event.get("output_video_uri")
        resolution = int(str(event.get("resolution", "240")).strip("p"))

        with tempfile.TemporaryDirectory() as tmpdirname:
            original_video = os.path.join(tmpdirname, "video.mp4")
            output_video = os.path.join(tmpdirname, "output.mp4")

            print(original_video_uri, output_video_uri, resolution)
            bucket, key, _ = get_bucket_key(original_video_uri)
            s3.download_file(Bucket=bucket, Key=key, Filename=original_video)

            # Encode audio, video and subtitles
            downsample_video(
                input_video,
                output_video,
                resolution=resolution
            )
            if not os.path.exists(output_video):
                raise Exception("Video was unable to encode.")

            # Upload the resultant video to the destination S3 bucket
            bucket, exported_video_key, _ = get_bucket_key(output_video_uri)
            s3.upload_file(Filename=output_video, Bucket=bucket, Key=exported_video_key)
            return {
                '_id': _id,
                'statusCode': 200,
                'body': 'Video downsampling successful!'
            }
    



runpod.serverless.start({"handler": handler})
