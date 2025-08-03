import os.path

import yt_dlp
import ffmpeg
from definitions import TEMP_DIR
import src.core.utils as core_utils


class YoutubeDownload:
    def download(self, url):
        if not self.validate_url(url):
            return '', 'Invalid youtube url'

        output_path = os.path.join(TEMP_DIR, f'{core_utils.get_random_id()}.webm')
        ydl_opts = {
            'outtmpl': output_path,
            'format': 'bestaudio/best',
            'noplaylist': True
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])

        if not os.path.exists(output_path):
            return '', 'Error with downloading youtube video'

        return output_path, ''

    def swap_video_audio(self, video_path, audio_path):
        input_video = ffmpeg.input(video_path)
        input_audio = ffmpeg.input(audio_path)
        output_path = os.path.join(TEMP_DIR, f'{core_utils.get_random_id()}.mp4')
        ffmpeg.output(
            input_video.video,
            input_audio.audio,
            output_path,
            vcodec='copy',
            acodec='aac',
            shortest=None  # This ensures output ends when the video ends
        ).run(overwrite_output=True)

        return output_path

    def validate_url(self, url):
        return url.startswith('https://www.youtube.com/watch?v=') or url.startswith('https://youtu.be/')
