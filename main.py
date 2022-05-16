from pytube import YouTube
import pathlib
from pprint import pprint
from googleapiclient.discovery import build
import requests
from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from typing import Any
from bson.objectid import ObjectId
import os, sys
import logging


MONGO_CLIENT = os.getenv("CLIENT_ADDR")
cursor = pymongo.MongoClient(MONGO_CLIENT)
db = cursor["youtube"]
channel = db["channels"]
videos = db["videos"]

BASE_DIR = pathlib.Path().home() / ".youtube"
google_api_key = os.getenv("API_KEY")


class YoutubeDownload:
    def __init__(self, api_key: str, link: str) -> None:
        self.__client: AsyncIOMotorClient = AsyncIOMotorClient('localhost')
        self.__api_key: str = api_key
        self.__BASE_DIR: pathlib.Path = pathlib.Path().home() / ".youtube"
        self.__yt: YouTube = YouTube(link)
        self.__vid_thumb_path: pathlib.Path = self.__BASE_DIR / "thumbnails" / self.__yt.author
        self.__logo_path: pathlib.Path = self.__BASE_DIR / 'logos'
        self.__channelId: str = self.__yt.channel_id
        self.__vid_path: pathlib.Path = self.__BASE_DIR / 'videos' / self.__yt.author
        self.__thumbnail_file_path = self.__BASE_DIR / "thumbnails" / self.__yt.author / f"{self.__yt.title}.jpg"
        logger = logging.getLogger('Youtube Downloader')
        logger.setLevel(logging.DEBUG)
        filename = pathlib.Path(self.__BASE_DIR / 'logs')
        filename.mkdir(parents=True, exist_ok=True)
        filename.touch(exist_ok=True)
        fh = logging.FileHandler(f"{self.__BASE_DIR / 'logs' / 'Youtube.logs'}", mode='a')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler(stream=sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] %(levelname)8s --- %(message)s ' +
                                  '(%(filename)s:%(lineno)s)',datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(fh)
        self.__logger = logger

    def __channelInfo(self) -> dict:
        data = {}
        youtube = build('youtube', 'v3', developerKey=self.__api_key)
        ch_request = youtube.channels().list(
            part='snippet,contentDetails,statistics',
            id=self.__channelId)
        ch_response: Any = ch_request.execute()
        thumbnail_url: str = ch_response["items"][0]["snippet"]["thumbnails"]['medium']['url']
        logo = requests.get(url=thumbnail_url)
        channel_name: str = ch_response["items"][0]["snippet"]["title"]
        logo_path: pathlib.Path = self.__logo_path / channel_name
        try:
            file: pathlib.Path = logo_path / 'logo.png'
            with open(file, 'wb') as f:
                f.write(logo.content)
        except FileNotFoundError:
            os.makedirs(logo_path)
            file: pathlib.Path = logo_path / "logo.png"
            with open(file, 'wb') as f:
                f.write(logo.content)
        logo_path = logo_path / 'logo.png'
        data["channel_name"] = channel_name
        data["description"] = ch_response["items"][0]["snippet"]["description"]
        data["joined"] = ch_response["items"][0]["snippet"]["publishedAt"]
        data["logo"] = str(logo_path)
        data["viewCount"] = int(ch_response["items"][0]["statistics"]['viewCount'])
        data["subscriberCount"] = int(ch_response["items"][0]["statistics"]['subscriberCount'])
        data["videoCount"] = int(ch_response["items"][0]["statistics"]['videoCount'])
        return data

    @property
    def __downloadVid(self):
        itag: int = self.__yt.streams.get_highest_resolution().itag
        ys: YouTube.streams = self.__yt.streams.get_by_itag(itag)
        self.__logger.info("downloading video")
        ys.download(output_path=str(self.__vid_path), filename=f"{self.__yt.title}.mp4")
        msg = f"downloaded! at {self.__vid_path}"
        resp = requests.get(url=self.__yt.thumbnail_url)
        self.__thumbnail_file_path = self.__vid_thumb_path / f"{self.__yt.title}.jpg"
        try:
            with open(self.__thumbnail_file_path, 'wb') as f:
                f.write(resp.content)
            self.__logger.debug('created thumbnail')
        except FileNotFoundError:
            os.makedirs(self.__vid_thumb_path)
            self.__logger.warning(f"{self.__vid_thumb_path} folder not found")
            self.__logger.debug(f"creating {self.__vid_thumb_path}")
            with open(self.__thumbnail_file_path, 'wb') as f:
                f.write(resp.content)
            self.__logger.debug(f"created thumbnail at {self.__thumbnail_file_path}")
        finally:
            return msg

    def addVideo(self) -> dict:
        resp = channel.find_one({
            "channel_name": self.__yt.author
        })
        if resp is None:
            self.__logger.warning(f"channel: {self.__yt.author} is not in db")
            data: dict = self.__channelInfo()
            _id: ObjectId = channel.insert_one(data).inserted_id
            self.__logger.debug(f"channel created successfully")
            channel_id: ObjectId = _id
        else:
            channel_id: ObjectId = resp["_id"]

        vid_data = {
            "title": self.__yt.title,
            "description": self.__yt.description,
            "duration": float(f"{(self.__yt.length / 60):.2f}"),
            "channel": channel_id,
            "published": self.__yt.publish_date,
            "thumbnail": str(self.__thumbnail_file_path)
        }
        query = videos.find_one({
            "title": self.__yt.title,
            "channel": channel_id
        })
        if query is not None:
            self.__logger.debug("video is available in db")
            files = os.listdir(self.__vid_path)
            files = [i.split('.')[0] for i in files]
            filename = self.__yt.title
            if filename in files:
                self.__logger.warning("video present in that folder")
            else:
                self.__logger.error("video is not found in folder")
                _ = self.__downloadVid
                self.__logger.info(f"Downloading video at {self.__vid_path}")

        else:
            self.__logger.error("video is not found at database")
            try:
                files = os.listdir(self.__vid_path)
                files = [i.split('.')[0] for i in files]
                filename = self.__yt.title
                if filename in files:
                    self.__logger.info(f"video found at {self.__vid_path}")
                    videos.insert_one(vid_data)
                    self.__logger.debug("inserted video information to database")
                else:
                    self.__logger.info(self.__downloadVid)
                    videos.insert_one(vid_data)
            except FileNotFoundError:
                self.__logger.info(self.__downloadVid)
                videos.insert_one(vid_data)
        return vid_data


vid = YoutubeDownload(api_key=google_api_key, link="https://youtu.be/gzrQvzYEvYc").addVideo()
