import requests
from datetime import date
import json
from airflow.decorators import task
from airflow.models import Variable

API_KEY = Variable.get("API_KEY")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE")
maxResult = 50


@task
def get_mainPlaylist_ID():
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"

    try:
        respose = requests.get(url)
        respose.raise_for_status()

        data = respose.json()
        playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

        return playlist_id
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_IDs(playlist_id):
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={maxResult}&playlistId={playlist_id}&key={API_KEY}"
    videoIds_list = list()
    pageToken = None

    try:

        while True:

            url = base_url

            if pageToken:
                url += f"&pageToken={pageToken}"

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            for item in data.get("items", []):
                videoIds_list.append(item["contentDetails"]["videoId"])

            pageToken = data.get("nextPageToken")

            if not pageToken:
                break
        return videoIds_list
    except requests.exceptions.RequestException as e:
        raise e


@task
def get_video_data(video_IDs):
    extracted_data = []
    video_data = dict()

    def batch_list(video_id_list, batch_size):
        for id in range(0, len(video_id_list), batch_size):
            yield video_id_list[id : id + batch_size]

    try:
        for batch in batch_list(video_IDs, maxResult):
            video_IDs_str = ",".join(batch)

            url = f"https://youtube.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails,topicDetails&id={video_IDs_str}&key={API_KEY}"

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            for item in data.get("items", []):
                video_id = item["id"]
                snippet = item["snippet"]
                contentDetails = item["contentDetails"]
                statistics = item["statistics"]
                topicDetails = item.get("topicDetails", {})
                tags = snippet.get("tags", [])

                video_data = {
                    "video_id": video_id,
                    "title": snippet.get("title", None),
                    "publishedAt": snippet.get("publishedAt", None),
                    "duration": contentDetails.get("duration", None),
                    "viewCount": statistics.get("viewCount", None),
                    "likeCount": statistics.get("likeCount", None),
                    "commentCount": statistics.get("commentCount", None),
                    "definition": contentDetails.get("definition", None),
                    "caption": contentDetails.get("caption", None),
                    "tags": tags,
                    "topicCategories": topicDetails.get("topicCategories", []),
                }

                extracted_data.append(video_data)
        return extracted_data
    except requests.exceptions.RequestException as e:
        raise e


@task
def save_to_json(extracted_data):
    file_path = f"./data/YT_data_{date.today()}.json"
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(extracted_data, file, indent=4, ensure_ascii=False)


if __name__ == "__main__":
    playlistID = get_mainPlaylist_ID()
    video_ids = get_video_IDs(playlistID)
    video_data = get_video_data(video_ids)
    save_to_json(video_data)
