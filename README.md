# tiktok-comment-scrapper-to-dataset
Scrapes Tiktok comments from a Tiktok video id. A working and improved version from RomySihananda's tiktok-comment-scrapper.

## Requirements

- **Python >= 3.11.4**
- **Requests >= 2.32.3**

## Installation

```sh
# Cloning Repository
git clone https://github.com/vinadsa/tiktok-comment-scrapper-to-dataset

# Change Directory
cd tiktok-comment-scrapper-to-dataset

# Install Requirements
pip install -r requirements.txt
```

## Example Usages

```sh
python main.py --url=7511092308184681771 --output=data
python main.py --url=7507084128119016709 --output=data
python main.py --url=7507084128119016709 --output=data --limit=50 # limit to 50 comments
python main.py --url=7511092308184681771 --limit=100 # limit to 100 comments, default output
```

### Flags

| Flag     | Alias |           Description           | Example         |       Default       |
| :------- | :---: | :-----------------------------: | :-------------- | :-----------------: |
| --url    |  -u   | Url or video id of tiktok video | --url=id or url | 7170139292767882522 |
| --output |  -o   |      json file output path      | --output=data   |        data         |
| --limit  |  -l   | Maximum number of comments to scrape | --limit=100 | No limit (scrape all) |


```json
{
  "caption": "makk aku jadi animee🤩#faceplay #faceplayapp #anime #harem #xysryo ",
  "date_now": "2023-12-10T22:06:04",
  "video_url": "https://t.tiktok.com/i18n/share/video/7170139292767882522/?_d=0&comment_author_id=6838487455625479169&mid=7157599449395496962&preview_pb=0&region=ID&share_comment_id=7310977412674093829&share_item_id=7170139292767882522&sharer_language=en&source=h5_t&u_code=0",
  "comments": [
    {
      "username": "user760722966",
      "nickname": "rehan",
      "comment": "testing 😁😁",
      "create_time": "2023-12-10T21:46:36",
      "avatar": "https://p16-sign-useast2a.tiktokcdn.com/tos-useast2a-avt-0068-giso/f64f2c7df8a16098d3b3c80e958ffc52~c5_100x100.jpg?x-expires=1702306800&x-signature=KhUeuGmPAVij9A8gbgh7wK6rn98%3D",
      "total_reply": 0,
      "replies": []
    },
    {
      "username": "user760722966",
      "nickname": "rehan",
      "comment": "bagus",
      "create_time": "2023-12-10T18:55:47",
      "avatar": "https://p16-sign-useast2a.tiktokcdn.com/tos-useast2a-avt-0068-giso/f64f2c7df8a16098d3b3c80e958ffc52~c5_100x100.jpg?x-expires=1702306800&x-signature=KhUeuGmPAVij9A8gbgh7wK6rn98%3D",
      "total_reply": 3,
      "replies": [
        {
          "username": "ryo.syntax",
          "nickname": "Bukan Rio",
          "comment": "good game",
          "create_time": "2023-12-10T18:56:19",
          "avatar": "https://p16-sign-useast2a.tiktokcdn.com/tos-useast2a-avt-0068-giso/be4a9d0479f29d00cb3d06905ff5a972~c5_100x100.jpg?x-expires=1702306800&x-signature=IvkeSvXmvkmE0hZG5dtgpqcFn3A%3D"
        }
        // more replies
      ]
    }
    // more comments
  ]
}
```

## License

This project is licensed under the [MIT License](LICENSE).
