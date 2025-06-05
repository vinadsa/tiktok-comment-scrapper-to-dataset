import pandas as pd
import uuid
from urllib.parse import urlparse, parse_qs

def extract_video_id_from_url(video_url):
    """
    Extracts simpler video ID's
    Example: from 'https://t.tiktok.com/i18n/share/video/7170139292767882522/?_d=0...'
             to '7170139292767882522'
    """
    if not video_url:
        return None
    try:
        parsed_url = urlparse(video_url)
        path_segments = [segment for segment in parsed_url.path.split('/') if segment]
        if path_segments:
            potential_id = path_segments[-1]
            if potential_id.isdigit():
                return potential_id
        
        if "video" in path_segments:
            video_index = path_segments.index("video")
            if video_index + 1 < len(path_segments) and path_segments[video_index + 1].isdigit():
                return path_segments[video_index + 1]

        query_params = parse_qs(parsed_url.query)
        if 'share_item_id' in query_params and query_params['share_item_id'][0].isdigit():
            return query_params['share_item_id'][0]
        if 'id' in query_params and query_params['id'][0].isdigit(): 
            return query_params['id'][0]

    except Exception:
        pass

    for segment in reversed(path_segments):
        if segment.isdigit():
            return segment
            
    return "_".join(path_segments[-2:]) if len(path_segments) >=2 else path_segments[-1] if path_segments else "unknown_video_id"


def convert_tiktok_json_to_dataframe(tiktok_video_data_list):
    """
    Converts a list of TikTok video JSON objects
    into a pandas DataFrame, flattening comments and replies.

    Args:
        tiktok_video_data_list (list): A list of dictionaries, where each dictionary
                                     represents a scraped video and its comments.
                                     Example:
                                     [
                                         {
                                             "caption": "video caption 1",
                                             "date_now": "2023-12-10T22:06:04",
                                             "video_url": "url1",
                                             "comments": [...]
                                         },
                                         {
                                             "caption": "video caption 2",
                                             "date_now": "2023-12-11T10:00:00",
                                             "video_url": "url2",
                                             "comments": [...]
                                         }
                                     ]
    Returns:
        pandas.DataFrame: A DataFrame with essential fields, where each row
                          is either a top-level comment or a reply.
    """
    all_entries_data = []

    for video_data in tiktok_video_data_list:
        video_url = video_data.get("video_url")
        video_id = extract_video_id_from_url(video_url)
        video_caption = video_data.get("caption")
        scrape_timestamp = video_data.get("date_now")

        comments = video_data.get("comments", [])
        for comment_dict in comments:
            parent_entry_id = str(uuid.uuid4())
            
            comment_entry = {
                "video_id": video_id,
                "video_url": video_url,
                "video_caption": video_caption,
                "scrape_timestamp": scrape_timestamp,
                "entry_id": parent_entry_id,
                "parent_comment_id": None,
                "is_reply": False,
                "text_content": comment_dict.get("comment"),
                "author_username": comment_dict.get("username"),
                "author_nickname": comment_dict.get("nickname"),
                "creation_timestamp": comment_dict.get("create_time"),
                "comment_total_replies": comment_dict.get("total_reply", 0)
                # "author_avatar_url": comment_dict.get("avatar") # optional if you wanna include avatar url
            }
            all_entries_data.append(comment_entry)

            replies = comment_dict.get("replies", [])
            for reply_dict in replies:
                reply_entry_id = str(uuid.uuid4()) 
                reply_entry = {
                    "video_id": video_id,
                    "video_url": video_url,
                    "video_caption": video_caption,
                    "scrape_timestamp": scrape_timestamp,
                    "entry_id": reply_entry_id,
                    "parent_comment_id": parent_entry_id,
                    "is_reply": True,
                    "text_content": reply_dict.get("comment"),
                    "author_username": reply_dict.get("username"),
                    "author_nickname": reply_dict.get("nickname"),
                    "creation_timestamp": reply_dict.get("create_time"),
                    "comment_total_replies": None # replies don't have it
                    # "author_avatar_url": reply_dict.get("avatar") # optional
                }
                all_entries_data.append(reply_entry)

    df = pd.DataFrame(all_entries_data)
    
    # Optional: Convert timestamp strings to datetime objects if needed for sorting/analysis
    if not df.empty:
        for col in ['scrape_timestamp', 'creation_timestamp']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

    return df

if __name__ == "__main__":
    # Example usage (for testing purposes)
    sample_tiktok_data = [
        {
            "caption": "makk aku jadi animee🤩#faceplay #faceplayapp #anime #harem #xysryo ",
            "date_now": "2023-12-10T22:06:04",
            "video_url": "https://t.tiktok.com/i18n/share/video/7170139292767882522/?_d=0&comment_author_id=6838487455625479169&mid=7157599449395496962&preview_pb=0®ion=ID&share_comment_id=7310977412674093829&share_item_id=7170139292767882522&sharer_language=en&source=h5_t&u_code=0",
            "comments": [
                {
                    "username": "user760722966",
                    "nickname": "rehan",
                    "comment": "testing 😁😁",
                    "create_time": "2023-12-10T21:46:36",
                    "avatar": "url_to_avatar1",
                    "total_reply": 0,
                    "replies": []
                },
                {
                    "username": "user123456",
                    "nickname": "another_user",
                    "comment": "bagus",
                    "create_time": "2023-12-10T18:55:47",
                    "avatar": "url_to_avatar2",
                    "total_reply": 1,
                    "replies": [
                        {
                            "username": "ryo.syntax",
                            "nickname": "Bukan Rio",
                            "comment": "good game",
                            "create_time": "2023-12-10T18:56:19",
                            "avatar": "url_to_avatar3"
                        }
                    ]
                }
            ]
        }
    ]

    print("Converting sample data...")
    df_converted = convert_tiktok_json_to_dataframe(sample_tiktok_data)

    if not df_converted.empty:
        print("\nDataFrame Head:")
        print(df_converted.head())
        print(f"\nTotal entries: {len(df_converted)}")
        
        # Optional:
        # csv_filename_test = "test_tiktok_comments_converted.csv"
        # df_converted.to_csv(csv_filename_test, index=False, encoding='utf-8-sig')
        # print(f"\nSample data saved to {csv_filename_test}")
    else:
        print("No data converted or DataFrame is empty.")