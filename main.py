import re
import os
import click
import json
from datetime import datetime

from loguru import logger

from tiktokcomment import TiktokComment
from tiktokcomment.typing import Comments

from converter.json_converter import convert_tiktok_json_to_dataframe, extract_video_id_from_url
__title__ = 'TikTok Comment Scrapper & Converter'
__version__ = '2.1.0'

@click.command(
    help=__title__
)
@click.version_option(
    version=__version__,
    prog_name=__title__
)
@click.option(
    "--url",
    help='TikTok video URL (e.g., https://www.tiktok.com/@user/video/7418294751977327878) or just the video ID.',
    required=True
)
@click.option(
    "--output",
    default='data/',
    help='Directory for output data (JSON and CSV)'
)
def main(
    url: str,
    output: str
):
    aweme_id_match = re.search(r"(\d{18,20})", url)
    if aweme_id_match:
        aweme_id = aweme_id_match.group(1)
        video_url_for_converter = url
    elif re.match(r"^\d+$", url):
        aweme_id = url
        video_url_for_converter = f"https://www.tiktok.com/node/share/video/{aweme_id}"
    else:
        logger.error("Invalid TikTok URL or Video ID provided. Please provide a valid URL or a numeric video ID.")
        click.echo("Error: Invalid TikTok URL or Video ID. Example ID: 7418294751977327878 or full URL.")
        return

    logger.info(f'Start scrap comments for video ID: {aweme_id}')

    try:
        scraper_instance = TiktokComment()
        
        scraped_data_object: Comments = scraper_instance(aweme_id=aweme_id)

        if not scraped_data_object or not hasattr(scraped_data_object, 'dict'):
            logger.error(f"Scraping failed or returned no data for video ID: {aweme_id}")
            return

        scraped_dict_for_json = scraped_data_object.dict 

        data_for_converter_input = [{
            "video_url": video_url_for_converter,
            "aweme_id": aweme_id,
            "date_now": datetime.now().isoformat(),
            "caption": scraped_dict_for_json.get("caption", f"Video {aweme_id}"),
            "comments": scraped_dict_for_json.get("comments", [])
        }]
        
        dir_path = os.path.dirname(output)
        base_filename = aweme_id

        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path)
            logger.info(f"Created output directory: {dir_path}")

        json_output_path = os.path.join(dir_path, f"{base_filename}.json")
        if not dir_path and output.endswith('/'):
             json_output_path = f"{output}{base_filename}.json"
        elif not dir_path and not output.endswith('/'):
             json_output_path = f"{output}/{base_filename}.json"


        with open(json_output_path, 'w', encoding='utf-8') as f:
            json.dump(scraped_dict_for_json, f, ensure_ascii=False, indent=2)
        logger.info(f'Saved raw comments for {aweme_id} to {json_output_path}')


        logger.info(f"Converting comments for {aweme_id} to CSV format...")
        
        comments_dataframe = convert_tiktok_json_to_dataframe(data_for_converter_input)

        if not comments_dataframe.empty:
            csv_output_path = os.path.join(dir_path, f"{base_filename}_converted.csv")
            if not dir_path and output.endswith('/'):
                 csv_output_path = f"{output}{base_filename}_converted.csv"
            elif not dir_path and not output.endswith('/'):
                csv_output_path = f"{output}/{base_filename}_converted.csv"


            try:
                comments_dataframe.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
                logger.info(f'Successfully converted and saved data for {aweme_id} to: {csv_output_path}')
            except Exception as e:
                logger.error(f"Error saving DataFrame to CSV for {aweme_id}: {e}")
        else:
            logger.warning(f"Converted DataFrame is empty for {aweme_id}. CSV not saved.")

    except ValueError as ve:
        logger.error(f"Value Error: {ve}")
        click.echo(f"Error: {ve}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during scraping or conversion for {aweme_id}: {e}")
        click.echo(f"An unexpected error occurred: {e}")


if __name__ == '__main__':
    main()