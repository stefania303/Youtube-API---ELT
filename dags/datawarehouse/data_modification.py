import logging
import psycopg2


logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cursor, connection, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"

            cursor.execute(
                f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title","Upload_Date", "Duration", "Video_Views", "Likes_Count", "Comments_Count", "Definition", "Caption", "Tags", "Topic_Categories")
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s,%(definition)s,%(caption)s,%(tags)s,%(topicCategories)s);""",
                row,
            )

        else:
            video_id = "Video_ID"
            try:

                cursor.execute(
                    f"""INSERT INTO {schema}.{table}("Video_ID", "Video_Title", "Upload_Date", "Duration","Video_Type", "Video_Views", "Likes_Count", "Comments_Count", "Is_hd","Has_captions","Tags","Tag_count", "Topic_Categories")
                            VALUES (%(Video_ID)s, %(Video_Title)s,%(Upload_Date)s,%(Duration)s,%(Video_Type)s,%(Video_Views)s,%(Likes_Count)s,%(Comments_Count)s,%(Is_hd)s,%(Caption)s,%(Tags)s,%(Tag_count)s,%(Topic_Categories)s);""",
                    row,
                )
            except psycopg2.Error as e:
                logger.error("pgcode=%s pgerror=%s", e.pgcode, e.pgerror)
                raise

        connection.commit()

        logger.info(f"Inserted row with video id : {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting row with video_id {row[video_id]}")


def update_rows(cursor, connection, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"
            video_title = "title"
            upload_date = "publishedAt"
            duration = "duration"
            video_views = "viewCount"
            likes_count = "likeCount"
            comments_count = "commentCount"
            definition = "definition"
            caption = "caption"
            tags = "tags"
            topic_categories = "topicCategories"
        else:
            video_id = "Video_ID"
            video_title = "Video_Title"
            upload_date = "Upload_Date"
            video_type = "Video_Type"
            video_views = "Video_Views"
            likes_count = "Likes_Count"
            comments_count = "Comments_Count"
            tags = "tags"

        cursor.execute(
            f""" UPDATE {schema}.{table}
                       SET  "Video_Title" = %({video_title})s,
                            "Video_Views" = %({video_views})s,
                            "Likes_Count" = %({likes_count})s, 
                            "Comments_Count" = %({comments_count})s
                        WHERE "Video_ID" =%({video_id})s AND "Upload_Date" = %({upload_date})s;  """,
            row,
        )

        connection.commit()

    except Exception as e:
        logger.error(f"Error updating row with video_id {row[video_id]}")


def delete_rows(cursor, connection, schema, ids_to_delete, row):
    try:
        ids_to_delete = f""" ({','.join(f"'{id}'" for id in ids_to_delete)})"""

        cursor.execute(
            f"""
                       DELETE FROM {schema}.{table}
                       WHERE "Video_ID" IN {ids_to_delete});"""
        )
        connection.commit()

        logger.info(f"Deleted rowa with video ida : {ids_to_delete}")

    except Exception as e:
        logger.error(f"Error deleting rows with video_ids {ids_to_delete}")
