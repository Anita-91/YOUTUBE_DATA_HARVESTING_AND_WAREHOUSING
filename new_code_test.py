
# 1. Install streamlit, 2. install google-api-python-client 3. MySQL Server - Workbench
#C:/Users/vikra/Learningpythons/GUVI_cours/Capstone_Proj/Youtube_project.py
#Tool -> YOUTUBE CHANNEL ID Finder Tool to find channel id of that channel

import streamlit as st
import pandas as pd
import pymysql,time
from datetime import datetime
from googleapiclient.discovery import build
#---


#channel_id ="UCnz-ZXXER4jOvuED5trXfEA" 
def connect_api():
    api_key = 'AIzaSyANx3dKusc-ECxnl1bBCKny3R6SesQMKjM'
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_key)
    return youtube

youtube= connect_api()

#-----Retreive  CHANNELS details
def get_channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for item in response['items']:
        channel_type = "Branded Channel" if item['snippet']['customUrl'] else "Standard Channel"
        channel_status = "Public" if not item['statistics']['hiddenSubscriberCount'] else "Private"
        data = {
                'Channel_id':channel_id,
                'Channel_name':item['snippet']['title'],
                'Channel_type': channel_type,
                'Channel_views' :item['statistics']['viewCount'],
                'Channel_description':item['snippet']['description'],
                'Channel_status': channel_status,
                'Playlist_id':item["contentDetails"]["relatedPlaylists"]["uploads"]
            }
    return data
#------- Retreive PLAYLIST details

def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()
                for item in response['items']:
                        data = dict(
                         Playlist_id=item['id'],
                         Channel_id=item['snippet']['channelId'],
                         Playlist_name=item['snippet']['title'],
                         Channel_name=item['snippet']['channelTitle'],
                         Published=item['snippet']['publishedAt'],
                         Video_count=item['contentDetails']['itemCount'])
                        All_data.append(data)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data

#------- Retreive VIDEO_IDS details
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        print("playlist response\n",response1)
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    print("len videoid:::",len(video_ids))
    
    return video_ids

#------ Retreive all VIDEOS details
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        
        for item in response["items"]:
            data=dict(
                    Video_id=item['id'],
                    Channel_name=item['snippet']['channelTitle'],
                    Channel_id=item['snippet']['channelId'],
                    Video_name=item['snippet']['title'],
                    Video_description=item['snippet'].get('description'),
                    Published=item['snippet']['publishedAt'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorites=item['statistics']['favoriteCount'],
                    Duration=item['contentDetails']['duration'],
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Caption_status=item['contentDetails']['caption'],
                    Tags=item['snippet'].get('tags'),
                    Definition=item['contentDetails']['definition']
                    )
            video_data.append(data) 
    #print("videos response \n",response)
    # print("================")
    return video_data


#------- Retreive COMMENT details
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(
                        Comment_id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
                
    except:
        pass
    # print("get_comment_info:::",Comment_data)
    # print("================")
    return Comment_data


#-- Table Creation in DB
def table_creation(): 
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Niku2020!')

    cursor_obj = connection.cursor()

    cursor_obj.execute("CREATE DATABASE IF NOT EXISTS YT_db")
    cursor_obj.execute("USE YT_db")
    
    create_channel_table = '''
        CREATE TABLE IF NOT EXISTS channel (
                channel_id VARCHAR(255),
                channel_name VARCHAR(255),
                Channel_type VARCHAR(255),
                channel_views INT,
                channel_description TEXT,
                channel_status VARCHAR(255),
                playlist_id VARCHAR(255)
    );
'''
    cursor_obj.execute(create_channel_table)

    create_playlist_table = '''
        CREATE TABLE IF NOT EXISTS playlist (
            playlist_id VARCHAR(255),
            channel_id VARCHAR(255),
            playlist_name VARCHAR(255),
            channel_name VARCHAR(255),
            published DATETIME,
            video_count INT
    );
'''
    cursor_obj.execute(create_playlist_table)

    create_video_table = '''
        CREATE TABLE IF NOT EXISTS video (
        video_id VARCHAR(255) PRIMARY KEY,
        video_name VARCHAR(255),
        channel_id VARCHAR(255),
        channel_name VARCHAR(255),
        video_description TEXT,
        published DATETIME,
        views BIGINT,
        likes BIGINT,
        comments BIGINT,
        favorites BIGINT,
        duration VARCHAR(255),
        thumbnail VARCHAR(255),
        caption_status VARCHAR(255)
    );
'''
    cursor_obj.execute(create_video_table)

    create_comment_table = '''
        CREATE TABLE IF NOT EXISTS comment (
        comment_id VARCHAR(255) PRIMARY KEY,
        video_id VARCHAR(255),
        comment_text TEXT,
        comment_author VARCHAR(255),
        comment_published DATETIME
    );
'''
    cursor_obj.execute(create_comment_table)
    connection.commit()
    connection.close()
    return True

####----- Insert to channel table
def insert_channel_table(channel_df):
    try:
        connection = pymysql.connect(
            host='localhost',
            user='root',
            password='Niku2020!',
            database='YT_db')

        with connection.cursor() as cursor:
            
            # Insert data from DataFrame into the MySQL table
            for index, row in channel_df.iterrows(): #7
                sql = '''
                    INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status, playlist_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (row['Channel_id'], row['Channel_name'], row['Channel_type'], int(row['Channel_views']), row['Channel_description'], row['Channel_status'], row['Playlist_id']))
      
        connection.commit()
        print("Channel Data inserted successfully!")

    except Exception as e:
        print(f"Error inserting channel data: {str(e)}")
    
    finally:
        connection.close()
    
def insert_playlist_table(playlist_df):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Niku2020!',
        database='YT_db',
        cursorclass=pymysql.cursors.DictCursor)

# Inserting data into MySQL table
    try:
        with connection.cursor() as cursor:
            # Inserting each row from playlist_df into MySQL table
            for index, row in playlist_df.iterrows(): #6
                published = datetime.strptime(row['Published'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                
                sql = '''
                    INSERT INTO playlist (playlist_id, channel_id, playlist_name, channel_name, published, video_count) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (row['Playlist_id'], row['Channel_id'], row['Playlist_name'], row['Channel_name'], published, int(row['Video_count'])))
        
        # Commit the transaction
        connection.commit()
        print("Playlist Data inserted successfully!")
    
    except Exception as e:
        print(f"Error inserting palylist data: {str(e)}")
    
    finally:
        # Close the connection
        connection.close()
        

#========== UI - streamlit run Youtube_project.py
with st.sidebar:
    st.write("Menu:")
    if st.button("HOME"):
         st.title(":blue[YOUTUBE DATA HARVESTING AND VISUALIZATION]")
         st.write("YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and extract data from numerous YouTube channels. MYSQL and Streamlit are used to develop a user-friendly application that allows users to store and retrieve YouTube channel informations like video,channel,playlist data,etc.")
    if st.button("EXTRACT"):
        st.write("You clicked on COLLECT and EXTRACT")
    if st.button("MIGRATE"):
        st.write("You clicked on MIGRATE")


#with st.expander("See explanation"):
    #st.write("The chart above shows some numbers I picked for you")


tab1, tab2, tab3= st.tabs(["HOME", "COLLECT and EXTRACT", "MIGRATION"])

with tab1:
   #st.image("C:/Users/vikra/Learningpythons/GUVI_cours/Capstone_Proj/logo.jpg", width=50)
   st.title(":blue[YOUTUBE DATA HARVESTING AND VISUALIZATION]")
   st.write("YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and extract data from numerous YouTube channels. MYSQL and Streamlit are used to develop a user-friendly application that allows users to store and retrieve YouTube channel informations like video,channel,playlist data,etc.")
with tab2:
   st.title(":blue[COLLECT AND EXTRACT CHANNEL DATA]")
   #st.markdown("# ")
   channel_id = st.text_input("#### Enter the YouTube Channel ID")
   st.markdown("## ")
   extract=st.button("##    :red[EXTRACT DATA]")
   if extract:
            st.markdown("#    ")
            channels_info= get_channel_info(channel_id)
            playlist_info=get_playlist_details(channel_id)
            video_ids= get_videos_ids(channel_id)
            videos_info= get_video_info(video_ids)
            comments_info= get_comment_info(video_ids)
            st.success('###### :black[Data Extracted Successfully]')
            st.write("#### :green[CHANNEL INFORMATION]")
            st.dataframe(channels_info)
           
            st.text("#### :green[PLAYLIST INFORMATION]")
            st.dataframe(playlist_info)
           
            st.text("#### :green[VIDEOS INFORMATION]")
            st.dataframe(videos_info)
           
            st.text("#### :green[COMMENTS INFORMATION]")
            st.dataframe(comments_info)
   
with tab3:
   st.title(":blue[MIGRATE DATA TO MYSQL DATABASE]")
   st.markdown("## ")
   ch_id = st.text_input("# Enter Channel ID") #UCnz-ZXXER4jOvuED5trXfEA
   migrate=st.button("## :red[MIGRATE TO MYSQL]")
   if migrate:
       channels_info= get_channel_info(ch_id)
       playlist_info=get_playlist_details(ch_id)
       video_ids= get_videos_ids(ch_id)
       videos_info= get_video_info(video_ids)
       comments_info= get_comment_info(video_ids)
       
       table_creation()
       print("All Tables Creation Done!!!!")
       
       #Converting Data to Dataframe
       channels_df=pd.DataFrame([channels_info])
       playlist_df=pd.DataFrame(playlist_info)
       comment_df=pd.DataFrame(comments_info)
       video_df=pd.DataFrame(videos_info)
       
       print("channels_df:::::::: \n",channels_df)
       print("playlist_df:::::::\n",playlist_df)
       print("comment_df:::::::\n",comment_df)
       print("video_df:::::::\n",video_df)
      
       insert_channel_table(channels_df)
       insert_playlist_table(playlist_df)
       
       st.success('##### :black[Data Stored in MYSQL Successfully]')






