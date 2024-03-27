from googleapiclient.discovery import build
import streamlit as st
import pandas as pd
import pymysql,time
import re
from datetime import datetime
import plotly.express as px

# add your own api key in "api_key"
#password='your own password'

def connect_api():
    api_key = 'xxx'
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_key)
    return youtube

youtube= connect_api()

def convert_date(timestamp):
    format_date = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
    return format_date

def duration_to_seconds(duration_str):
    match = re.match(r'PT(\d+)M(\d+)S', duration_str)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        total_seconds = minutes * 60 + seconds
        return total_seconds
    else:
        sec=00
        return sec

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
                         Published=convert_date(item['snippet']['publishedAt']),
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
                    Published=convert_date(item['snippet']['publishedAt']),
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorites=item['statistics']['favoriteCount'],
                    Duration=duration_to_seconds(item['contentDetails']['duration']),
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
                        Comment_published=convert_date(item['snippet']['topLevelComment']['snippet']['publishedAt'])
                        )
                Comment_data.append(data)
                
    except:
        pass
    return Comment_data


#-- Table Creation in DB
def table_creation(): 
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='your password')

    cursor_obj = connection.cursor()

    cursor_obj.execute("CREATE DATABASE IF NOT EXISTS youtube_db")
    cursor_obj.execute("USE youtube_db")
    
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
        duration INT,
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
            password='your password',
            database='youtube_db')
            #autocommit=True)

        with connection.cursor() as cursor:
            
            # Insert data from DataFrame into the MySQL table
            for index, row in channel_df.iterrows():
                sql = '''
                    INSERT INTO channel (channel_id, channel_name, channel_type, channel_views, channel_description, channel_status, playlist_id) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (row['Channel_id'], row['Channel_name'], row['Channel_type'], int(row['Channel_views']), row['Channel_description'], row['Channel_status'], row['Playlist_id']))
        
        # Commit the transaction
        connection.commit()
        print("Channel Data inserted successfully!")

    except Exception as e:
        print(f"Error inserting channel data: {str(e)}")
    
    finally:
        # Close the connection
        connection.close()
    
def insert_playlist_table(playlist_df):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='your password',
        database='youtube_db',
        cursorclass=pymysql.cursors.DictCursor)


    try:
        with connection.cursor() as cursor:
            # Inserting each row from playlist_df into MySQL table
            for index, row in playlist_df.iterrows():                 
                sql = '''
                    INSERT INTO playlist (playlist_id, channel_id, playlist_name, channel_name, published, video_count) 
                    VALUES (%s, %s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (row['Playlist_id'], row['Channel_id'], row['Playlist_name'], row['Channel_name'], row['Published'], int(row['Video_count'])))
        
        connection.commit()
        print("Playlist Data inserted successfully!")
    
    except Exception as e:
        print(f"Error inserting palylist data: {str(e)}")
    
    finally:
        # Close the connection
        connection.close()
        
def insert_comment_table(comment_df):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='your password',
        database='youtube_db',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            for index, row in comment_df.iterrows():                
                sql = '''
                    INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published) 
                    VALUES (%s, %s, %s, %s, %s)
                '''
                cursor.execute(sql, (row['Comment_id'], row['Video_id'], row['Comment_text'], row['Comment_author'], row['Comment_published']))
        
        # Commit the transaction
        connection.commit()
        print("Comment Data inserted successfully!!!")
    
    except Exception as e:
        print(f"Error inserting comment data: {str(e)}")
    
    finally:
        connection.close()
        
def insert_video_table(video_df):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='your password',
        database='youtube_db',
        cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            for index, row in video_df.iterrows():                
                sql = '''
                    INSERT INTO video (video_id, video_name, channel_id, channel_name, video_description, published, views, likes, comments, favorites, duration, thumbnail, caption_status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                '''                                                                                                                         
                cursor.execute(sql, (row['Video_id'], row['Video_name'], row['Channel_id'], row['Channel_name'],row['Video_description'], row['Published'], int(row['Views']), int(row['Likes']), int(row['Comments']), int(row['Favorites']), row['Duration'], row['Thumbnail'], row['Caption_status']))
        
        connection.commit()
        print("Videos Data inserted successfully.....")
    
    except Exception as e:
        print(f"Error inserting video data: {str(e)}")
    
    finally:
        connection.close()

def fetch_data(query):
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='your password',
        database='youtube_db'
    )
    cursor = connection.cursor()

    try:
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return data, columns

    except Exception as e:
        st.error(f"Error fetching query: {e}")

    finally:
        cursor.close()
        connection.close()
        
#========== UI - streamlit run Youtube_project.py
with st.sidebar:
    image_path="C:/Users/vikra/Learningpythons/GUVI_cours/Capstone_Proj/logo.jpg"
    st.image(image_path, width=90)

with st.sidebar:
    st.title(":black[Project Overview]")
    if st.button("ABOUT"):
         st.write("This project provides users to retrieve youtube channel informations like Playlists, Videos, Comments, Channellist, etc using the YouTube API.")
    if st.button("EXTRACT"):
        st.write("COLLECT DATA: Youtube API and the Channel ID is used to collect channel details, videos, playlists. Then all these data are converted to dataframe using Pandas.")
    if st.button("MIGRATION"):
        st.write("DATA to MYSQL: All the channel informations are stored in MYSSQL and also used for efficient querying and analysis.")
    if st.button("VISUALIZATION"):
        st.write("QUERY SEARCH: Search and Fetch data from the MYSQL database to get business insights and visualization to analyze and make predictions for future.")
button_styles = """
    <style>
        .stButton>button {
              background-color: #04AA6D; /* Green */
              border: none;
              color: white;
              padding: 10px 30px;
              text-align: center;
              text-decoration: none;
              display: inline-block;
              font-size: 16px;
            
        }
        
    </style>
"""
st.markdown(button_styles, unsafe_allow_html=True)

tab1, tab2, tab3,tab4 = st.tabs(["HOME", "COLLECT / EXTRACT", "MIGRATION", "DATA VISUALIZATION"])

with tab1:
   st.write("## :blue[YOUTUBE DATA HARVESTING AND WAREHOUSING ROJECT]")
   st.write("YouTube Data Harvesting and Warehousing is a project that intends to provide users with the ability to access and extract data from numerous YouTube channels. MYSQL and Streamlit are used to develop a user-friendly application that allows users to store and retrieve YouTube channel informations like video,channel,playlist data,etc.")
   st.write("#### :blue[STEPS TO PROCEED:]")
   st.write("1. To collect large amounts of data from YouTube channel and extracted it.") 
   st.write("2. Extracted channel informations are stored in Data warehouse such as MYSQL.") 
   st.write("3. Fetch Data from MYSQL to analyse data to get business insights with visualization to make predictions.")

with tab2:
   st.write("## :blue[COLLECT AND EXTRACT CHANNEL DATA]")
   st.markdown("# ")
   channel_id = st.text_input("#### Enter the YouTube Channel ID")
   st.markdown("## ")
   extract=st.button("##    :black[EXTRACT DATA]")
   if extract:
       with st.spinner('Please Wait...'):
            time.sleep(20)
            #st.markdown("#    ")
            channels_info= get_channel_info(channel_id)
            playlist_info=get_playlist_details(channel_id)
            video_ids= get_videos_ids(channel_id)
            videos_info= get_video_info(video_ids)
            comments_info= get_comment_info(video_ids)
            st.success('##### :black[Data Extracted Successfully]')
            st.write("#### :green[CHANNEL INFORMATION]")
            st.dataframe(channels_info)
            st.write("#### :green[PLAYLIST INFORMATION]")
            st.dataframe(playlist_info)
            st.write("#### :green[VIDEOS INFORMATION]")
            st.dataframe(videos_info)
            st.write("#### :green[COMMENTS INFORMATION]")
            st.dataframe(comments_info)
   
with tab3:
   st.write("## :blue[MIGRATE DATA TO MYSQL DATABASE]")
   st.markdown("# ")
   ch_id = st.text_input("#### Enter Channel ID") #UCnz-ZXXER4jOvuED5trXfEA
   st.markdown("## ")
   migrate=st.button("## :black[MIGRATE TO MYSQL]")
   if migrate:
       channels_info= get_channel_info(ch_id)
       playlist_info=get_playlist_details(ch_id)
       video_ids= get_videos_ids(ch_id)
       videos_info= get_video_info(video_ids)
       comments_info= get_comment_info(video_ids)
       table_creation()
       print("All Tables Creation Done!!!!")
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
       insert_comment_table(comment_df)
       insert_video_table(video_df)
       print("Insert Done!!!!!")    
       st.success('##### :black[Data Stored in MYSQL Successfully]')

with tab4:
    st.write("## :blue[DATA VISUALIZATION]")
    q1 = '1. What are the names of all the videos and their corresponding channels?'
    q2 = '2. Which channels have the most number of videos, and how many videos do they have?'
    q3 = '3. What are the top 10 most viewed videos and their respective channels?'
    q4 = '4. How many comments were made on each video, and what are their corresponding video names?'
    q5 = '5. Which videos have the highest number of likes, and what are their corresponding channel names?'
    q6 = '6. What is the total number of likes for each video, and what are their corresponding video names?'
    q7 = '7. What is the total number of views for each channel, and what are their corresponding channel names?'
    q8 = '8. What are the names of all the channels that have published videos in the year 2022?'
    q9 = '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?'
    q10 = '10. Which videos have the highest number of comments, and what are their corresponding channel names?'
    question = st.selectbox('Select any question to get insights/visualization',(q1,q2,q3,q4,q5,q6,q7,q8,q9,q10),index=None,placeholder="Choose query...",)
    st.markdown("#")
    insights = st.button("## :black[BUSINESS INSIGHTS]")
    if insights:
        if question == q1:
            query = "SELECT v.video_name, c.channel_name FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id;"
            data, columns = fetch_data(query)
            df = pd.DataFrame(data, columns=columns)
            st.dataframe(df)
            
        elif question == q2:
            query="SELECT c.channel_name, COUNT(v.video_id) AS video_count FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name ORDER BY video_count DESC LIMIT 1;"
            data, columns = fetch_data(query)
            df = pd.DataFrame(data, columns=columns)
            st.dataframe(df)
        elif question == q3:
            query="SELECT c.channel_name,v.video_name, v.views FROM video v JOIN channel c ON v.channel_id = c.channel_id ORDER BY v.views DESC LIMIT 10;"
            data, columns = fetch_data(query)
            df = pd.DataFrame(data, columns=columns)
            st.dataframe(df)
            st.write("### :green[Top 10 most viewed videos :]")
            fig = px.bar(df,
                 x=columns[2],
                 y=columns[1],
                 orientation='h',
                 color=columns[0]
                )
            st.plotly_chart(fig,use_container_width=True)
        elif question == q4: 
            query="SELECT v.video_name, COUNT(cm.comment_id) AS comment_count FROM video v LEFT JOIN comment cm ON v.video_id = cm.video_id GROUP BY v.video_name;"
            data, columns = fetch_data(query)
            df = pd.DataFrame(data, columns=columns)
            st.dataframe(df)
            st.write("### :green[Total Comment Vs video :]")
            
            fig = px.bar(df, x=columns[1], y=columns[0], orientation='v')
            st.plotly_chart(fig,use_container_width=True)
            

        elif question == q5:
            query="SELECT v.video_name, c.channel_name, v.likes as Likes_count FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id ORDER BY v.likes DESC LIMIT 10;"
            data, columns = fetch_data(query)
            df = pd.DataFrame(data, columns=columns)
            st.dataframe(df)
            st.write("### :green[Top 10 most liked videos :]")
            fig = px.bar(df, x=columns[2], y=columns[0], orientation='h', color=columns[1])
            st.plotly_chart(fig,use_container_width=True)
         
        elif question == q6:
             query="SELECT v.video_name, SUM(v.likes) AS Total_likes FROM video v GROUP BY v.video_name;"
             data, columns = fetch_data(query)
             df = pd.DataFrame(data, columns=columns)
             st.dataframe(df)
             st.write("### :green[Total Likes Vs video :]")
             fig = px.bar(df, x=columns[1], y=columns[0], orientation='h')
             st.plotly_chart(fig,use_container_width=True)
        elif question == q7:
             query="SELECT c.channel_name, SUM(v.views) AS total_views FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name;"
             data, columns = fetch_data(query)
             df = pd.DataFrame(data, columns=columns)
             st.dataframe(df) 
             st.write("### :green[Channels vs Views :]")
             fig = px.bar(df, x=columns[1], y=columns[0], orientation='h')
             st.plotly_chart(fig,use_container_width=True)
        elif question == q8:
             query="SELECT DISTINCT c.channel_name FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id WHERE YEAR(v.published) = 2022;"
             data, columns = fetch_data(query)
             df = pd.DataFrame(data, columns=columns)
             st.dataframe(df)
        elif question == q9: 
             query="SELECT c.channel_name, AVG(v.duration) AS avg_duration FROM channel c INNER JOIN video v ON c.channel_id = v.channel_id GROUP BY c.channel_name;"
             data, columns = fetch_data(query)
             df = pd.DataFrame(data, columns=columns)
             st.dataframe(df)
        elif question == q10:
             query="SELECT v.video_name, c.channel_name, COUNT(cm.comment_id) AS comment_count FROM video v INNER JOIN channel c ON v.channel_id = c.channel_id LEFT JOIN comment cm ON v.video_id = cm.video_id GROUP BY v.video_name, c.channel_name ORDER BY comment_count DESC LIMIT 1;"
             data, columns = fetch_data(query)
             df = pd.DataFrame(data, columns=columns)
             st.dataframe(df)
 









