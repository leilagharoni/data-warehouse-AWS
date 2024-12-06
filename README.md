**Project description:**

A startup called Sparkify has grown its user base and song database and wants to move its data process onto the cloud. Its data, including user activity logs and song metadata in JSON format, resides in AWS S3.
As a data engineer, I build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.


**Original Data Sources:**

**Song data:** s3://udacity-dend/song_data
The Song data is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

**Log data:** s3://udacity-dend/log_data

**Log data json path:** s3://udacity-dend/log_json_path.json



**Database Schema Design:**

User Story: A user plays a song whose artist is artist_name at time start_time using an agent.
From the above story, we can extract the necessary information/dimensions:

1) Who: users dimension
3) What: songs and artists dimension
4) When: time dimension
5) How many: songplays fact

In the next step, I have designed the Star Schema for the Sparikfy for Song play data, where in we have the centralized fact table surrounded by the 4 dimension tables.


**Fact Table:**

songplays: records in event data associated with song plays i.e. records with page NextSong
column list: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


**Dimension Tables**

1) users: users in the app
    
column list: user_id, first_name, last_name, gender, level

2) songs: songs in music database

column list: song_id, title, artist_id, year, duration

3) artists: artists in music database

column list: artist_id, name, location, latitude, longitude

4) time: timestamps of records in song plays broken down into specific units

column list: start_time, hour, day, week, month, year, weekday


**ETL Process:**
1. Setup IAM user (with programmatic) Access Key and Secret Key
2. Create a Redshift cluster
3. Fill the HOST and ARN in dwh.cfg
4. Run create_tables.py
5. Run etl.py to load data from S3 into staging tables and then transfer it into target tables (fact and dimension tables).
