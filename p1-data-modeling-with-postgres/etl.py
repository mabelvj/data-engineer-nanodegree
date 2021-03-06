import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import logging


def process_song_file(cur, filepath):
    """Process songs files in `filepath`

    The function will extract the song and artist information 
    and store tem into the songs and artists table respectively
    
    Arguments: 
    - cur: cursor variable
    - filepath: file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data =  df[['artist_id', "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values.tolist()[0]

    cur.execute(artist_table_insert, artist_data)
    
    # insert song record
    song_data = df[['song_id', "title", "artist_id", "year", "duration"]].values.tolist()[0]

    cur.execute(song_table_insert, song_data)



def process_log_file(cur, filepath):
    """Process logs files in `filepath`
    
    This function will read the jsons in `log_data` and save the songplays info, 
    users info and start_time info using the respective queries.

    First, it will be filtered only the log records where the `page=NextSong`
    (only the info related to songplays).

    Then, the information about the `start_time` will be generated:
    `('timestamp', "hour", "day", "week", "month", "year", "weekday")`,
    so it can be stored in the **time** table.

    Finally, data from users will be inserted into the **users** table.

    For the song plays, it will be required to iterate through each record and
    fetch the **artist_id** and **song_id** so the **songplays** fact table can be linked
    to the **artists** and **songs** dimension tables.
    
    Arguments: 
    - cur: cursor variable
    - filepath: file path to the song file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[lambda x: x['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month,  t.dt.year,  t.dt.weekday)
    column_labels = ('timestamp', "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.concat(time_data, keys=column_labels, axis=1)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]] 

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            continue 


        # insert songplay record
        songplay_data = (pd.to_datetime(row['ts'], unit='ms'), row.userId,
                         row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """Process files in `filepath`

    The function will iterate and extract data from the files in filepath
    and pass them to `funct` to be processed
    
    Arguments: 
    - cur: cursor variable
    - conn: connector variable
    - filepath: file path to the song file
    - func: the function that will process the data
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")

    with conn:
        with conn.cursor() as cur:
            process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    with conn:
        with conn.cursor() as cur:
            process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()