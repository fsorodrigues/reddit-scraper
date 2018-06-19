#! /usr/bin/env python3

# importing modules
import time
import datetime
import requests
import json
import pandas as pd
import praw
import re

# set datetime string pattern
# limit_date below will need to match this format
# CHANGE VALUE HERE
date_pattern = '%Y-%m-%d %H:%M:%S' # == YYYY-MM-DD HH:MM:SS

# date limit for search
# expects a string following the format set above in date_pattern
# CHANGE VALUE HERE
limit_date = '2018-06-18 00:00:00'

# set interval used to split time and run queries.
# expects an int/float
# CHANGE VALUES HERE ON YOUR CONVENIENCE usually, 60 minutes bins are a very conservative number
minutes = 60 # this will cluster data in hour bins

# subreddit to be queried
# case insensitive
# CHANGE VALUE HERE
subreddit = 'SUBREDDIT'

# reddit client login
# CHANGE VALUES HERE
client_id='CLIENT_ID' # 14 CHAR
client_secret='CLIENT_SECRET' # 27 CHAR
user_agent='USER_AGENT' # app user agent name
user_name='USERNAME' # your login handle 
password='PASSWORD' # your login password

# transform timestamp strings into Epoch Unix notation
# visit https://www.epochconverter.com for further documentatiion
# expects a string following the format set above in time_pattern
def get_epoch(date_time):
    return int(time.mktime(time.strptime(date_time,date_pattern)))

# calculates interval in seconds. 
def min_interval(minutes): 
    return minutes * 60

# transforms Epoch Unix into datetime objects
def get_date(submission):
    time = submission
    return datetime.datetime.fromtimestamp(time)

# gets string-formatted current time. Time zone: UTC/GMT
now = time.strftime(date_pattern, time.gmtime())
# creates list of Epoch Unix notation times
time_splits = list(range(get_epoch(limit_date),get_epoch(now),min_interval(minutes)))
# calculates the number of iterations
length = len(time_splits)
# set subset of useful columns from submissions data
sub_subset = ['author','created_utc','full_link','id','num_comments','permalink','retrieved_on','subreddit','subreddit_id','title','timestamp']

# URL setup 
# visit https://github.com/pushshift/api for further documentation
# base for query
base_url = 'https://api.pushshift.io/reddit/search/submission/?'
# max number of search results per iteration [1-500]
size=500

# starts empty pandas DataFrame
sub_df = pd.DataFrame()

# loops through iterations
for i in range(0,length-1,1):
    # time queries
    after = time_splits[i]
    before = time_splits[i+1]
    
    # make get request for data
    r = requests.get(base_url+'subreddit='+ subreddit +'&after='+ str(after) +'&before='+ str(before) +'&size='+ str(size))
    # r = requests.get(base_url+'subreddit='+ subreddit +'&after='+ str(after) +'&before='+ str(before) +'&size='+ str(size))
    
    # load json returned into a pandas DataFrame
    json_data = json.loads(r.text)['data']
    json_df = pd.DataFrame(json_data)
    sub_df = pd.concat([sub_df,json_df])
    
    print('cur_iter:',str(i+1)+' / '+str(length),'scraped_len:',str(len(json_data)),'total_len:',str(len(sub_df)))

print('submission scraping done')
# set proper indexes for rows
sub_df = sub_df.reset_index(drop=True)

# get datetime
sub_df['timestamp'] = sub_df['created_utc'].apply(get_date)

# export csv
sub_df[sub_subset].to_csv('./submission_raw_data.csv',index=False)
print('submission file saved')

# start reddit instance with dev app permissions
reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     user_agent=user_agent,
                     username=user_name,
                     password=password)

# create dict sctructure to store scraped data
comments_dict = {'submission_id':[],'body':[],'comment_id':[],'parent_id':[],'created_utc':[],'author':[],'score':[]}
    
# loop over submission ids scraped above and get comments
list_ids = sub_df['id']
length_ids = len(list_ids)
x = 1

for submission in list_ids:
    # request submission
    get_submission = reddit.submission(id=submission)

    # handle replace more
    # see https://praw.readthedocs.io/en/latest/tutorials/comments.html#the-replace-more-method for further documentation
    get_submission.comments.replace_more(limit=None)
    
    # get flattened list of comments (all levels/tiers)
    list_comments = get_submission.comments.list()
    length_comments = len(list_comments)
    
    print('cur_iter: '+str(x)+' / '+str(length_ids),'scraped_len:',str(length_comments),'total_len:',str(len(comments_dict['submission_id'])))
    x += 1
    
    # extract data from response and pass it to dict format
    for comment in list_comments:
        comments_dict['submission_id'].append(submission)
        comments_dict['body'].append(comment.body)
        comments_dict['comment_id'].append(comment.id)
        comments_dict['parent_id'].append(comment.parent_id)
        comments_dict['created_utc'].append(comment.created_utc)
        comments_dict['author'].append(comment.author)
        comments_dict['score'].append(comment.score)

# load json returned into a pandas DataFrame
comm_df = pd.DataFrame(comments_dict)

print('comment scraping done')
# set proper indexes for rows
comm_df = comm_df.reset_index(drop=True)

# get datetime
comm_df['timestamp'] = comm_df['created_utc'].apply(get_date)

# remove pattern
comm_df['parent_id_edit'] = comm_df['parent_id'].str.replace(r't\d_','')

# export csv
comm_df.to_csv('./comment_raw_data.csv',index=False)
print('comment file saved')