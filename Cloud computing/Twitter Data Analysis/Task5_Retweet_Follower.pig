-- Load data to alias data
data = load 's3://assignment2-data/link_status_search_with_ordering_real.csv' USING PigStorage(',') as (link:chararray, id:int, create_at:chararray, create_at_long:int, inreplyto_screen_name:chararray, inreplyto_user_id:int, source:chararray, bad_user_id:int, user_screen_name:chararray, order_of_users:int, user_id:int);

follower_data = load 's3://assignment2-data/distinct_users_from_search_table_real_map.csv' USING PigStorage(',') as(user_id:int, user_screen_name:chararray, indegree:int, outdegree:int, bad_user_id:int);
-- filter tweets that are not retweets
retweets = FILTER data BY inreplyto_user_id != -1;

-- Group retweets by user id of user it is made.
group_by_id_name_1 = GROUP retweets BY (user_id, user_screen_name);

-- count of retweets made by each user.
retweet_counts_1 =  FOREACH group_by_id_name_1 generate group.user_id as user_id_2, COUNT(retweets) AS retweet_count_1;

count_with_followers_1 = JOIN retweet_counts_1 by user_id_2, follower_data by user_id;

final_result_2 = FOREACH count_with_followers_1 GENERATE user_id_2, retweet_count_1, indegree, outdegree;

-- Storing the result into AWS s3.
STORE final_result_2 INTO 's3://assignment2-data/Part5_2_result';