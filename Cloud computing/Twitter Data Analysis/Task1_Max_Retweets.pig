-- Load data to alias data
data = load 's3://assignment2-data/link_status_search_with_ordering_real.csv' USING PigStorage(',') as (link:chararray, id:int, create_at:chararray, create_at_long:int, inreplyto_screen_name:chararray, inreplyto_user_id:int, source:chararray, bad_user_id:int, user_screen_name:chararray, order_of_users:int, user_id:int);

-- filter tweets that are not retweets
retweets = FILTER data BY inreplyto_user_id != -1;

-- Group retweets by user id and screen name of user it is made.
group_by_id_name = GROUP retweets BY (inreplyto_user_id,inreplyto_screen_name);

-- count of retweets got by each user.
retweet_counts =  FOREACH group_by_id_name generate group.inreplyto_user_id, group.inreplyto_screen_name, COUNT(retweets) AS retweet_count;

-- ordering the counts in descending order
ordered_counts = ORDER retweet_counts BY retweet_count desc;

-- Take the first result ie., user who got most retweets.
final_result = LIMIT ordered_counts 1;

-- Storing the result into AWS s3.
STORE final_result INTO 's3://assignment2-data/Part1_result';
