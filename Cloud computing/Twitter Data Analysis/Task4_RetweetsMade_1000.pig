-- Load .csv to data alias
data = load 's3://assignment2-data/link_status_search_with_ordering_real.csv' USING PigStorage(',') AS (link:chararray, id:int, create_at:chararray, create_at_long:int, inreplyto_screen_name:chararray, inreplyto_user_id:int, source:chararray, bad_user_id:int, user_screen_name:chararray, order_of_users:int, user_id:int);
-- Filter out the tweets that are not retweets
retweets = FILTER data BY inreplyto_user_id != -1;
-- Grouping retweets by user id and screen name who made the retweet.
group_by_id_name = GROUP retweets BY (user_id,user_screen_name);
-- Generate the count of retweets made by the user.
retweet_counts =  FOREACH group_by_id_name GENERATE group.user_id, group.user_screen_name, COUNT(retweets) AS retweet_count;
-- Ordering them in the descending order of number of retweets made.
ordered_counts = ORDER retweet_counts BY retweet_count desc;
-- limiting the result to top 1000 users.
final_result = LIMIT ordered_counts 1000;
-- Storing the result to AWS s3.
STORE final_result INTO 's3://assignment2-data/Part4_result';

