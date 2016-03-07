-- load the data into s3.
data = load 's3://assignment2-data/link_status_search_with_ordering_real.csv' USING PigStorage(',') as (link:chararray, id:int, create_at:chararray, create_at_long:int, inreplyto_screen_name:chararray, inreplyto_user_id:int, source:chararray, bad_user_id:int, user_screen_name:chararray, order_of_users:int, user_id:int);
-- group the data by link.
Topic_Trend = GROUP data BY link;

-- for each link we get the total number of tweets and retweets.
result1 = FOREACH Topic_Trend {
			temp = FILTER data BY inreplyto_user_id != -1;
			GENERATE group as link, COUNT(data) AS tweet_count, COUNT(temp) AS retweet_count;
};

-- Sorting in descending order of number of tweets so that we can get the most trending tweets at the top.
result2 = ORDER result1 BY tweet_count desc;

-- Most trended tweet.
final_result_1 = LIMIT result2 1;

-- Part 3, data for plotting.
-- we filter the data to get most trending link.
for_part_3 = JOIN final_result_1 by link, data by link;
-- grouping by the date the tweet is made including retweets.
group_by_date = GROUP for_part_3 BY create_at;
-- for each date we count the number of tweets made.
Total_Tweets_for_Top = FOREACH group_by_date GENERATE group, COUNT(for_part_3);
-- We store date and respective count as output.
STORE Total_Tweets_for_Top INTO 's3://assignment2-data/Part3_1_Result';

-- for each date we count the number of retweets made.
Total_Retweets_for_Top= FOREACH group_by_date {
							temp = FILTER for_part_3 BY inreplyto_user_id != -1;
							GENERATE group, COUNT(temp);
};
-- we store date and respective retweet count as output.
STORE Total_Retweets_for_Top INTO 's3://assignment2-data/Part3_2_Result';