--To count word frequency. Match the reviews with distinct ID
--1.Load data from bai1_result 
reviews = LOAD '/media/sf_DS200_BigData/Lab2/bai1_result/part-r-00000' USING PigStorage(';') as (id:int,word_bag:bag{t:(w:chararray)},category:chararray,aspect:chararray,sentiment:chararray);
flattened_words = FOREACH reviews GENERATE 
    id, 
    FLATTEN(word_bag) AS word;
    
distinct_words = DISTINCT flattened_words;
grouped_words = GROUP distinct_words by word;
word_count = FOREACH grouped_words GENERATE 
    group AS word, 
    COUNT(distinct_words) AS frequency;
result_words = FILTER word_count BY frequency > 500;
STORE result_words INTO '/media/sf_DS200_BigData/Lab2/bai2_result/word_count' USING PigStorage(';');

distinct_categories = DISTINCT (FOREACH reviews GENERATE id,category);
grouped_categories = GROUP distinct_categories BY category;
category_count = FOREACH grouped_categories GENERATE 
    group as category,
    COUNT(distinct_categories) as count;
STORE category_count INTO '/media/sf_DS200_BigData/Lab2/bai2_result/category_count' USING PigStorage(';');

distinct_aspects = DISTINCT (FOREACH reviews GENERATE id,aspect);
grouped_aspects = GROUP distinct_aspects BY aspect;
aspect_count = FOREACH grouped_aspects GENERATE 
    group as aspect,
    COUNT(distinct_aspects) as count;
STORE aspect_count INTO '/media/sf_DS200_BigData/Lab2/bai2_result/aspect_count' USING PigStorage(';');
