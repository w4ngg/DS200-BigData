reviews = LOAD '/media/sf_DS200_BigData/Lab2/bai1_result/part-r-00000' USING PigStorage(';') as (id:int,word_bag:bag{t:(w:chararray)},category:chararray,aspect:chararray,sentiment:chararray);
flattened_words = FOREACH reviews GENERATE 
    category, 
    FLATTEN(word_bag) AS word;

category_grouped = group flattened_words by (category,word);
count_words = FOREACH category_grouped GENERATE flatten(group) as (category,word), COUNT(flattened_words) as freq;
grouped_by_category = GROUP count_words BY category;
top_5_related = FOREACH grouped_by_category {
    sorted = ORDER count_words BY freq DESC;
    top_5 = LIMIT sorted 5;
    GENERATE FLATTEN(top_5);
};
final_result = ORDER top_5_related BY category,freq DESC;
STORE final_result INTO '/media/sf_DS200_BigData/Lab2/bai5_result' USING PigStorage(',');