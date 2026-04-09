reviews = LOAD '/media/sf_DS200_BigData/Lab2/bai1_result/part-r-00000' USING PigStorage(';') as (id:int,word_bag:bag{t:(w:chararray)},category:chararray,aspect:chararray,sentiment:chararray);
flattened_words = FOREACH reviews GENERATE 
    category, 
    sentiment, 
    FLATTEN(word_bag) AS word;


pos_data = FILTER flattened_words BY sentiment == 'positive';

pos_grouped = GROUP pos_data BY (category, word);
pos_counts = FOREACH pos_grouped GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(pos_data) AS freq;

pos_category_group = GROUP pos_counts BY category;

top_5_pos = FOREACH pos_category_group {
    sorted = ORDER pos_counts BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};


neg_data = FILTER flattened_words BY sentiment == 'negative';

neg_grouped = GROUP neg_data BY (category, word);
neg_counts = FOREACH neg_grouped GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(neg_data) AS freq;

neg_category_group = GROUP neg_counts BY category;

top_5_neg = FOREACH neg_category_group {
    sorted = ORDER neg_counts BY freq DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

final_pos = FOREACH top_5_pos GENERATE 'Positive' AS label, category, word, freq;
final_neg = FOREACH top_5_neg GENERATE 'Negative' AS label, category, word, freq;

final_all = UNION final_pos, final_neg;

STORE final_all INTO '/media/sf_DS200_BigData/Lab2/bai4_result' USING PigStorage(',');