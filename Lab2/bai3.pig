reviews = LOAD '/media/sf_DS200_BigData/Lab2/bai1_result/part-r-00000' USING PigStorage(';') as (id:int,word_bag:bag{t:(w:chararray)},category:chararray,aspect:chararray,sentiment:chararray);
aspect_sentiment = distinct (foreach reviews generate id,aspect,sentiment);
pos_aspect = filter aspect_sentiment by sentiment == 'positive';
grouped_pos_aspect = GROUP pos_aspect BY aspect;
pos_aspect_count = foreach grouped_pos_aspect generate
    group as aspect,
    COUNT(pos_aspect) as count;
pos_sorted = ORDER pos_aspect_count BY count DESC;
top_pos = Limit pos_sorted 1;


neg_aspect = filter aspect_sentiment by sentiment == 'negative';
grouped_neg_aspect = GROUP neg_aspect BY aspect;
neg_aspect_count = foreach grouped_neg_aspect generate
    group as aspect,
    COUNT(neg_aspect) as count;
neg_sorted = ORDER neg_aspect_count BY count DESC;
top_neg = limit neg_sorted 1;

labeled_pos = FOREACH top_pos GENERATE 'Positive Top' AS label, aspect, count;
labeled_neg = FOREACH top_neg GENERATE 'Negative Top' AS label, aspect, count;

result = UNION labeled_pos, labeled_neg;
--Đưa kết quả về 1 file
final_result = ORDER result BY label DESC parallel 1;
-- 3. Lưu vào chung một thư mục
STORE final_result INTO '/media/sf_DS200_BigData/Lab2/bai3_result' USING PigStorage(',');