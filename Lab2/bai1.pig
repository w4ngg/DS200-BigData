--1.Load review
raw_reviews = LOAD '/media/sf_DS200_BigData/Lab2/hotel-review.csv' USING PigStorage(';') as (id:int, review:chararray, type1:chararray, type2:chararray,sentiment:chararray);

--2. Load stop words
stop_words = LOAD '/media/sf_DS200_BigData/Lab2/stopwords.txt' USING PigStorage('\n') as (sw:chararray);

--3. Lowercase 
lower_reviews = foreach raw_reviews generate
                id,                
                LOWER(REPLACE(review, '[\\.,!\\?]', '')) AS review,
                LOWER(type1) as type1, 
                LOWER(type2) as type2, 
                sentiment;

--4. Tokenize
tokenized_reviews = foreach lower_reviews generate
                    id, 
                    FLATTEN(TOKENIZE(review)) as word,
                    type1,
                    type2,
                    sentiment;

--5. Remove stop words
joined_data = JOIN tokenized_reviews BY word LEFT OUTER, stop_words BY sw;
filter_reviews = FILTER joined_data BY stop_words::sw is NULL;
grouped_data = GROUP filter_reviews BY (id,type1,type2,sentiment);
final_result = foreach grouped_data generate
                group.id as id,
                filter_reviews.tokenized_reviews::word AS words,
                group.type1 as type1,
                group.type2 as type2,
                group.sentiment as sentiment;
                
STORE final_result INTO '/media/sf_DS200_BigData/Lab2/bai1_result' USING PigStorage(';');