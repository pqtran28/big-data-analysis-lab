clean_words = LOAD '/ds200-assignments/assignment-2/output/bai1_output/part-r-00000' 
              USING PigStorage(';') 
              AS (id:chararray, word:chararray);

raw_data = LOAD '/ds200-assignments/assignment-2/hotel-review.csv' 
           USING PigStorage(';') 
           AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, category:chararray);

word_env = JOIN clean_words BY id, raw_data BY id;
needed_data = FOREACH word_env GENERATE 
    clean_words::word AS word, 
    raw_data::category AS category;

grouped_data = GROUP needed_data BY (category, word);
word_counts = FOREACH grouped_data GENERATE 
    FLATTEN(group) AS (category, word), 
    COUNT(needed_data) AS occurrence;

pos_data = FILTER word_counts BY category == 'positive';
neg_data = FILTER word_counts BY category == 'negative';

-- Sắp xếp và lấy Top 5 cho Positive
sorted_pos = ORDER pos_data BY occurrence DESC;
top5_pos = LIMIT sorted_pos 5;

-- Sắp xếp và lấy Top 5 cho Negative
sorted_neg = ORDER neg_data BY occurrence DESC;
top5_neg = LIMIT sorted_neg 5;

STORE top5_pos INTO '/ds200-assignments/assignment-2/output/bai5_relate_positive' USING PigStorage('\t');
STORE top5_neg INTO '/ds200-assignments/assignment-2/output/bai5_relate_negative' USING PigStorage('\t');