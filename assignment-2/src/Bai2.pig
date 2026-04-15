clean_words = LOAD '/ds200-assignments/assignment-2/output/bai1_output/part-r-00000' USING PigStorage(';') AS (id:chararray, word:chararray, topic:chararray, aspect:chararray, category:chararray);

raw_data = LOAD '/ds200-assignments/assignment-2/hotel-review.csv' USING PigStorage(';') 
           AS (id:chararray, comment:chararray, topic:chararray, aspect:chararray, category:chararray);

-- task 1:
grouped_words = GROUP clean_words BY word;
word_counts = FOREACH grouped_words GENERATE group AS word, COUNT(clean_words) AS count;
high_freq_words = FILTER word_counts BY count > 500;

STORE high_freq_words INTO '/ds200-assignments/assignment-2/output/bai2_word_count' USING PigStorage('\t');

--task 2:
-- sentiment = category
grouped_category = GROUP raw_data BY category;
category_counts = FOREACH grouped_category GENERATE group AS category, COUNT(raw_data) AS num_comments;

STORE category_counts INTO '/ds200-assignments/assignment-2/output/bai2_category_count' USING PigStorage('\t');

-- task 3:
aspect_data = FOREACH raw_data GENERATE aspect;
filtered_aspects = FILTER aspect_data BY aspect IS NOT NULL AND aspect != '';

grouped_aspects = GROUP filtered_aspects BY aspect;
aspect_counts = FOREACH grouped_aspects GENERATE 
    group AS aspect, 
    COUNT(filtered_aspects) AS num_comments;

STORE aspect_counts INTO '/ds200-assignments/assignment-2/output/bai2_aspect_count' USING PigStorage(';');