-- 1. Load data
reviews = LOAD '/ds200-assignments/assignment-2/hotel-review.csv' USING PigStorage(';') 
          AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, sentiment:chararray);

-- 2. Load stopwords
stopwords = LOAD '/ds200-assignments/assignment-2/stopwords.txt' AS (word:chararray);

-- clean, lowercase, word segment
lower_data = FOREACH reviews GENERATE 
    id, 
    LOWER(REPLACE(review,'[0-9,.&@!%+/\\\\?\\\':()>^~*#=\\[\\]{}"-]', '')) AS review,
    topic,
    aspect,
    sentiment;


tokenized_data = FOREACH lower_data GENERATE 
    id, 
    FLATTEN(TOKENIZE(review)) AS word,
    topic,
    aspect,
    sentiment;

filtered_words = FILTER tokenized_data BY word IS NOT NULL AND SIZE(word) > 0;

joined_data = JOIN filtered_words BY word LEFT OUTER, stopwords BY word;
-- collect all data of left, if not in stopword => null in right

-- keep words that not in stopwords table (null in right)
filtered_result = FILTER joined_data BY stopwords::word IS NULL;

-- output
final_output = FOREACH filtered_result GENERATE 
    filtered_words::id AS id, 
    filtered_words::word AS word,
    filtered_words::topic as topic,
    filtered_words::aspect as aspect,
    filtered_words::sentiment as sentiment;

-- 4. export output into HDFS
STORE final_output INTO '/ds200-assignments/assignment-2/output/bai1_output' USING PigStorage(';');