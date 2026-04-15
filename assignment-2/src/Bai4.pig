-- 1. LOAD DỮ LIỆU ĐÃ TÁCH TỪ (BÀI 1)
clean_words = LOAD '/ds200-assignments/assignment-2/output/bai1_output/part-r-00000' 
              USING PigStorage(';') 
              AS (id:chararray, word:chararray);

-- 2. LOAD DỮ LIỆU GỐC (Lấy đúng cột Category là cột thứ 3)
raw_data = LOAD '/ds200-assignments/assignment-2/hotel-review.csv' 
           USING PigStorage(';') 
           AS (id:chararray, review:chararray, category:chararray, aspect:chararray, sentiment:chararray);

-- 3. JOIN ĐỂ GẮN NHÃN CATEGORY VÀ SENTIMENT CHO TỪ
word_full_info = JOIN clean_words BY id, raw_data BY id;

-- Lấy các cột cần thiết: từ, phân loại (category) và sắc thái (sentiment)
needed_data = FOREACH word_full_info GENERATE 
    clean_words::word AS word, 
    raw_data::category AS category,
    raw_data::sentiment AS sentiment;

-- 4. TÍNH TẦN SUẤT TỪ THEO (CATEGORY + SENTIMENT)
grouped_data = GROUP needed_data BY (category, sentiment, word);
word_counts = FOREACH grouped_data GENERATE 
    FLATTEN(group) AS (category, sentiment, word), 
    COUNT(needed_data) AS count;

-- 5. XÁC ĐỊNH 5 TỪ TÍCH CỰC NHẤT THEO TỪNG CATEGORY
-- Ở đây mình sẽ lọc theo sentiment 'positive'
positives = FILTER word_counts BY sentiment == 'positive';
grouped_pos = GROUP positives BY category;
top5_positive = FOREACH grouped_pos {
    sorted = ORDER positives BY count DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

-- 6. XÁC ĐỊNH 5 TỪ TIÊU CỰC NHẤT THEO TỪNG CATEGORY
-- Ở đây mình sẽ lọc theo sentiment 'negative'
negatives = FILTER word_counts BY sentiment == 'negative';
grouped_neg = GROUP negatives BY category;
top5_negative = FOREACH grouped_neg {
    sorted = ORDER negatives BY count DESC;
    top = LIMIT sorted 5;
    GENERATE FLATTEN(top);
};

-- 7. STORE KẾT QUẢ (Dùng Tab cho chuyên nghiệp)
STORE top5_positive INTO '/ds200-assignments/assignment-2/output/bai4_top5_pos_by_category' USING PigStorage('\t');
STORE top5_negative INTO '/ds200-assignments/assignment-2/output/bai4_top5_neg_by_category' USING PigStorage('\t');