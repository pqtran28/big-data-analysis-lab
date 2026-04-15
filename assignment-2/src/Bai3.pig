raw_data = LOAD '/ds200-assignments/assignment-2/hotel-review.csv' 
           USING PigStorage(';') 
           AS (id:chararray, review:chararray, topic:chararray, aspect:chararray, category:chararray);

valid_data = FILTER raw_data BY aspect IS NOT NULL AND aspect != '' AND category IS NOT NULL;

-- 3. GOM NHÓM THEO CẢ ASPECT VÀ CATEGORY
grouped_data = GROUP valid_data BY (aspect, category);

-- 4. TÍNH SỐ LƯỢNG CHO TỪNG CẶP (ASPECT - CATEGORY)
counts = FOREACH grouped_data GENERATE 
    FLATTEN(group) AS (aspect, category), 
    COUNT(valid_data) AS num_reviews;

-- 5. TÌM KHÍA CẠNH TÍCH CỰC NHẤT (POSITIVE)
positives = FILTER counts BY category == 'positive';
sorted_positives = ORDER positives BY num_reviews DESC;
top_positive = LIMIT sorted_positives 1;

-- 6. TÌM KHÍA CẠNH TIÊU CỰC NHẤT (NEGATIVE)
negatives = FILTER counts BY category == 'negative';
sorted_negatives = ORDER negatives BY num_reviews DESC;
top_negative = LIMIT sorted_negatives 1;

-- 7. LƯU KẾT QUẢ
STORE top_positive INTO '/ds200-assignments/assignment-2/output/bai3_top_positive' USING PigStorage(';');
STORE top_negative INTO '/ds200-assignments/assignment-2/output/bai3_top_negative' USING PigStorage(';');