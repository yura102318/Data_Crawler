-- 为race_events表添加新字段
-- 执行前请备份数据库！

-- 日期拆分字段
ALTER TABLE race_events
ADD COLUMN year INT COMMENT '年份' AFTER event_date;

ALTER TABLE race_events
ADD COLUMN month INT COMMENT '月份' AFTER year;

ALTER TABLE race_events
ADD COLUMN day INT COMMENT '日' AFTER month;

-- 地点拆分字段
ALTER TABLE race_events
ADD COLUMN province VARCHAR(100) COMMENT '省份' AFTER location;

ALTER TABLE race_events
ADD COLUMN city VARCHAR(100) COMMENT '城市' AFTER province;

ALTER TABLE race_events
ADD COLUMN county VARCHAR(100) COMMENT '区县' AFTER city;

ALTER TABLE race_events
ADD COLUMN is_domestic BOOLEAN DEFAULT TRUE COMMENT '境内/境外' AFTER county;

-- 赛事类型字段
ALTER TABLE race_events
ADD COLUMN event_type VARCHAR(50) COMMENT '赛事类型' AFTER event_level;

ALTER TABLE race_events
ADD COLUMN is_online BOOLEAN DEFAULT FALSE COMMENT '线上/线下' AFTER event_type;

-- 赛事详情字段
ALTER TABLE race_events
ADD COLUMN event_detail TEXT COMMENT '赛事详情HTML' AFTER description;

-- 创建索引（优化查询）
CREATE INDEX idx_year ON race_events(year);
CREATE INDEX idx_province ON race_events(province);
CREATE INDEX idx_event_type ON race_events(event_type);
CREATE INDEX idx_is_domestic ON race_events(is_domestic);