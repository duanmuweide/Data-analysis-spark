CREATE DATABASE IF NOT EXISTS spark_ecommerce
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

CREATE USER IF NOT EXISTS 'spark'@'%' IDENTIFIED BY 'spark123456';
GRANT ALL PRIVILEGES ON spark_ecommerce.* TO 'spark'@'%';
FLUSH PRIVILEGES;

USE spark_ecommerce;

CREATE TABLE IF NOT EXISTS ads_sales_time_trend (
  stat_date DATE NOT NULL,
  stat_hour INT NOT NULL,
  order_count BIGINT NOT NULL,
  user_count BIGINT NOT NULL,
  total_amount DECIMAL(18, 2) NOT NULL,
  discount_amount DECIMAL(18, 2) NOT NULL,
  pay_amount DECIMAL(18, 2) NOT NULL,
  avg_order_amount DECIMAL(18, 2) NOT NULL,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (stat_date, stat_hour)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_category_sales_rank (
  stat_date DATE NOT NULL,
  category VARCHAR(50) NOT NULL,
  order_count BIGINT NOT NULL,
  quantity_sum BIGINT NOT NULL,
  amount_sum DECIMAL(18, 2) NOT NULL,
  avg_item_amount DECIMAL(18, 2) NOT NULL,
  channel_count BIGINT NOT NULL,
  category_rank INT NOT NULL,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (stat_date, category),
  KEY idx_category_rank (stat_date, category_rank)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_user_value_level (
  user_id VARCHAR(20) NOT NULL,
  province VARCHAR(50),
  city VARCHAR(50),
  member_level VARCHAR(20),
  recency_days INT NOT NULL,
  frequency BIGINT NOT NULL,
  monetary DECIMAL(18, 2) NOT NULL,
  r_score INT NOT NULL,
  f_score INT NOT NULL,
  m_score INT NOT NULL,
  total_score INT NOT NULL,
  user_level VARCHAR(30) NOT NULL,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id),
  KEY idx_user_level (user_level),
  KEY idx_member_level (member_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS ads_category_association_rules (
  antecedent VARCHAR(50) NOT NULL,
  consequent VARCHAR(50) NOT NULL,
  support DECIMAL(10, 6) NOT NULL,
  confidence DECIMAL(10, 6) NOT NULL,
  lift DECIMAL(10, 6) NOT NULL,
  pair_order_count BIGINT NOT NULL,
  antecedent_order_count BIGINT NOT NULL,
  consequent_order_count BIGINT NOT NULL,
  total_basket_count BIGINT NOT NULL,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (antecedent, consequent),
  KEY idx_lift (lift),
  KEY idx_confidence (confidence)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS rt_category_window_sales (
  window_start DATETIME NOT NULL,
  window_end DATETIME NOT NULL,
  category VARCHAR(50) NOT NULL,
  order_count BIGINT NOT NULL,
  pay_amount DECIMAL(18, 2) NOT NULL,
  batch_id BIGINT NOT NULL,
  update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (window_start, window_end, category),
  KEY idx_update_time (update_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;