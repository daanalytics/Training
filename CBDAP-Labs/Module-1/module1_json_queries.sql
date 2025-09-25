-- ============================================================================
-- Module 1: Snowflake JSON Querying Demonstrations
-- ============================================================================
-- This script demonstrates various ways to query JSON data stored in VARIANT columns
-- showcasing Snowflake's ability to handle data variety and volume
-- ============================================================================

-- Use the database and schema
USE DATABASE bigdata_demo;
USE SCHEMA json_demo;

-- ============================================================================
-- STEP 1: Load Sample JSON Data
-- ============================================================================

-- Insert sample JSON data directly into the table
INSERT INTO customer_data (customer_info, file_source)
VALUES 
('{
  "customer_id": "CUST001",
  "personal_info": {
    "first_name": "John",
    "last_name": "Doe",
    "email": "john.doe@email.com",
    "phone": "+1-555-0123",
    "date_of_birth": "1985-03-15",
    "address": {
      "street": "123 Main St",
      "city": "New York",
      "state": "NY",
      "zip_code": "10001",
      "country": "USA"
    }
  },
  "preferences": {
    "communication": {
      "email_notifications": true,
      "sms_notifications": false,
      "preferred_language": "en"
    },
    "shopping": {
      "favorite_categories": ["electronics", "books", "clothing"],
      "budget_range": {
        "min": 100,
        "max": 1000,
        "currency": "USD"
      }
    }
  },
  "activity": {
    "registration_date": "2023-01-15T10:30:00Z",
    "last_login": "2024-01-20T14:22:33Z",
    "total_orders": 45,
    "lifetime_value": 2340.50,
    "loyalty_tier": "Gold"
  },
  "metadata": {
    "source": "web_registration",
    "campaign_id": "SPRING2023",
    "referral_code": "FRIEND123",
    "tags": ["high_value", "frequent_buyer", "tech_savvy"]
  }
}', 'direct_insert'),

('{
  "customer_id": "CUST002",
  "personal_info": {
    "first_name": "Sarah",
    "last_name": "Smith",
    "email": "sarah.smith@email.com",
    "phone": "+1-555-0456",
    "date_of_birth": "1992-07-22",
    "address": {
      "street": "456 Oak Ave",
      "city": "Los Angeles",
      "state": "CA",
      "zip_code": "90210",
      "country": "USA"
    }
  },
  "preferences": {
    "communication": {
      "email_notifications": true,
      "sms_notifications": true,
      "preferred_language": "en"
    },
    "shopping": {
      "favorite_categories": ["beauty", "fashion", "home"],
      "budget_range": {
        "min": 50,
        "max": 500,
        "currency": "USD"
      }
    }
  },
  "activity": {
    "registration_date": "2023-06-10T16:45:00Z",
    "last_login": "2024-01-19T09:15:22Z",
    "total_orders": 23,
    "lifetime_value": 890.25,
    "loyalty_tier": "Silver"
  },
  "metadata": {
    "source": "mobile_app",
    "campaign_id": "SUMMER2023",
    "referral_code": null,
    "tags": ["beauty_enthusiast", "social_media_user"]
  }
}', 'direct_insert');

-- ============================================================================
-- STEP 2: Basic JSON Querying Examples
-- ============================================================================

-- Query 1: Extract basic customer information using dot notation
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:personal_info.last_name::STRING as last_name,
    customer_info:personal_info.email::STRING as email
FROM customer_data;

-- Query 2: Extract nested address information
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:personal_info.address.city::STRING as city,
    customer_info:personal_info.address.state::STRING as state,
    customer_info:personal_info.address.zip_code::STRING as zip_code
FROM customer_data;

-- Query 3: Extract array data (favorite categories)
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:preferences.shopping.favorite_categories as categories,
    ARRAY_SIZE(customer_info:preferences.shopping.favorite_categories) as category_count
FROM customer_data;

-- ============================================================================
-- STEP 3: Advanced JSON Querying Examples
-- ============================================================================

-- Query 4: Flatten array data to analyze individual categories
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    f.value::STRING as favorite_category
FROM customer_data,
LATERAL FLATTEN(input => customer_info:preferences.shopping.favorite_categories) f;

-- Query 5: Extract numeric values and perform calculations
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:activity.total_orders::NUMBER as total_orders,
    customer_info:activity.lifetime_value::NUMBER(10,2) as lifetime_value,
    customer_info:activity.lifetime_value::NUMBER(10,2) / customer_info:activity.total_orders::NUMBER as avg_order_value
FROM customer_data;

-- Query 6: Query with conditions on JSON data
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:activity.loyalty_tier::STRING as loyalty_tier,
    customer_info:activity.lifetime_value::NUMBER(10,2) as lifetime_value
FROM customer_data
WHERE customer_info:activity.loyalty_tier::STRING IN ('Gold', 'Platinum');

-- ============================================================================
-- STEP 4: Handling JSON Variety (Different Structures)
-- ============================================================================

-- Query 7: Handle optional fields that may not exist in all records
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:metadata.referral_code::STRING as referral_code,
    customer_info:metadata.custom_fields.industry::STRING as industry,
    customer_info:metadata.social_media.followers::NUMBER as social_followers
FROM customer_data;

-- Query 8: Use GET_PATH function for safer navigation
SELECT 
    customer_info:customer_id::STRING as customer_id,
    GET_PATH(customer_info, 'personal_info.first_name')::STRING as first_name,
    GET_PATH(customer_info, 'metadata.custom_fields.industry')::STRING as industry,
    GET_PATH(customer_info, 'metadata.social_media.followers')::NUMBER as social_followers
FROM customer_data;

-- Query 9: Extract tags and analyze customer segments
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:metadata.tags as tags,
    customer_info:metadata.source::STRING as registration_source
FROM customer_data
WHERE ARRAY_CONTAINS('high_value'::VARIANT, customer_info:metadata.tags);

-- ============================================================================
-- STEP 5: Aggregation and Analytics on JSON Data
-- ============================================================================

-- Query 10: Aggregate customer data by loyalty tier
SELECT 
    customer_info:activity.loyalty_tier::STRING as loyalty_tier,
    COUNT(*) as customer_count,
    AVG(customer_info:activity.lifetime_value::NUMBER(10,2)) as avg_lifetime_value,
    AVG(customer_info:activity.total_orders::NUMBER) as avg_orders
FROM customer_data
GROUP BY customer_info:activity.loyalty_tier::STRING
ORDER BY avg_lifetime_value DESC;

-- Query 11: Analyze customer preferences by registration source
SELECT 
    customer_info:metadata.source::STRING as registration_source,
    COUNT(*) as customer_count,
    ARRAY_AGG(DISTINCT customer_info:preferences.communication.preferred_language::STRING) as languages,
    ARRAY_AGG(DISTINCT customer_info:preferences.shopping.budget_range.currency::STRING) as currencies
FROM customer_data
GROUP BY customer_info:metadata.source::STRING;

-- Query 12: Find customers by geographic distribution
SELECT 
    customer_info:personal_info.address.state::STRING as state,
    customer_info:personal_info.address.city::STRING as city,
    COUNT(*) as customer_count,
    AVG(customer_info:activity.lifetime_value::NUMBER(10,2)) as avg_lifetime_value
FROM customer_data
WHERE customer_info:personal_info.address.state::STRING IS NOT NULL
GROUP BY customer_info:personal_info.address.state::STRING, 
         customer_info:personal_info.address.city::STRING
ORDER BY customer_count DESC;

-- ============================================================================
-- STEP 6: JSON Functions and Transformations
-- ============================================================================

-- Query 13: Create a flattened view of customer data
CREATE OR REPLACE VIEW customer_data_flattened AS
SELECT 
    customer_info:customer_id::STRING as customer_id,
    customer_info:personal_info.first_name::STRING as first_name,
    customer_info:personal_info.last_name::STRING as last_name,
    customer_info:personal_info.email::STRING as email,
    customer_info:personal_info.phone::STRING as phone,
    customer_info:personal_info.address.city::STRING as city,
    customer_info:personal_info.address.state::STRING as state,
    customer_info:activity.total_orders::NUMBER as total_orders,
    customer_info:activity.lifetime_value::NUMBER(10,2) as lifetime_value,
    customer_info:activity.loyalty_tier::STRING as loyalty_tier,
    customer_info:metadata.source::STRING as registration_source,
    customer_info:preferences.communication.email_notifications::BOOLEAN as email_notifications,
    customer_info:preferences.communication.sms_notifications::BOOLEAN as sms_notifications,
    customer_info:preferences.shopping.budget_range.min::NUMBER as budget_min,
    customer_info:preferences.shopping.budget_range.max::NUMBER as budget_max,
    customer_info:created_at::TIMESTAMP_NTZ as created_at
FROM customer_data;

-- Query 14: Use the flattened view for standard SQL queries
SELECT 
    loyalty_tier,
    COUNT(*) as customer_count,
    AVG(lifetime_value) as avg_lifetime_value,
    AVG(total_orders) as avg_orders
FROM customer_data_flattened
GROUP BY loyalty_tier
ORDER BY avg_lifetime_value DESC;

-- ============================================================================
-- STEP 7: JSON Data Manipulation
-- ============================================================================

-- Query 15: Update JSON data by adding new fields
UPDATE customer_data 
SET customer_info = OBJECT_INSERT(
    customer_info,
    'last_updated',
    CURRENT_TIMESTAMP()::STRING,
    TRUE
)
WHERE customer_info:customer_id::STRING = 'CUST001';

-- Query 16: Create a new JSON structure from existing data
SELECT 
    OBJECT_CONSTRUCT(
        'customer_summary',
        OBJECT_CONSTRUCT(
            'id', customer_info:customer_id::STRING,
            'name', CONCAT(customer_info:personal_info.first_name::STRING, ' ', customer_info:personal_info.last_name::STRING),
            'tier', customer_info:activity.loyalty_tier::STRING,
            'value', customer_info:activity.lifetime_value::NUMBER(10,2),
            'orders', customer_info:activity.total_orders::NUMBER
        )
    ) as customer_summary
FROM customer_data;

-- ============================================================================
-- Volume and Variety Support Summary:
-- ============================================================================
-- VOLUME: Snowflake can handle massive volumes of JSON data through:
-- 1. Columnar storage with automatic compression
-- 2. Automatic partitioning and clustering
-- 3. Elastic scaling of compute resources
-- 4. Parallel processing of large datasets
-- 5. Micro-partitions for efficient querying
--
-- VARIETY: Snowflake handles diverse JSON structures through:
-- 1. VARIANT column type - stores JSON in optimized binary format
-- 2. Semi-structured data querying with dot notation and functions
-- 3. Automatic schema evolution - new fields don't break existing queries
-- 4. Type inference and automatic casting
-- 5. Functions like GET_PATH for safe navigation of nested structures
-- 6. LATERAL FLATTEN for handling arrays
-- 7. OBJECT_CONSTRUCT for building new JSON structures
-- ============================================================================
