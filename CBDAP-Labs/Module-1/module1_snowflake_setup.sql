-- ============================================================================
-- Module 1: Snowflake Big Data Support - Volume & Variety Demonstration
-- ============================================================================
-- This script demonstrates how Snowflake supports the Volume and Variety 
-- aspects of Big Data through JSON data handling with VARIANT columns
-- ============================================================================

-- Step 1: Create Database
-- ============================================================================
CREATE OR REPLACE DATABASE bigdata_demo
COMMENT = 'Database for demonstrating Snowflake Big Data capabilities';

-- Step 2: Create Schema
-- ============================================================================
CREATE OR REPLACE SCHEMA bigdata_demo.json_demo
COMMENT = 'Schema for JSON data variety demonstrations';

-- Step 3: Create Table with VARIANT Column for JSON Data
-- ============================================================================
CREATE OR REPLACE TABLE bigdata_demo.json_demo.customer_data (
    id NUMBER AUTOINCREMENT PRIMARY KEY,
    customer_info VARIANT,
    raw_json VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_source STRING
)
COMMENT = 'Table demonstrating VARIANT column for storing and querying JSON data of various structures';

-- Step 4: Create a staging table for loading data
-- ============================================================================
CREATE OR REPLACE TABLE bigdata_demo.json_demo.staging_customer_data (
    json_data VARIANT
);

-- Step 5: Create file format for JSON loading
-- ============================================================================
CREATE OR REPLACE FILE FORMAT bigdata_demo.json_demo.json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE
COMMENT = 'File format for loading JSON data';

-- Step 6: Create a stage for loading JSON files
-- ============================================================================
CREATE OR REPLACE STAGE bigdata_demo.json_demo.json_stage
FILE_FORMAT = json_format
COMMENT = 'Stage for loading JSON files into Snowflake';

-- ============================================================================
-- Volume Support Demonstration:
-- Snowflake can handle massive volumes of JSON data through:
-- 1. Automatic partitioning and clustering
-- 2. Columnar storage with compression
-- 3. Elastic scaling of compute resources
-- 4. Parallel processing of large JSON datasets
-- ============================================================================

-- ============================================================================
-- Variety Support Demonstration:
-- Snowflake handles diverse JSON structures through:
-- 1. VARIANT column type - stores JSON in optimized binary format
-- 2. Semi-structured data querying with dot notation
-- 3. Automatic schema evolution
-- 4. Type inference and casting
-- ============================================================================
