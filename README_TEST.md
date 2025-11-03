# ETL Test Suite

## Overview
This test suite provides comprehensive testing for the ETL.scala script without requiring Oracle database credentials or Kudu cluster access.

## Running Tests

### Prerequisites
- sbt (Scala Build Tool) installed
- Java 8 or higher

### Execute Tests
```bash
sbt test
```

## Test Structure

### Test Files
- `src/test/scala/etl/ETLJobTest.scala` - Main test suite with mocked data

### What is Tested
1. **Schema Validation**: Verifies that the DataFrame has all expected columns
2. **Timezone Adjustments**: Tests the +3/24 hour adjustment for activation dates
3. **CASE Logic**: Validates packageplanid and deactivationdate CASE statements
4. **Metadata Injection**: Confirms bdl_* metadata fields are present and correct
5. **Active/Inactive Status**: Tests different status scenarios

### Mocking Strategy
- **Oracle JDBC Connection**: Replaced with directly created DataFrames using sample data
- **Kudu Operations**: Tests verify data structure without actual Kudu writes
- **Sample Data**: Represents the expected output from the Oracle SQL query after transformations

## Sample Test Data
The test suite includes mock data representing:
- Active assets (status_cd = 'Active', deactivationdate = '2099-12-31')
- Inactive assets (status_cd = 'Inactive', deactivationdate = actual date + 3/24 hours)
- Various packageplanid scenarios (sp_num vs part_num)
- Timezone-adjusted timestamps (created + 3/24 hours)
- Metadata fields (bdl_created_date, bdl_db_created_date, bdl_pipelineid, bdl_source)

## Notes
- The original ETL.scala remains unchanged for backward compatibility
- All tests run in Spark local mode
- No external database connections are required
- Tests verify transformations match the SQL query logic in ETL.scala lines 26-59
