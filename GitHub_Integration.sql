-- =====================================================
-- GitHub Integration Setup for Snowflake Workspaces
-- =====================================================

-- 1. Secret (already created)
CREATE OR REPLACE SECRET SALES_DEV.COMMON.GIT_SECRET
  TYPE = PASSWORD
  USERNAME = 'roshpatil7447-ops'
  PASSWORD = '***'  -- PAT stored securely; use ALTER SECRET to rotate
  COMMENT = 'GitHub PAT for Git integration with Snowflake workspaces';

-- 2. API Integration
CREATE OR REPLACE API INTEGRATION GIT_API_INTEGRATION
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/roshpatil7447-ops')
  ALLOWED_AUTHENTICATION_SECRETS = (SALES_DEV.COMMON.GIT_SECRET)
  ENABLED = TRUE;

-- 3. Verify
DESCRIBE SECRET SALES_DEV.COMMON.GIT_SECRET;
DESCRIBE INTEGRATION GIT_API_INTEGRATION;