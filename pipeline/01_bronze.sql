-- Bronze Layer: Streaming ingestion from source tables
-- Each table mirrors the source with no transformations

CREATE OR REFRESH STREAMING TABLE bronze_appeals
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.appeals);

CREATE OR REFRESH STREAMING TABLE bronze_members
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.members);

CREATE OR REFRESH STREAMING TABLE bronze_eligibility
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.eligibility);

CREATE OR REFRESH STREAMING TABLE bronze_providers
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.providers);

CREATE OR REFRESH STREAMING TABLE bronze_prior_authorizations
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.prior_authorizations);

CREATE OR REFRESH STREAMING TABLE bronze_claims
AS SELECT * FROM STREAM(hls_amer_catalog.`appeals-review`.claims);
