# ex-marketo-bulk

Keboola Connection docker app for fetching responses from  [Marketo REST API](http://developers.marketo.com/rest-api/bulk-extract/). Available under `kds-team.ex-marketo-bulk`

## Functionality
This component allows KBC to bulk fetch data from either Leads or Activity endpoint.

## Parameters
- Munchkin ID token - The first part of Identity. Can be found in Admin > Web Services menu in the REST API section
- Client ID token
- Client Secret token
- Endpoint - Use either `Activities` or `Leads`
- Desired Activities - Valid only for `Activities` endpoint. The `Delete Lead` activity is not supported.
- Month/Year for Created - used for backfill. Required format is MMM YYYY (e.g. Jan 2019).
- How many days back you want to go with 'Created'? - alternative to 'Month/Year for Created'. Basically if you specify e.g. '7', then the time range specified is from (Today - 7 days) to Today. If both this and 'Month/Year for Created' are used, the 'Month/Year for Created' is disregarded.
- Month/Year for Updated - used for backfill. Required format is MMM YYYY (e.g. Jan 2019).
- How many days back you want to go with 'Updated'? - alternative to 'Month/Year for Updated'. Basically if you specify e.g. '7', then the time range specified is from (Today - 7 days) to Today. If both this and 'Month/Year for Updated' are used, the 'Month/Year for Updated' is disregarded. Valid only for `Leads` endpoint.

## Outcome
For `Leads` endpoint the resulting table contains all possible columns. This is hardcoded and cannot be changed. The PK is `id`, loads are incremental.

For `Activities` endpoint the PK is `MarketoGUID`, the loads are incremental as well.
