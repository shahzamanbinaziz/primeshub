import os
import pandas as pd
from googleads import ad_manager, oauth2
import tempfile
import json

# --- CONFIGURATION ---
NETWORK_CODE = '23327488191'
APPLICATION_NAME = 'PrimeAdsHub_Reporting'

# Your Service Account Details
KEY_DATA = {
  "type": "service_account",
  "project_id": "prime-ads-hub",
  "private_key_id": "e9ea45e55a74cc1e56f91ed0d0e11961e20d7682",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC6CU7PPyKw5elT\nhbzUUYzw4FUjnY9yWtQ7GBADIlHI9nlXBxQUh9WctzlbH5NmfOF0grk5mnQqD87B\ne/8OPto7vESUL7c4PdOLE94LzddHszppI2fybx5Glk7olW4+0/5hd3A/wx8hr0D7\ndCxWV1kfofvgm+fGqCdtSBteeTL4cCKIBArHW07FZVbU7xR+SipbEkldZF/X09ol\nc9SAwyxO7uLwVGdQAIxGfT1DZ2IrkVNUJofCCTZqc/YY1dQgdewu4QUXEUXZwVv9\nQ++2TL4rkp24ygiPCChRAaDgM4fxmWIu0L13QNms6HVucqpeBkRUVRx/cqt0GKUm\nCjFDQNxdAgMBAAECggEAFuT22Svup8D/lCsI+EMZ+nnNGH83LE5PwH7/V3dlg7sL\nZ0Gkf7tQt0LYMOXjpLD5KPa+dz+SDKwZ2HdbReRHxKKweOEfZfE746l8Ac1g8T62\nEULNc8kne23bg6WCJgK+Uz3Y50vqvKE9+MQNAcopmmo7nmJpZWoDwh/l2FxHgMnM\nErO8Cn5vaIQKGmYX+zNTkeGZKTndhll8ZN5iyD1BD3nIyMWqQhYqKeBYBTOO3XXf\nMdf+OkrjeLRQpZQwdq1wWyAQlNz7hFLl4n7TgBDjFmGCTrfRlBXt0EJI2pLlRiI/\n2xbPLGkT7XEVoeuxkwIW16K9hhRhEZ0OCKYXbw7cAQKBgQDu9bmn0VwqTciRnpkt\n2ypxrUiT7zuMxCdDg/7x3z064HqRTHLkmen8/3bG5ebxwd1SUkiZpzjqWUNP12Qh\n9ITLotzENgMOWX3yPRDVRkO5+uYwVcVLcuzPcajxglX8pWVa+j472EPvKE5UY/Ca\n01UnQOQkbE3c8ZjDYWLwFi54XQKBgQDHTXNA4zToNVKejbRj3MehmLLUeIOgvI9z\nRIylioynnj7bzrOGUmA5DSk0z3tHcUYYbOZg/52jS/Uhsz/IH/Z1ZZatMqz/YYZX\nLXxKXLltuIfGtP5tvoKOfDMv8ENsDDBIdE+mtiSfFaiXvJzp8imlsst/YIIEhoLI\nE0GX1yW0AQKBgDzdoCVrwVMRLvZQdGnmuj/sSGFN/VgUmn+q/mQzXZBCn1WlKFqs\ DZqgo2t0IcgQfkQ6qz1gB7JBfFC450ty0eRgnmTn8Q1VpCvwe/onBJc5nipPnopi\nQolwRP0HGsnYgyGSPgnWQy+Gj7UVI7L8A2OVNsdEQuz1KNkTVDUdIUcNAoGALmXI\ndA2w7nIjdsf0e98VFnivASnBMvVSy/nkaFF15zu+1HstbhLVVdLLigDXaU1kjSEl\nDOXVNAPl4F+TdKqEPNZWmqGWhqmUlc0AB2vIu1NfQJI4PSJB0Jv3aqybdZbs0qFJ\nPb1fjy2CnziIqyn2Kh4So+e6vQT3g06AUbIDlAECgYEA1uExESpqYjifKW4ZKfCa\nm3RYDs0FNITiC/tU+i3m+4pCtbjKqcb4HimivEUPdx2Eh8xZlOQ+nWNEQkg72yiH\n5nksFlo3xaLQwNk+7wmvxufZAPt32Y75eju6MlteOwYGXPibFzqUusHyYQ3kRycV\nwX8a+qHII56+6MGt4y1HV0s=\n-----END PRIVATE KEY-----\n",
  "client_email": "prime-ads-hub@prime-ads-hub.iam.gserviceaccount.com",
}

def fetch_gam_data():
    # Save the key data to a temporary file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as tmp:
        json.dump(KEY_DATA, tmp)
        key_file_path = tmp.name

    try:
        # Create OAuth2 client manually to avoid "Configuration Missing" errors
        oauth2_client = oauth2.GoogleServiceAccountClient(
            key_file_path, oauth2.GetAPIScope('ad_manager'))

        # Initialize the Ad Manager client
        client = ad_manager.AdManagerClient(
            oauth2_client, APPLICATION_NAME, NETWORK_CODE)

        # Initialize the ReportService
        report_downloader = client.GetDataService('ReportService', version='v202408')

        # Define the report
        report_job = {
            'reportQuery': {
                'dimensions': ['DATE', 'AD_UNIT_NAME', 'COUNTRY_NAME'],
                'columns': ['AD_SERVER_IMPRESSIONS', 'AD_SERVER_CLICKS', 'AD_SERVER_CPM_AND_CPC_REVENUE'],
                'dateRangeType': 'LAST_7_DAYS',
            }
        }

        print("Starting Ad Manager Report Job...")
        report_job = report_downloader.runReportJob(report_job)
        
        # Download the report
        report_file = tempfile.NamedTemporaryFile(suffix='.csv.gz', delete=False)
        report_downloader.downloadReport(report_job['id'], 'CSV_DUMP', report_file)
        report_file.close()

        # Process and save the data
        df = pd.read_csv(report_file.name, compression='gzip')
        # Convert micro-currency to standard
        df['Revenue'] = df['Column.AD_SERVER_CPM_AND_CPC_REVENUE'] / 1000000
        
        df.to_csv('prime_ads_report.csv', index=False)
        print("✅ Data successfully saved to prime_ads_report.csv")

    except Exception as e:
        print(f"❌ Error during fetch: {e}")
        raise e
    finally:
        if os.path.exists(key_file_path):
            os.remove(key_file_path)

if __name__ == "__main__":
    fetch_gam_data()
