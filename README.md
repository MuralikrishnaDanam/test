import sys
from datetime import datetime
import pyspark
from pyspark.sql import SparkSession

def log_events(description):
    now = datetime.now()
    print("Time: " + str(now), "|", "Description: " + description)

class DataLoader:
    referenced_tables = [
        "fullcopy_company_eu",
        "fullcopy_company_business_segment_eu",
        "fullcopy_business_segment_eu",
        "fullcopy_contact_eu",
        "fullcopy_identifier",
        "fullcopy_company"
    ]

    accounts_extract_query = """
    SELECT DISTINCT
        accountid as ID,
        '' as OUTLET_MFID_ID__C,
        '' as ACCOUNT_SIV_ID__C,
        parentaccountid as PARENTID,
        name,
        CONCAT(address1_line1,'',address1_line2,'',address1_line3) as BILLINGSTREET,
        address1_city as BILLINGCITY,
        address1_county AS BILLINGSTATE,
        address1_postalcode as BILLINGPOSTALCODE,
        van_countryid_name as BILLINGCOUNTRY,
        telephone1 as PHONE,
        websiteurl as WEBSITE,
        van_fcanumber,
        '' as SIV_OFFICE_STATUS__C,
        van_addresstype,
        van_vanguardclientstatus,
        van_companysubtypeid,
        van_territoryid,
        van_lov_tierid,
        '' as IS_PROSPECT__C,
        '' as DUPLICATE_GROUP_ID__C
    FROM fullcopy_company_eu company
    LEFT JOIN fullcopy_company_business_segment_eu  segment
    ON company.accountid = segment.van_companyid
    LEFT JOIN fullcopy_business_segment_eu  businesssegment
    ON segment.van_businesssegment = businesssegment.van_businesssegmentid
    WHERE van_countryid_name = 'United Kingdom'
    AND company.statuscode = 'Active'
    AND segment.statuscode = 'Active'
    AND businesssegment.van_name like '%UK%'
    AND name not in ('UK Direct Retail','UK retail direct','Unspecified Client - United Kingdom')
    """

    contacts_extract_query = """
    SELECT DISTINCT
        contactid as ID,
        '' as CONTACT_SIV_ID__C,
        '' as CONTACT__MFID_ID__C,
        accountid,
        van_salutations as SALUTATION,
        firstname,
        lastname,
        jobtitle,
        contact.telephone1 as TELEPHONE,
        mobilephone,
        emailaddress1 as EMAIL,
        '' as LINKEDIN,
        CONCAT(contact.address1_line1, '', contact.address1_line2, '', contact.address1_line3) as MAILINGSTREET,
        contact.address1_city as MAILINGCITY,
        contact.address1_county as MAILINGSTATE,
        contact.address1_postalcode as MAILINGPOSTALCODE,
        contact.address1_country as MAILINGCOUNTRY,
        van_identifier,
        '' as IS_PROSPECT__C,
        '' as MOVER_STATUS__C,
        '' as DUPLICATE_GROUP_ID__C
    FROM fullcopy_contact_eu  contact
    LEFT JOIN fullcopy_identifier  identifier  
    ON contact.contactid = identifier.van_customerid
    LEFT JOIN fullcopy_company  company
    ON contact.parentcustomerid = company.accountid
    WHERE contact.address1_country = 'United Kingdom'
    AND contact.statuscode = 'Active'
    AND name <> 'The Vanguard Group'
    """

    platforms_extract_query = """
    SELECT accountid AS ID,
    name as FC_PLATFORM_NAME
    FROM fullcopy_company_eu
    WHERE van_companysubtypeid = 'Platform' AND van_vanguardclientstatus = 'Existing Client' AND van_startdate = '2019-01-01'
    """

    def __init__(self):
    # Create Spark session
        spark = SparkSession \
            .builder \
            .appName('Generate Salesforce Audience Builder CSV file') \
            .enableHiveSupport() \
            .getOrCreate()

    def run_query(self, query, destination, name):
        df = self.spark.sql(query)
        df = df.toDF(*[c.upper() for c in df.columns])  # Make column names all uppercase
        (df.coalesce(1)
         .write.mode('overwrite')
         .format('csv')
         .option("header", True)
         .option("emptyValue", '')
         .save(destination))
        log_events("Successfully extracted {name} data and saved to sandbox bucket")

    def load(self):
        accounts_destination = "s3://vgi-ics-int-us-east-1-ukb2b-sandbox/accounts"
        contacts_destination = "s3://vgi-ics-int-us-east-1-ukb2b-sandbox/contacts"
        platforms_destination = "s3://vgi-ics-int-us-east-1-ukb2b-sandbox/platforms"

        self.run_query(self,self.accounts_extract_query, accounts_destination, "accounts")
        self.run_query(self,self.contacts_extract_query, contacts_destination, "contacts")
        self.run_query(self,self.platforms_extract_query, platforms_destination, "platforms")


if __name__ == '__main__':
    log_events("Start Extraction")
    data_loader = DataLoader()
    data_loader.load()
