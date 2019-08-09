import numpy as np
import pandas as pd
import os
import IP2Location
import uuid

# Reading customer1, customer2 and Iso country code master csv file through pandas read_csv and store into DataFrame.
def loadAndMerge_csvfiles():
    df_customer1 = pd.read_csv("customer1.csv")
    df_customer2 = pd.read_csv("customer2.csv")
    df_out = pd.DataFrame()
    df3 = df_customer1.merge(df_customer2,on="id", how="outer")
    df_countrymaster =  pd.read_csv("data/wikipedia-iso-country-codes.csv",keep_default_na=False)
    return df3, df_countrymaster

# This Function is used to derive geo location like city, state, country, zipcode, country alpha 2  from customer's IPv4 Address.
def getGeoLocations_fromIPAddress(df_out):
    country_short,country,city,zipcode,region,locale,referral_code = ([] for i in range(7))
    IP2LocObj = IP2Location.IP2Location()
    for index,rows in df_out.iterrows():
        if pd.notnull(rows['ip']):
            IP2LocObj.open("data/IP-COUNTRY-REGION-CITY-LATITUDE-LONGITUDE-ZIPCODE-TIMEZONE-ISP-DOMAIN-NETSPEED-AREACODE-WEATHER-MOBILE-ELEVATION-USAGETYPE-SAMPLE.BIN")
            rec = IP2LocObj.get_all(rows['ip'])
            country_short.append(rec.country_short)
            zipcode.append(rec.zipcode)
            region.append(rec.region)
            country.append(rec.country_long)
            city.append(rec.city)
            locale.append('en-' + rec.country_short.lower())
        else:
            country_short.append(np.NaN)
            zipcode.append(np.NaN)
            region.append(np.NaN)
            country.append(np.NaN)
            city.append(np.NaN)
            locale.append(np.NaN)
        random = str(uuid.uuid4()).upper().replace("-","")
        referral_code.append(random[0:10])
    return region,city,zipcode,country,country_short,locale,referral_code


# This function is used to create the  final output dataframe that will be exported eventually
def dataTransformation():
    df3,df_countrymaster = loadAndMerge_csvfiles()
    df_out = pd.DataFrame()
    df_out['external_id'] = df3['id']
    df_out['opted_in'] = np.where(df3['tier'].isnull(), True, False)
    df_out['external_id_type'] = 'file'
    df_out['email'] = df3['email']
    df_out.loc[df_out['external_id'] == '4903g34', 'ip'] = '19.5.10.1'
    df_out.loc[df_out['external_id'].isin(['jh41922','48982nf']),'ip'] = '14.141.149.158'
    region,city,zipcode,country,country_short,locale,referral_code = getGeoLocations_fromIPAddress(df_out)
    df_out['locale'] = locale
    df_out = df_out[['external_id','opted_in','external_id_type','email','locale','ip']]
    df_out['dob'] = None
    df_out['address'] = None
    df_out['city'] = city
    df_out['state'] = region
    df_out['zip'] = zipcode
    df_out['countrycode_alpha2'] = country_short
    df_countrylink = df_out.loc[:,['countrycode_alpha2']].merge(df_countrymaster.loc[:,['Alpha-2 code','Alpha-3 code']],left_on='countrycode_alpha2',right_on='Alpha-2 code', how='inner')
    df_out['country'] = df_countrylink['Alpha-3 code']
    if "countrycode_alpha2" in df_out.columns:
        df_out.drop('countrycode_alpha2',axis=1,inplace=True)
    df_out['gender'] = np.where(df3['sex'] == 0, 'm','f')
    df_out['first_name'] = df3['first_name']
    df_out['last_name'] = df3['last_name']
    df_out['referral'] = referral_code
    df_out['phone_numbers'] = df3['attr2']
    return df_out

df_new = dataTransformation()
df_new.to_csv("out.csv",index=False)