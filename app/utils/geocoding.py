"""

"""
import os
import sys
import requests
import googlemaps
import json

from pathlib import Path
cwd = Path(__file__).parent.parent.parent
sys.path.append(str(cwd))
from env import ENV

from utils.aws import S3Connector
import geopandas as gpd
import fiona

# print(gpd.io.file.fiona.drvsupport.supported_drivers.keys())
# exit()
class Geocode:
    def __init__(self,address):
        google_api_key = os.getenv("GOOGLE_API_KEY",None)
        if google_api_key is None: google_api_key = ENV.GOOGLE_API_KEY
        
        self.client = googlemaps.Client(key = google_api_key)
        self.address = address

    def get_latlon(self):
        result = self.client.geocode(self.address)
        try:
            post_code = result[0]["address_components"][4]["long_name"].replace("-","")
            latitude = result[0]["geometry"]["location"]["lat"]
            longitude = result[0]["geometry"]["location"]["lng"]
            # location_type = result[0]["geometry"]["location_type"]
        except:
            post_code = None
            latitude,longitude = None, None
        
        if post_code is not None:
            pref_code = self.get_pref(post_code)
        
        return latitude,longitude,pref_code
    
    def get_pref(self,post_code):
        
        ## 郵便番号API: http://zipcloud.ibsnet.co.jp/doc/api
        res = requests.get("https://zipcloud.ibsnet.co.jp/api/search?zipcode={}".format(post_code))
        if res.status_code == 200:
            try:
                pref_code = json.loads(res.content.decode('utf-8'))["results"][0]["prefcode"]
            except:
                pref_code = None
        else:
            pref_code = None
        return pref_code

class HinanPoint:
    def __init__(self,environment,aws_config,info):
        """
        environment: development/production
        aws_config:
        info[list] lat,lon,pref_code 東京:13
        """
        self.s3_connector = S3Connector(environment,aws_config)
        self.info = info
    
    def read_shp_csv_to_s3bucket(self,object="tmp"):
        
        # shp_name = "/Users/sori-mac-v1/Downloads/P20-12_13_GML/P20-12_13.shp"
        shp_name = "/Users/sori-mac-v1/work/LineBot/data/P20-12_13_GML/P20-12_13.shp"
        
        # with fiona.open(shp_name, "r") as shp:
        #     features = [feature for feature in shp]
        
        # print(features)
        # exit()
        print(os.path.abspath("../../data/tmp.csv"))
        exit()
        # # fiona.open("s3://development-resme/data/hinanjo/P20-12_13_GML/P20-12_13.shp")
        gdf = gpd.read_file(shp_name,driver = 'ESRI Shapefile',encoding="cp932")
        """ https://nlftp.mlit.go.jp/ksj/gml/datalist/KsjTmplt-P20.html (情報)"""
        renames = {
            "緯度" : "lat",
            "経度" : "lon",
            "P20_002" : "name",
            "P20_003" : "address",
            "P20_005" : "capasity",
            "P20_007" : "flgEQ",
            "P20_008" : "flgTSU",
            "P20_009" : "flgFL",
            "P20_010" : "flgCLU",
            "P20_011" : "flgOTHER",
            "P20_012" : "flgALL",
        }
        gdf = gdf[list(renames.keys())]
        gdf = gdf.rename(columns = renames)
        gdf.to_csv("../../data/tmp.csv",index=False,encoding="cp932")
        
        ##s3-upload
        self.s3_connector.file_upload(os.path.abspath("../../data/tmp.csv"), "data/hinanjo/P20-12_13_GML" , "pref13.csv")    
        print(gdf.columns)
        pass
        
if __name__ == "__main__":
    address= "東京都板橋区上板橋"
    
    gmap = Geocode(address)
    info = gmap.get_latlon()
    print(info)