#!/usr/bin/env python3.7
#coding:utf-8

import datetime
import urllib
import os

class RKIcachedreports(object):
    """Encapsualtes data handling from local cache, try to fetch newer version"""
    def __init__(self,source=None,location=None):
        """"""
        self.started = datetime.datetime.now()
        self.storage_location = "data/RKIsituationreports"
        self.filename = "latest_report.csv"
        # Handle source argument
        if source == None:
            source = "https://github.com/matthiaslinden/RKI_COVID_SituationReport/"+self.storage_location+"/"+self.filename
        self.source_url = source
        # Handle (local)location argument
        if location != None:
            self.storage_location = location
        # Try to open local copy, if outdated --> fetch
        is_latest = self.OpenLocal()
        if not is_latest:
            if self.Fetch():
                print("Unable to retrieve updated source")
        
    def Fetch(self,url=None):
        """Fetch latest version from supplied url"""
        if url == None:
            url = self.source_url
        success = False
        latest_data = urllib.request.urlopen(url)
        with open(self.storage_location+"/"+self.filename,"w+") as f:
            f.write(datetime.datetime.now().isoformat()+"\n")
            f.write(latest_data)
            success = True
        if success:
            self.Parse(latest_data)
        return success
    
    def OpenLocal(self,location=None):
        """Try to open the local version and check if it's dated"""
        if location == None:
            location = self.storage_location+"/"+self.filename
            with open(location,"r") as f:
                creation_date = datetime.datetime.fromisoformat(f.readline()[:-1])
                age = datetime.datetime.now() - creation_date
                if age > datetime.timedelta(hours = 4):
                    return False
                else:
                    self.Parse(f.read())
                    return True
            return False
    
    def Parse(self,file_data):
        """Parse the inputdata"""
        print("Parse",file_data)
        
class RKIsituationreports(RKIcachedreports):
    def __init__(self,source="default"):
        baseargs = {}
        if source != "default":
            baseargs["source"] = source
        super(RKIsituationreports,self).__init__(**baseargs)

def main():
    situationreports = RKIsituationreports()

if __name__=="__main__":
	main()