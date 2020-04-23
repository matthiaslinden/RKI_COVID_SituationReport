#!/usr/bin/env python3.7
#coding:utf-8

import datetime
from urllib import request
import os
import pandas as pd
import numpy as np

class RKIcachedreports(object):
    """Encapsualtes data handling from local cache, try to fetch newer version"""
    def __init__(self,source=None,location=None,force_download=False):
        """"""
        self.started = datetime.datetime.now()
        self.storage_location = "data/RKIsituationreports"
        self.filename = "latest_report.csv"
        # Handle source argument
        if source == None:
            source = "https://github.com/matthiaslinden/RKI_COVID_SituationReport/raw/master/"+self.storage_location+"/"+self.filename
        self.source_url = source
        # Handle (local)location argument
        if location != None:
            self.storage_location = location
        # Try to open local copy, if outdated --> fetch
        if not force_download:
            is_latest = self.OpenLocal()
        else:
            is_latest = False
        if not is_latest:
            if not self.Fetch():
                print("Unable to retrieve updated source")
        
    def Fetch(self,url=None):
        """Fetch latest version from supplied url"""
        if url == None:
            url = self.source_url
        success = False
        remote_file = request.urlopen(url)
        creation_date = datetime.datetime.fromisoformat(remote_file.readline().decode("utf-8")[:-1])
#        latest_data = remote_file.read()
        with open(self.storage_location+"/"+self.filename,"wb+") as f:
            f.write((datetime.datetime.now().isoformat()+"\n").encode("utf-8"))
            f.write(latest_data)
            success = True
        if success:
            print("Fetched remote situation report, as of %s"%(creation_date.isoformat()))
#            self.Parse(latest_data.decode("utf-8"))
            self.Parse(remote_file)
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
                    print("Use locally cached situation report last fetched %s ago"%age)
                    self.Parse(f)#f.read())
                    return True
            return False
    
    def Parse(self,f):
        """Parse the inputdata -- Replace in derived class"""
        print("Parse",f.read())
        
class RKIsituationreports(RKIcachedreports):
    """ Abstracts a collection of RKI's situation reports """
    def __init__(self,source="default",**kwargs):
        baseargs = kwargs
        if source != "default":
            baseargs["source"] = source
        super(RKIsituationreports,self).__init__(**baseargs)
        
    def Parse(self,f):
        df = pd.read_csv(f,sep=";")
        print(df)

def main():
    situationreports = RKIsituationreports()

if __name__=="__main__":
	main()