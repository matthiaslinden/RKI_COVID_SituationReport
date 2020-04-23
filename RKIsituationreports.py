#!/usr/bin/env python3.7
#coding:utf-8

from io import BytesIO
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
        remote_data = remote_file.read()
        with open(self.storage_location+"/"+self.filename,"wb+") as f:
            f.write((datetime.datetime.now().isoformat()+"\n").encode("utf-8"))
            f.write(remote_data)
            success = True
        if success:
            print("Fetched remote situation report, as of %s"%(creation_date.isoformat()))
            self.Parse(BytesIO(remote_data))
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
        self.sit_df = None
        if source != "default":
            baseargs["source"] = source
        super(RKIsituationreports,self).__init__(**baseargs)
        
    def Parse(self,f):
        """Replaces base-class function, postprocess datatypes"""
        self.sit_df = pd.read_csv(f,sep=";",index_col=0)
        c = self.sit_df.columns
        for k in c:
            self.sit_df[k] = self.sit_df[k].convert_dtypes(convert_integer=True)
        self.sit_df = self.sit_df.rename(columns={"Unnamed: 0":"date"})
    
    def GetFilteredCopy(self,columns):
        return self.sit_df.filter(columns,axis=1).copy()
    
    def ICU(self,additional_columns=[]):
        columns = ["hospitalized","ICU_current","ICU_finished","ICU_deaths","ICU_beds","ICU_beds_available","ICU_clinics"]
        columns += additional_columns
        icu = self.GetFilteredCopy(columns)
        for k in columns:
            icu[k+"_perday"] = icu[k].diff()
        f,d,c = icu["ICU_finished_perday"],icu["ICU_deaths_perday"],icu["ICU_current_perday"]
        icu["ICU_entered_perday"] = c+f        
        return icu 
    
    def Symptoms(self,additional_columns=[]):
        columns = ["with_symptoms_reported","symptom_no_symptoms","symptom_cough","symptom_pneumonia"]
        columns += additional_columns
        symptoms = self.GetFilteredCopy(columns)
        for k in columns:
            symptoms[k+"_perday"] = symptoms[k].diff()
        
        return symptoms
    
    def Plot(self):
        
        f,d,c = self.sit_df["ICU_finished"].diff(),self.sit_df["ICU_deaths"].diff(),self.sit_df["ICU_current"].diff()
        entered = c+f

def main():
    situationreports = RKIsituationreports()
    
    icu = situationreports.ICU(additional_columns=["deaths","reported"])
    print(icu)
    
    symptoms = situationreports.Symptoms(additional_columns=["deaths"])
    print(symptoms)

if __name__=="__main__":
	main()