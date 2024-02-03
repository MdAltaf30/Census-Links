# -*- coding: utf-8 -*-
"""
Created on Sun May  7 13:27:07 2023

@author: onais
"""

import DWM00_Driver as DWM
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network
import re
from functools import reduce # Python3
import ER_Metrics_22 as ER
import functools
import ER_Metrics__indiv as ERR
import numpy as np
import TEST_FUZZY_MATCH_MERGE as ts
import jellyfish as sd
import numpy_indexed as npi
from tqdm import tqdm
import ER_Metrics_2
from fuzzywuzzy import fuzz
#DWM.DWM_Cluster("S2-parms.txt")
from sklearn.metrics import PrecisionRecallDisplay
#Parsing 1st program
Address_4CAF50=open("SOG Clean Occupancy Data.txt","r")







def Blocking_Clustering(file,file_name, parms,Clusters_With_ID):
    Lines = file.readlines()
    DF=[]
    ii=0
    count = 0
    FinalList=[]
    FinalLink={}
    Nodes_Final=[]
    Nodes_Relationship=[]
    Record_Nodes_Link=[]
    Record_Link_Cluster=[]
    Record_Record=[]
    fileHandle = open('USAddressWordTable.txt', 'r')
    NamefileHandle = open('NamesWordTableOpt.txt', 'r')
    SplitWordTable = open('SplitWordTable.txt', 'r')
    Cluster_Link_ID={}
    
    def unique(a):
        order = np.lexsort(a.T)
        a = a[order]
        diff = np.diff(a, axis=0)
        ui = np.ones(len(a), 'bool')
        ui[1:] = (diff != 0).any(axis=1) 
        return a[ui]
    with open("Output_File.txt","w") as out:
        out.write("------------------ Output -----------------------")
        out.write("\n")
           # perform file operations
        # Strips the newline character
        Count=len(Lines)
        DF=pd.DataFrame()
        C=1
        CC=1
        JsonData={}
        AllAddress_Key_Value_As_MASK_Comp={}
        Observation=0
        Total=0
        dataFinal={}
        NameListFinal=[]
        AddressListFinal=[]
        for line in tqdm(Lines):
            line=line.strip("\n").split("|")
            ID=line[0]
            line=line[1] .strip() 
            Old_Address=line.strip()
            USAD_Conversion_Dict={"1":"USAD_SNO","2":"USAD_SPR","3":"USAD_SNM","4":"USAD_SFX","5":"USAD_SPT","6":"USAD_ANM","7":"USAD_ANO","8":"USAD_CTY","9":"USAD_STA","10":"USAD_ZIP","11":"USAD_ZP4","12":"USAD_BNM","13":"USAD_BNO","14":"USAD_RNM"}
            
            USAD_Conversion_Dict_Detail={"1":"USAD_SNO Street Number","2":"USAD_SPR Street Pre-directional","3":"USAD_SNM Street Name","4":"USAD_SFX Street Suffix","5":"USAD_SPT Street Post-directional","6":"USAD_ANM Secondary Address Name","7":"USAD_ANO Secondary Address Number","8":"USAD_CTY City Name","9":"USAD_STA State Name","10":"USAD_ZIP Zip Code","11":"USAD_ZP4 Zip 4 Code","12":"USAD_BNM Box Name","13":"USAD_BNO Box Number","14":"USAD_RNM Route Name"}
        
            
            List=USAD_Conversion_Dict.keys()
            FirstPhaseList=[]
            Address=re.sub(',',' ',line)
            Address=re.sub(' +', ' ',Address)
            Address=re.sub('[.]','',Address)
            #Address=re.sub('#','',Address)    
            Address=Address.upper()
            AddressList = re.split("\s|\s,\s ", Address)
            tmp1=0
            NameList=[]
            RevisedAddressList=[]
            SplitMask=""
            for A in AddressList:
                FirstPhaseDict={}
                NResult=False 
                try:
                    Compare=A[0].isdigit()
                except:
                    a=0
                if A==",":
                    SplitMask+=","
                elif Compare:
                    SplitMask+="A"
                else:
                    NR=True
                    for line in SplitWordTable:
                    
                        fields=line.split('|')
                        if A==(fields[0]):
                            SplitMask+=fields[1].strip()
                            NR=False
                            break
                    if NR:
                        SplitMask+="W"
                SplitWordTable.seek(0)
            Name=""
            indexSplit=0
            for m in range(len(SplitMask)):
                if SplitMask[m] in ("W","P",",") :
                    continue
                else:
                    indexSplit=m
                    break
        
            RevisedAddressList = AddressList[indexSplit:len(AddressList)]
         
            NameList = AddressList[0:indexSplit]
            try:
                if NameList[len(NameList)-1]==",":
                    NameList.pop(len(NameList)-1)
            except:
                continue
           
            NameListFinal.append([ID,' '.join(NameList)])
            AddressListFinal.append([ID,' '.join(RevisedAddressList)])        
        file_n = open(file_name+"FileN.txt", "w")
        out.write("Step- 1 Address Parser Output")
        out.write("Name Splitting")
        out.write("\n")
        ID_Name={}
        Dict_of_Name={}
        for element in NameListFinal:
            print(element)
            Dict_of_Name[element[0].strip()]=element[1]
            file_n.write(element[0]+"|"+element[1])
            file_n.write("\n")
            ID_Name[element[0]]=sd.soundex(element[1])
            out.write(element[0]+"|"+element[1])
            out.write("\n")
        file_n.close()
        
        file_a = open(file_name+"FileA.txt", "w")
        out.write("Address Splitting")
        out.write("\n")
        for element in AddressListFinal:
            file_a.write(element[0]+"|"+element[1])
            file_a.write("\n")
            out.write(element[0]+"|"+element[1])
            out.write("\n")
        file_a.close()
        
        DWM.DWM_Cluster(parms)
        
        file_Address=open(file_name+"FileA-LinkIndex.txt","r")
        Address_Cluster = file_Address.readlines()
        file_a_r = file
        
        file_a_r=file_a_r.readlines()
        file_n_r = open(file_name+"FileN.txt", "r")
        LinesRead=file_n_r.readlines()
        Clusters=set()
        for i in Address_Cluster:
            find_Address=i.split(",")
            Clusters_With_ID.append([find_Address[0].strip(),find_Address[1].strip()])
            if find_Address[1].strip()!="ClusterID":
                Clusters.add(find_Address[1].strip())
        
        del Clusters_With_ID [0]
        Clusters_Dict={}
        i=1
        Clusters=list(Clusters)
        Clusters.sort(reverse=False)
        for j in Clusters:
            Clusters_Dict[j]="C"+str(i)
            i+=1
        t=1
        file_a_w = open(file_name+"SOG Clean Occupancy Data1.txt", "w")
        
        i=0
        for k in Clusters_With_ID:
            Clusters_With_ID[i][1]=Clusters_Dict[Clusters_With_ID[i][1]]+"_"+file_name
           
            i+=1
        out.write("Clusters Formation using Data Washing Machine")
        out.write("\n")
        for k in tqdm(file_a_r):
            splitData=k.split("|")
            n=0
            for l in Clusters_With_ID:
                if splitData[0].strip()==Clusters_With_ID[n][0]:
                    
                    file_a_w.write(k.strip()+"|"+Clusters_With_ID[n][1])
                    out.write(k.strip()+"|"+Clusters_With_ID[n][1])
                    out.write("\n")
                    file_a_w.write("\n")
                    break
                n+=1
        file_n_r.close()  
        file_a_w.close() 
        with open(file_name+"FileClusters.csv","w") as wr:
            wr.write("ID,Cluster"+"\n")
           
            for i in Clusters_With_ID:
                wr.write(str(i[0]+","+i[1]+"\n"))
        
        print(Clusters_With_ID)
        
        out.write("Appending the same clusters to the Names File")
        out.write("\n\n")
        out.write("\n")
        Clusters_With_ID=np.array(Clusters_With_ID)
        
       
        
        
        # for k,v in tqdm(Dict_of_Name.items()):
        #     n=0
        #     if k==Clusters_With_ID[n][0]:
        #         print(k.strip()+"|"+Clusters_With_ID[n][1])
        #         file_n_w.write(k.strip()+"|"+Clusters_With_ID[n][1])
        #         file_n_w.write("\n")
                
        #     n+=1
        
        with open(file_name+"FileNM.txt", "w") as file_n_w:
            for cl in tqdm(Clusters_With_ID):
                if cl[0] in list(Dict_of_Name.keys()):
                    splitData=Dict_of_Name[cl[0]].split(" ")
                    for o in splitData:
                        file_n_w.write(o+"|"+cl[1])
                        out.write(o+"|"+cl[1])
                        out.write("\n")
                        file_n_w.write("\n")
        file_n_r = open(file_name+"FileNM.txt", "r")
    
        print("Completed Soundex")
        LinesRead=file_n_r.readlines()
        return Clusters_With_ID 
        
        

dataset1_2020=open("Data2020.txt","r")
dataset2_2030=open("Data2030.txt","r")


# dataset1_2020=open("Data2020.txt","r")
# dataset2_2030=open("Data2030.txt","r")


Nodes_Final=[]


Cluster_to_Nodes=[]
Cluster_to_Name_left={}
Cluster_to_Name_right={}

Clusters_with_ID_left=[]
Clusters_with_ID_right=[]

Clusters_with_ID_left=np.array(Blocking_Clustering(dataset1_2020,"2020","File_A_Parms.txt",Clusters_with_ID_left))
Clusters_with_ID_right=np.array(Blocking_Clustering(dataset2_2030,"2030","File_B_Parms.txt",Clusters_with_ID_right))

keys_Clusters_with_ID_left = np.unique(Clusters_with_ID_left[:, 1])
keys_Clusters_with_ID_right = np.unique(Clusters_with_ID_right[:, 1])
# Group the values by key using a dictionary comprehension and numpy's boolean indexing
Clusters_with_ID_left = {key: list(Clusters_with_ID_left[Clusters_with_ID_left[:, 1] == key][:, 0]) for key in keys_Clusters_with_ID_left}
Clusters_with_ID_right = {key: list(Clusters_with_ID_right[Clusters_with_ID_right[:, 1] == key][:, 0]) for key in keys_Clusters_with_ID_right}





















































