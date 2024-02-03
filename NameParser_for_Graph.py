# -*- coding: utf-8 -*-
"""
Created on Sun Aug 14 01:19:13 2022

@author: onais
"""

import re
from tqdm import tqdm
import pandas as pd
import json 
#Parsing 1st program
ii=0
count = 0
FinalList=[]
fileHandle = open('NamesWordTableOpt.txt', 'r')
# Strips the newline character
C=1
CC=1
JsonData={}
AllName_Key_Value_As_MASK_Comp={}
Observation=0
Total=0
dataFinal={}
def ExtractNames(line):

        Names_Conversion_Dict={"1":"Prefix Title","2":"Given Name", "3":"Surname","4" :"Generational Suffix", "5":"Suffix Title"}    
        List=Names_Conversion_Dict.keys()
        
        FirstPhaseList=[]
        Name=re.sub(',',' , ',line)
        Name=re.sub(' +', ' ',Name)
        Name=re.sub(' +', ' ',Name)
        Name=re.sub('[.]','',Name)
        #Name=re.sub('#','',Name)    
        Name=Name.upper()
        NameList = re.split("\s|\s,\s ", Name)
        NameList = ' '.join(NameList).split()
        #del(NameList[len(NameList)-1])
        TrackKey=[]
        Mask=[]
        Combine=""
        LoopCheck=1
        for A in NameList:
            FirstPhaseDict={}
            NResult=False
            if A==",":
                O=0
                Mask.append(Combine)
                Combine=""
                FirstPhaseList.append(",")
                #FirstPhaseList.append("Seperator")
            elif A==" ":
                continue
            elif A!="," and len(A)==1:
                Combine+="I"
                TrackKey.append("I")
                FirstPhaseDict["I"] = A
                FirstPhaseList.append(FirstPhaseDict)
            else:
                for line in fileHandle:
                    fields=line.split('|')
                    if A==(fields[0]):
                        NResult=True
                        temp=fields[1]
                        Combine+=temp[0]
                        FirstPhaseDict[temp[0]] = A
                        FirstPhaseList.append(FirstPhaseDict)
                        TrackKey.append(temp[0])
                        break
                if NResult==False:
                    Combine+="W"
                    TrackKey.append("W")
                    FirstPhaseDict["W"] = A
                    FirstPhaseList.append(FirstPhaseDict)
            if LoopCheck==len(NameList):
                Mask.append(Combine)
            fileHandle.seek(0)
            LoopCheck+=1
        Mask_1=",".join(Mask)
        Names_Conversion_Mapping={"Prefix Title":[],"Given Name":[], "Surname":[],"Generational Suffix":[],"Suffix Title":[]}
        Start=0
        Counts=0
        FirstPhase_WithComma=FirstPhaseList
        FirstPhaseList = [FirstPhaseList[b] for b in range(len(FirstPhaseList)) if FirstPhaseList[b] != ","]
        data={}
        with open('JSONMappingNameDefault.json', 'r+', encoding='utf-8') as f:
            data = json.load(f)
        Found=False
        FoundDict={}
        for tk,tv in data.items():
            if(tk==Mask_1):
                FoundDict[tk]=tv
                Found=True
                break
        FoundExcept=False
        with open('NameExceptionFile.json', 'r+', encoding='utf-8') as g:
            Stat = json.load(g)
            if Mask_1 in Stat.keys():
                FoundExcept=True
        Mappings={}
        if Found:
            
            for K2,V2 in FoundDict[Mask_1].items():
                Temp=""
                for p in V2:
                    for K3,V3 in FirstPhaseList[p-1].items():
                       Temp+=" "+V3
                       Temp=Temp.strip()
                       Mappings[K2]=Temp
            try:
                Mappings=Mappings["Surname"]+" "+Mappings["Given Name"]
                return Mappings
            except:
                return Mappings
        else:
            NameList =' '.join(NameList)
            return Name
        
