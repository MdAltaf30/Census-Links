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



import sqlite3


connection = sqlite3.connect('CensusLinking.db')
cursor = connection.cursor()

# Step 4: Execute the CREATE TABLE statement
create_table_query = '''
    CREATE TABLE IF NOT EXISTS Name_Address_File (
        ID TEXT PRIMARY KEY,
        NAME TEXT,
        ADDRESS TEXT
    )
cursor.execute(create_table_query)
'''

# Commit the changes and close the connection
connection.commit()
connection.close()


dasd
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
        
        

dataset1_2020=open("1940Data.txt","r")
dataset2_2030=open("1950Data.txt","r")


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




left_dataset=open("2020"+"FileNM.txt")
right_dataset=open("2030"+"FileNM.txt")

left_lines=left_dataset.readlines()
right_lines=right_dataset.readlines()

# code ----0


left_lines = np.genfromtxt("2020"+"FileNM.txt", delimiter='|',dtype=str)
right_lines = np.genfromtxt("2030"+"FileNM.txt", delimiter='|',dtype=str)



pairs_final=np.array([[0,0]])
check=0
for i in tqdm(left_lines):
    
    left_data=i[0]
    Cluster_full_left=i[1]
    Cluster_left=i[1] .split("_")
    Cluster_id_left=Cluster_left[0]
    Cluster_dataset_left=Cluster_left[1]
    for j in right_lines:
        Check=1
        right_data=j[0]
        Cluster_full_right=j[1]
        Cluster_right=j[1] .split("_")
        Cluster_id_right=Cluster_right[0]
        Cluster_dataset_right=Cluster_right[1]
        if left_data==right_data and not np.in1d([Cluster_full_left,Cluster_full_right], pairs_final).all():
            
            pairs_final = np.vstack([pairs_final, [Cluster_full_left,Cluster_full_right]])
            break
    





left_lines,right_lines="",""
pairs_final = np.delete(pairs_final,0, axis=0)
edge_color=[]
g=Network(width="100%",notebook=False,directed=True)
NodesCluster=[]
for p in tqdm(pairs_final):
     p=list(p)
     g.add_node(p[0],color='blue',title=p[0],label=p[0],shape="image", image ="https://github.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/blob/main/Household%20Graph/Image.png?raw=true",size=30)
     g.add_node(p[1],color='blue',title=p[1],label=p[1],shape="image", image ="https://raw.githubusercontent.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/main/Household%20Graph/house2.png",size=30)   
     g.add_edge(p[0],p[1],color='black',width=2,arrowStrikethrough=False)

pairs_final=np.array(pairs_final)

left_name = pd.read_csv("2020"+"FileN.txt", delimiter='|',header=None)
right_name = pd.read_csv("2030"+"FileN.txt", delimiter='|',header=None)

left_name[0] = left_name[0].astype(str)
left_name[0] = left_name[0].apply(lambda x: x.strip())

right_name[0] = right_name[0].astype(str)
right_name[0] = right_name[0].apply(lambda x: x.strip())


visited=set()




graph = {}
for edge in pairs_final:
    if edge[0] not in graph:
        graph[edge[0]] = []
    if edge[1] not in graph:
        graph[edge[1]] = []
    graph[edge[0]].append(edge[1])
    graph[edge[1]].append(edge[0])




combine = {**Clusters_with_ID_right, **Clusters_with_ID_left}
for key,value in tqdm(graph.items()):
    left_node_child=combine[key]
    
    left_list=[]
    left_name_df=pd.DataFrame(columns=['id','Name'])
    for i in left_node_child:
        left_list.append(i)
    left_name_df = left_name[left_name[0].isin(left_list)]
    
    for m in value:
        
        right_node_chid=combine[m]
        right_list=[]
        right_name_df=pd.DataFrame(columns=['id','Name'])
    
        if (m not in visited) and (key not in visited):
            for i in right_node_chid:
                right_list.append(i)
            
            right_name_df = right_name[right_name[0].isin(right_list)]
           
            if len(left_name_df)>1 and len(right_name_df)>1:
                match=ts.fuzzy_match(
                                left_name_df,
                                right_name_df,
                                1,
                                1,
                                threshold=80,
                               
                            )
                df_output = left_name_df.merge(
                                match,
                                how='left',
                                left_index=True,
                                right_on='df_left_id'
                            ).merge(
                                right_name_df,
                                how='left',
                                left_on='df_right_id',
                                right_index=True,
                                suffixes=['_df1', '_df2']
                            )
                df_output=df_output.dropna()
                df_output=df_output.drop_duplicates(subset="0_df2")
                
                if len(df_output[df_output['match_score']>70])>1:
                    
                    match=df_output[df_output['match_score']>75]
                    for index, row in left_name_df.iterrows(): 
                        g.add_node(row[0],label=row[1],shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white")
                        g.add_edge(row[0],m,color='#00ff00',width=3)
                    
                    for index, row in right_name_df.iterrows(): 
                        g.add_node(row[0],label=row[1],shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white")
                        g.add_edge(row[0],key,color='#00ff00',width=3)
                        
                    
                    for index, row in match.iterrows():
                        g.add_edge(row["0_df1"],row['0_df2'],color='blue',width=2)
                    
                    visited.add(key)
                    visited.add(m)
                    break
                    


    
    
    
        

    
    
    
     




# search for the value in the 'column2' column of the DataFrame



# left_name_df = left_name[left_name[0].str.contains("")]









g.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=100, spring_strength=0.08, damping=0.4, overlap=0)
g.show_buttons(filter_=['physics'])
g.show('Graph_Final.html',local=False)