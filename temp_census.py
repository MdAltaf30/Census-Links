# -*- coding: utf-8 -*-
"""
Created on Thu May 11 07:30:52 2023

@author: onais
"""

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


dataset1_2020=open("Data2020.txt","r")
dataset2_2030=open("Data2030.txt","r")


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


# left_lines = np.genfromtxt("2020"+"FileNM.txt", delimiter='|',dtype=str)
# right_lines = np.genfromtxt("2030"+"FileNM.txt", delimiter='|',dtype=str)



# pairs_final=np.array([[0,0]])
# check=0
# for i in tqdm(left_lines):
    
#     left_data=i[0]
#     Cluster_full_left=i[1]
#     Cluster_left=i[1] .split("_")
#     Cluster_id_left=Cluster_left[0]
#     Cluster_dataset_left=Cluster_left[1]
#     for j in right_lines:
#         Check=1
#         right_data=j[0]
#         Cluster_full_right=j[1]
#         Cluster_right=j[1] .split("_")
#         Cluster_id_right=Cluster_right[0]
#         Cluster_dataset_right=Cluster_right[1]
#         if left_data==right_data and not np.in1d([Cluster_full_left,Cluster_full_right], pairs_final).all():
            
#             pairs_final = np.vstack([pairs_final, [Cluster_full_left,Cluster_full_right]])
#             break
    



#Code ----1

    
# left_lines = np.genfromtxt("2020"+"FileNM.txt", delimiter='|', dtype=str)
# right_lines = np.genfromtxt("2030"+"FileNM.txt", delimiter='|', dtype=str)

# # create dictionary to store data from right_lines
# right_dict = {}
# for j in right_lines:
#     right_data = j[0]
#     Cluster_full_right = j[1]
#     Cluster_right = j[1].split("_")
#     Cluster_id_right = Cluster_right[0]
#     Cluster_dataset_right = Cluster_right[1]
#     if right_data not in right_dict:
#         right_dict[right_data] = set()
#     right_dict[right_data].add(Cluster_full_right)

# # iterate through left_lines and compare with data from right_dict
# pairs_final = np.empty((0, 2), dtype=str)
# for i in tqdm(left_lines):
#     left_data = i[0]
#     Cluster_full_left = i[1]
#     Cluster_left = i[1].split("_")
#     Cluster_id_left = Cluster_left[0]
#     Cluster_dataset_left = Cluster_left[1]
#     for j in right_lines:
#         right_data = j[0]
#         Cluster_full_right = j[1]
#         Cluster_right = j[1].split("_")
#         Cluster_id_right = Cluster_right[0]
#         Cluster_dataset_right = Cluster_right[1]
#         if left_data == right_data and not np.any(np.logical_and(pairs_final[:, 0] == Cluster_full_left, pairs_final[:, 1] == Cluster_full_right)):
#             pairs_final = np.vstack([pairs_final, [Cluster_full_left, Cluster_full_right]])
            
# pairs_final = np.unique(pairs_final, axis=0)


# left_lines = np.genfromtxt("2020"+"FileNM.txt", delimiter='|', dtype=str)
# right_lines = np.genfromtxt("2030"+"FileNM.txt", delimiter='|', dtype=str)

# # create dictionary to store data from right_lines
# right_dict = {}
# for j in right_lines:
#     right_data = j[0]
#     Cluster_full_right = j[1]
#     Cluster_right = j[1].split("_")
#     Cluster_id_right = Cluster_right[0]
#     Cluster_dataset_right = Cluster_right[1]
#     if right_data not in right_dict:
#         right_dict[right_data] = set()
#     right_dict[right_data].add(Cluster_full_right)

# # iterate through left_lines and compare with data from right_dict
# pairs_final = np.empty((0, 2), dtype=str)
# for i in tqdm(left_lines):
#     left_data = i[0]
#     Cluster_full_left = i[1]
#     Cluster_left = i[1].split("_")
#     Cluster_id_left = Cluster_left[0]
#     Cluster_dataset_left = Cluster_left[1]
#     if left_data in right_dict:
#         right_data_set = right_dict[left_data]
#         for Cluster_full_right in right_data_set:
#             if not np.any(np.logical_and(pairs_final[:, 0] == Cluster_full_left, pairs_final[:, 1] == Cluster_full_right)):
#                 pairs_final = np.vstack([pairs_final, [Cluster_full_left, Cluster_full_right]])

# pairs_final = np.unique(pairs_final, axis=0)



left_lines = np.genfromtxt("2020FileNM.txt", delimiter='|', dtype=str)
right_lines = np.genfromtxt("2030FileNM.txt", delimiter='|', dtype=str)

# create dictionary to store data from right_lines
right_dict = {}
for j in right_lines:
    right_data = j[0]
    Cluster_full_right = j[1]
    Cluster_right = Cluster_full_right.split("_")
    Cluster_id_right = Cluster_right[0]
    Cluster_dataset_right = Cluster_right[1]
    if right_data not in right_dict:
        right_dict[right_data] = set()
    right_dict[right_data].add(Cluster_full_right)

# iterate through left_lines and compare with data from right_dict
pairs_final = set()
for i in tqdm(left_lines):
    left_data = i[0]
    Cluster_full_left = i[1]
    Cluster_left = Cluster_full_left.split("_")
    Cluster_id_left = Cluster_left[0]
    Cluster_dataset_left = Cluster_left[1]
    if left_data in right_dict:
        right_data_clusters = right_dict[left_data]
        for Cluster_full_right in right_data_clusters:
            if Cluster_full_left != Cluster_full_right:
                pairs_final.add((Cluster_full_left, Cluster_full_right))

pairs_final = np.array(list(pairs_final))





print(len(pairs_final))



left_lines,right_lines="",""
pairs_final = np.delete(pairs_final,0, axis=0)
edge_color=[]
g=Network(width="100%",notebook=False,directed=True)
NodesCluster=[]
for p in tqdm(pairs_final):
     print(p)
     p=list(p)
     g.add_node(p[0],color='blue',title=p[0],label=p[0],shape="image", image ="https://github.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/blob/main/Household%20Graph/Image.png?raw=true",size=30)
     g.add_node(p[1],color='blue',title=p[1],label=p[1],shape="image", image ="https://raw.githubusercontent.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/main/Household%20Graph/house2.png",size=30)   
     g.add_edge(p[0],p[1],color='black',width=2,arrowStrikethrough=False)

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
                                threshold=55,
                               
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
                
                if len(df_output[df_output['match_score']>50])>1:
                    
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
                    














# for p in tqdm(pairs_final):
        
#     left_node_child=Clusters_with_ID_left[p[0]]
#     right_node_chid=Clusters_with_ID_right[p[1]]
    
#     left_list=[]
#     right_list=[]
    
#     left_name_df=pd.DataFrame(columns=['id','Name'])
#     right_name_df=pd.DataFrame(columns=['id','Name'])
    
#     if (p[0] not in visited) and ( p[1] not in visited):
            
            
        
#         for i in left_node_child:
#             left_list.append(i)
                
#         left_name_df = left_name[left_name[0].isin(left_list)]
            
#         for i in right_node_chid:
#             right_list.append(i)
        
#         right_name_df = right_name[right_name[0].isin(right_list)]
       
#         if len(left_name_df)>1 and len(right_name_df)>1:
#             match=ts.fuzzy_match(
#                             left_name_df,
#                             right_name_df,
#                             1,
#                             1,
#                             threshold=80,
                           
#                         )
#             df_output = left_name_df.merge(
#                             match,
#                             how='left',
#                             left_index=True,
#                             right_on='df_left_id'
#                         ).merge(
#                             right_name_df,
#                             how='left',
#                             left_on='df_right_id',
#                             right_index=True,
#                             suffixes=['_df1', '_df2']
#                         )
#             df_output=df_output.dropna()
#             df_output=df_output.drop_duplicates(subset="0_df2")
            
#             if len(df_output[df_output['match_score']>60])>0:
                
#                 print(df_output)
                
#                 match=df_output[df_output['match_score']>75]
#                 for index, row in left_name_df.iterrows(): 
#                     g.add_node(row[0],label=row[1],shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white")
#                     g.add_edge(row[0],p[0],color='#00ff00',width=3)
                
#                 for index, row in right_name_df.iterrows(): 
#                     g.add_node(row[0],label=row[1],shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white")
#                     g.add_edge(row[0],p[1],color='#00ff00',width=3)
                    
                
#                 for index, row in match.iterrows():
#                     g.add_edge(row["0_df1"],row['0_df2'],color='blue',width=2)
                
#                 visited.add(p[0])
#                 visited.add(p[1])
                

            
# print(visited)

            
            
            
            
        
        
   
    
 # right_list.append(right_name_df.iloc[0, 1])
    
    
    
        

    
    
    
     




# search for the value in the 'column2' column of the DataFrame



# left_name_df = left_name[left_name[0].str.contains("")]









g.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=100, spring_strength=0.08, damping=0.4, overlap=0)
g.show_buttons(filter_=['physics'])
g.show('Graph_Final.html',local=False)


















            
        
# Record_Nodes_Link=[]

# Record_Link_Cluster=[]





# combining_centers=open("FileNM.txt","r")
# LinesRead=combining_centers.readlines()
# Final_Cluster=[]
# Final_Cr=np.array([0,0])
# file_n_w = open("FileNM.txt", "w")
# for j in LinesRead:
#     SplitW=j.split("|")
#     for p in LinesRead[1:len(LinesRead)-1]:
#         SplitC=p.split("|")
#         if SplitW[0]==SplitC[0] and SplitW[1].strip()!=SplitC[1].strip():
#             Final_Cluster.append((SplitW[1].strip(),SplitC[1].strip()))
#             new=[SplitW[1].strip(),SplitC[1].strip()]
#             Final_Cr = np.vstack([Final_Cr,new])
#             break
# Final_Cr = np.delete(Final_Cr,(0), axis=0)     
# Final_C=np.array(Final_Cr)
# outt = np.vstack(list(set(map(tuple,map(sorted,Final_C)))))
# print("Pairs Completed")        

# Cluster_to_Cluster=[]

# edge_color=[]
# g=Network(width="100%",notebook=False)
# NodesCluster=set()
# print("Cluster-1.1")
# with open("FileNM.txt","w") as O:
#     for k in tqdm(outt):
#         O.writelines(k[0]+","+k[1])
#         O.writelines("\n")
#         edge_color.append(10)
#         g.add_node(k[0],color='blue',title=k[0],label=k[0],shape="image", image ="https://github.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/blob/main/Household%20Graph/Image.png?raw=true",size=30)
#         g.add_node(k[1],color='blue',title=k[1],label=k[1],shape="image", image ="https://github.com/Univ-Of-Arkansas-at-LITTLE-ROCK/CensusBureauNameAddress/blob/main/Household%20Graph/Image.png?raw=true",size=30)
#         Nodes_Final.append([k[0],k[1]])
      
#         g.add_edge(k[0],k[1],color='black',width=2,arrowStrikethrough=False)
#         NodesCluster.add(k[0])
#         NodesCluster.add(k[1])
#         Cluster_to_Cluster.append([k[0],k[1]])
# # open_Cluster.close()
# Map_File=open("SOG Clean Occupancy Data1.txt","r")
# Map=Map_File.readlines()
# node_color=[]
# Cluster_to_Nodes=[]
# for k in Map:
#     SplitX=k.split("|")
#     #(SplitX[2])
#     Record_Nodes_Link.append([SplitX[0].strip(),SplitX[1].strip()])
    
#     g.add_node(SplitX[0].strip(),shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white", title=SplitX[1].strip(),label=SplitX[1].strip().split(",")[0])
#     try:
#         g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='#00ff00',width=3)
#         Record_Link_Cluster.append([SplitX[2].strip(),SplitX[0].strip()])
#     except:
#         g.add_node(SplitX[2].strip(),labelHighlightBold =True,color='blue',title=SplitX[2].strip(),label=SplitX[2].strip())
#         g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='#00ff00',width=3)
#         Record_Link_Cluster.append([SplitX[2].strip(),SplitX[0].strip()])

#     edge_color.append(10)
#     node_color.append("#4CAF50")
#     Cluster_to_Nodes.append([SplitX[0].strip(),SplitX[2].strip(),SplitX[1].strip()])
# Map_File.close()
#   # C1,C2
#   # C2,C3
#   # C4,C5
#   # C1,C3
# # Build your graph
# Cluster_Dictionary={}
# for m in tqdm(NodesCluster):
#     for j in Cluster_to_Nodes:
#         if j[1]==m:
#             try:
#                 if Cluster_Dictionary[m]:
#                     temp=Cluster_Dictionary[m]
#                     Cluster_Dictionary[m].append(j[0])
#                     Cluster_Dictionary[m]=temp
#             except:
#                 Cluster_Dictionary[m]=[j[0]] 

# Pattern_Clusters={}
# list_of_key=[]
# for key,value in Cluster_Dictionary.items():
#     NextNode=''
#     Array_of_Path=[]
    
#     for j in Cluster_to_Cluster:
#         temp=set()
#         temp.add(key)
#         t=set()
#         t.add(key)
#         t.add(j[0])
#         t1=set()
#         t1.add(key)
#         t1.add(j[1])
#         if (key in j) and (t not in list_of_key) and (t1 not in list_of_key):
#             if j[0]!=key:
#                 NextNode=j[0]
#                 temp.add(j[0])
#                 Array_of_Path.append(NextNode)
#                 list_of_key.append(temp)
#                 temp=set()
#                 continue
            
                
#         Pattern_Clusters[key]=Array_of_Path
# Visited=[]    

# LinkedNodes=[]
# print(Pattern_Clusters)
# Map_File=open("SOG Clean Occupancy Data1.txt","r")

# for key, val in Pattern_Clusters.items():
#     lst=[]
#     i=0
#     Left_Dict={}
#     for o in Cluster_Dictionary[key]:
#         lst.append([str(i)+"_left",o,ID_Name[o]])
#         Left_Dict[str(i)+"_left"]=o
#         i+=1
#     df_left=pd.DataFrame(lst,columns=["ID","col_b","col_a"])
#     for l in val:
#         if (l or key) not in Visited:
#             i=0
#             lst2=[]
#             Right_Dict={}
#             for ii in Cluster_Dictionary[l]:
#                 lst2.append([str(i)+"_right",ii,ID_Name[ii]])
#                 Right_Dict[str(i)+"_right"]=ii
#                 i+=1
#             df_right=pd.DataFrame(lst2, columns=["ID","col_b","col_a"])
#             df_matches = ts.fuzzy_match(
#                 df_left,
#                 df_right,
#                 'col_a',
#                 'col_a',
#                 threshold=55,
               
#             )
           
#             df_output = df_left.merge(
#                 df_matches,
#                 how='left',
#                 left_index=True,
#                 right_on='df_left_id'
#             ).merge(
#                 df_right,
#                 how='left',
#                 left_on='df_right_id',
#                 right_index=True,
#                 suffixes=['_df1', '_df2']
#             )
            
#             df_output.set_index('df_left_id', inplace=True)       # For some reason the first merge operation wrecks the dataframe's index. Recreated from the value we have in the matches lookup table
#             df_output = df_output[['col_a_df1', 'col_b_df1', 'col_b_df2','match_score']]      # Drop columns used in the matching
#             df_output.index.name = 'id'
#             df_output=df_output.dropna()
#             df_output=df_output.drop_duplicates(subset="col_b_df2")
#             if len(df_output)>1:
#                 Visited.append(l)
#                 Visited.append(key)
#                 check_for=0
#                 first=""
#                 for index, row in df_output.iterrows():
#                         first=row['col_b_df2']
#                         out.write(row['col_b_df1']+"\t\t"+first)
#                         out.write("\n")
#                         FinalLink[first]=first
#                         FinalLink[row['col_b_df1']]=first
#                         width=(row['match_score']/100)*10
#                         if width>=8:
                            
#                             g.add_edge(row['col_b_df1'],first,color='blue',width=2)
#                             Record_Record.append([row['col_b_df1'],first])
#                             LinkedNodes.append(row['col_b_df1'])
#                             LinkedNodes.append(row['col_b_df2'])
#                         if width <8:
#                             Record_Record.append([row['col_b_df1'],first])
#                             g.add_edge(row['col_b_df1'],first,color='red',width=2)
#                             LinkedNodes.append(row['col_b_df1'])
#                             LinkedNodes.append(row['col_b_df2'])

# g.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=100, spring_strength=0.08, damping=0.4, overlap=0)
# g.show_buttons(filter_=['physics'])
# g.show('Graph_Final.html',local=False)




















      
   