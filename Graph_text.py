# -*- coding: utf-8 -*-
"""
Created on Wed Oct  5 17:14:34 2022

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
Lines = Address_4CAF50.readlines()
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
    file_n = open("FileN.txt", "w")
    out.write("Step- 1 Address Parser Output")
    out.write("Name Splitting")
    out.write("\n")
    ID_Name={}
    Dict_of_Name={}
    for element in NameListFinal:
        Dict_of_Name[element[0]]=element[1]
        file_n.write(element[0]+"|"+element[1])
        file_n.write("\n")
        ID_Name[element[0]]=sd.soundex(element[1])
        out.write(element[0]+"|"+element[1])
        out.write("\n")
    file_n.close()
    
    file_a = open("FileA.txt", "w")
    out.write("Address Splitting")
    out.write("\n")
    for element in AddressListFinal:
        file_a.write(element[0]+"|"+element[1])
        file_a.write("\n")
        out.write(element[0]+"|"+element[1])
        out.write("\n")
    file_a.close()
    DWM.DWM_Cluster("File_A_Parms.txt")
    
    file_Address=open("FileA-LinkIndex.txt","r")
    Address_Cluster = file_Address.readlines()
    file_a_r = open("SOG Clean Occupancy Data.txt", "r")
    
    file_a_r=file_a_r.readlines()
    file_n_r = open("FileN.txt", "r")
    LinesRead=file_n_r.readlines()
    Clusters_With_ID=[]
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
    file_a_w = open("SOG Clean Occupancy Data1.txt", "w")
    i=0
    for k in Clusters_With_ID:
        Clusters_With_ID[i][1]=Clusters_Dict[Clusters_With_ID[i][1]]
        i+=1
    out.write("Clusters Formation using Data Washing Machine")
    out.write("\n")
    for k in tqdm(file_a_r):
        splitData=k.split("|")
        n=0
        for l in Clusters_With_ID:
            if splitData[0]==Clusters_With_ID[n][0]:
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
    with open("FileNM.txt", "w") as file_n_w:
        for cl in tqdm(Clusters_With_ID):
            if cl[0] in Dict_of_Name.keys():
                splitData=Dict_of_Name[cl[0]].split(" ")
                
                if len(splitData) >= 2:
                    t1=splitData[0]
                    t2=splitData[1]
                    splitData.clear()
                    splitData.append(t1)
                    splitData.append(t2)
                    print(splitData[0][0:3]+splitData[1][0:4]+"|"+cl[1])
                    file_n_w.write(splitData[0][0:3]+splitData[1][0:4]+"|"+cl[1])
                    out.write(splitData[0][0:3]+splitData[1][0:4]+"|"+cl[1])
                    out.write("\n")
                    file_n_w.write("\n")
                else:
                    file_n_w.write(splitData[0][0:5]+"|"+cl[1])
                    out.write(splitData[0][0:3]+splitData[1][0:4]+"|"+cl[1])
                    out.write("\n")
                    file_n_w.write("\n")
                
    file_n_r = open("FileNM.txt", "r")

    print("Completed Soundex")
    LinesRead=file_n_r.readlines()
    
    
    #END OF SOUNDEX
      
    combining_centers=open("FileNM.txt","r")
    LinesRead=combining_centers.readlines()
    Final_Cluster=[]
    Final_Cr=np.array([0,0])
    file_n_w = open("FileNM.txt", "w")
    for j in LinesRead:
        SplitW=j.split("|")
        for p in LinesRead[1:len(LinesRead)-1]:
            SplitC=p.split("|")
            if SplitW[0]==SplitC[0] and SplitW[1].strip()!=SplitC[1].strip():
                Final_Cluster.append((SplitW[1].strip(),SplitC[1].strip()))
                new=[SplitW[1].strip(),SplitC[1].strip()]
                Final_Cr = np.vstack([Final_Cr,new])
                break
    Final_Cr = np.delete(Final_Cr,(0), axis=0)     
    Final_C=np.array(Final_Cr)
    outt = np.vstack(list(set(map(tuple,map(sorted,Final_C)))))
    print("Pairs Completed")        

    Cluster_to_Cluster=[]
    out.write("Final Cluster Map.........")
    out.write("\n")
    edge_color=[]
    g=Network(width="100%",notebook=False)
    NodesCluster=set()
    print("Cluster-1.1")
    with open("FileNM.txt","w") as O:
        for k in tqdm(outt):
            O.writelines(k[0]+","+k[1])
            O.writelines("\n")
            edge_color.append(10)
            g.add_node(k[0],color='blue',title=k[0],label=k[0],shape="image", image ="https://raw.githubusercontent.com/OnaisKhanMohammed/CensusBureauDataEntry/main/Image.png",size=30)
            g.add_node(k[1],color='blue',title=k[1],label=k[1],shape="image", image ="https://raw.githubusercontent.com/OnaisKhanMohammed/CensusBureauDataEntry/main/Image.png",size=30)
            Nodes_Final.append([k[0],k[1]])
            out.write(k[0]+","+k[1])
            out.write("\n")
            g.add_edge(k[0],k[1],color='black',width=2,arrowStrikethrough=False)
            NodesCluster.add(k[0])
            NodesCluster.add(k[1])
            Cluster_to_Cluster.append([k[0],k[1]])
    # open_Cluster.close()
    Map_File=open("SOG Clean Occupancy Data1.txt","r")
    Map=Map_File.readlines()
    node_color=[]
    Cluster_to_Nodes=[]
    for k in Map:
        SplitX=k.split("|")
        #(SplitX[2])
        Record_Nodes_Link.append([SplitX[0].strip(),SplitX[1].strip()])
        
        g.add_node(SplitX[0].strip(),shape="image",labelHighlightBold =True, image ="https://github.com/OnaisKhanMohammed/CensusBureauDataEntry/blob/main/person.png?raw=true",size=15,text_color="white", title=SplitX[1].strip(),label=SplitX[1].strip().split(",")[0])
        try:
            g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='#00ff00',width=3)
            Record_Link_Cluster.append([SplitX[2].strip(),SplitX[0].strip()])
        except:
            g.add_node(SplitX[2].strip(),labelHighlightBold =True,color='blue',title=SplitX[2].strip(),label=SplitX[2].strip())
            g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='#00ff00',width=3)
            Record_Link_Cluster.append([SplitX[2].strip(),SplitX[0].strip()])

        edge_color.append(10)
        node_color.append("#4CAF50")
        Cluster_to_Nodes.append([SplitX[0].strip(),SplitX[2].strip(),SplitX[1].strip()])
    Map_File.close()
     # C1,C2
     # C2,C3
     # C4,C5
     # C1,C3
    # Build your graph
    
    g.show('Graph_Final.html',local=False) 
    Cluster_Dictionary={}
    for m in tqdm(NodesCluster):
        for j in Cluster_to_Nodes:
            if j[1]==m:
                try:
                    if Cluster_Dictionary[m]:
                        temp=Cluster_Dictionary[m]
                        Cluster_Dictionary[m].append(j[0])
                        Cluster_Dictionary[m]=temp
                except:
                    Cluster_Dictionary[m]=[j[0]] 
    
    Pattern_Clusters={}
    list_of_key=[]
    for key,value in Cluster_Dictionary.items():
        NextNode=''
        Array_of_Path=[]
        
        for j in Cluster_to_Cluster:
            temp=set()
            temp.add(key)
            t=set()
            t.add(key)
            t.add(j[0])
            t1=set()
            t1.add(key)
            t1.add(j[1])
            if (key in j) and (t not in list_of_key) and (t1 not in list_of_key):
                if j[0]!=key:
                    NextNode=j[0]
                    temp.add(j[0])
                    Array_of_Path.append(NextNode)
                    list_of_key.append(temp)
                    temp=set()
                    continue
                
                    
            Pattern_Clusters[key]=Array_of_Path
    Visited=[]    
    out.write("TRAVERSING.................\n")
    out.write("LINK -1 ID  | LINK -2 ID\n")
    LinkedNodes=[]
    print(Pattern_Clusters)
    Map_File=open("SOG Clean Occupancy Data1.txt","r")

    for key, val in Pattern_Clusters.items():
        lst=[]
        i=0
        Left_Dict={}
        for o in Cluster_Dictionary[key]:
            lst.append([str(i)+"_left",o,ID_Name[o]])
            Left_Dict[str(i)+"_left"]=o
            i+=1
        df_left=pd.DataFrame(lst,columns=["ID","col_b","col_a"])
        for l in val:
            if (l or key) not in Visited:
                i=0
                lst2=[]
                Right_Dict={}
                for ii in Cluster_Dictionary[l]:
                    lst2.append([str(i)+"_right",ii,ID_Name[ii]])
                    Right_Dict[str(i)+"_right"]=ii
                    i+=1
                df_right=pd.DataFrame(lst2, columns=["ID","col_b","col_a"])
                df_matches = ts.fuzzy_match(
                    df_left,
                    df_right,
                    'col_a',
                    'col_a',
                    threshold=85,
                   
                )
               
                df_output = df_left.merge(
                    df_matches,
                    how='left',
                    left_index=True,
                    right_on='df_left_id'
                ).merge(
                    df_right,
                    how='left',
                    left_on='df_right_id',
                    right_index=True,
                    suffixes=['_df1', '_df2']
                )
                
                df_output.set_index('df_left_id', inplace=True)       # For some reason the first merge operation wrecks the dataframe's index. Recreated from the value we have in the matches lookup table
                df_output = df_output[['col_a_df1', 'col_b_df1', 'col_b_df2','match_score']]      # Drop columns used in the matching
                df_output.index.name = 'id'
                df_output=df_output.dropna()
                df_output=df_output.drop_duplicates(subset="col_b_df2")
                if len(df_output)>1:
                    Visited.append(l)
                    Visited.append(key)
                    check_for=0
                    first=""
                    for index, row in df_output.iterrows():
                            first=row['col_b_df2']
                            out.write(row['col_b_df1']+"\t\t"+first)
                            out.write("\n")
                            FinalLink[first]=first
                            FinalLink[row['col_b_df1']]=first
                            width=(row['match_score']/100)*10
                            if width>=8:
                                
                                g.add_edge(row['col_b_df1'],first,color='blue',width=2)
                                Record_Record.append([row['col_b_df1'],first])
                                LinkedNodes.append(row['col_b_df1'])
                                LinkedNodes.append(row['col_b_df2'])
                            if width <8:
                                Record_Record.append([row['col_b_df1'],first])
                                g.add_edge(row['col_b_df1'],first,color='red',width=2)
                                LinkedNodes.append(row['col_b_df1'])
                                LinkedNodes.append(row['col_b_df2'])

                    out.write("---- New Record- ------")
                out.write("\n")
    g.force_atlas_2based(gravity=-50, central_gravity=0.01, spring_length=100, spring_strength=0.08, damping=0.4, overlap=0)
    g.show_buttons(filter_=['physics'])
    g.show('Graph_Final.html',local=False) 

# print(Nodes_Final)
# print(Record_Nodes_Link)
# print(Record_Link_Cluster)
# print(Record_Record)


# from neo4j import GraphDatabase

# uri = "neo4j+s://14bca3e3.databases.neo4j.io"
# driver = GraphDatabase.driver(uri, auth=("neo4j", "5SkGCc9Vyc3HUUhrjSqZ_xxBCX1N3x7gBgBA32JIy_I"))

# def create_cluster_nodes(tx, name):
#     tx.run("CREATE (a:Cluster {name: $name})", name=name)

# def create_Record_nodes(tx, recid,name,metadata):
#     tx.run("CREATE (a:Person {name: $name,recid:$recid,address:$metadata})", recid=recid,name=name,metadata=metadata)

# def create_relationship_Nodes(tx, name, friend):
#     tx.run("MATCH (a:Cluster),(b:Cluster) WHERE a.name =$name and b.name=$friend "
#            "CREATE (a)-[r:Cluster_Link]->(b)", name=name, friend=friend)
    
# def create_relationship_Records(tx, name, recid):
#     tx.run("MATCH (a:Cluster), (b:Person) WHERE a.name =$name AND b.recid = $recid "
#            "CREATE (a)-[r: House_Link]->(b)",name=name, recid=recid)    

# def create_relationship_Record_Record(tx, recid, friend):
#     tx.run("MATCH (a:Person) , (b:Person) WHERE a.recid =$recid AND b.recid = $friend "
#            "CREATE (a)-[:Same_Person]->(b)", recid=recid, friend=friend)

# # def create_relationship_Records_Records(tx, name, recid):
# #     tx.run("MATCH (a:Cluster) WHERE a.name = $name "
# #            "CREATE (a)-[:KNOWS]->(:Person {recid: $recid})",
# #            name=name, recid=recid)

# Nodes_WDup=reduce(lambda x,y :x+y ,Nodes_Final)
# Nodes_WDup=set(Nodes_WDup)
# with driver.session() as session:
#     for i in Nodes_WDup:
#         session.execute_write(create_cluster_nodes,i)
        
#     for i in Nodes_Final:
#         session.execute_write(create_relationship_Nodes,i[0],i[1])
        
#     for m in Record_Nodes_Link:
#         session.execute_write(create_Record_nodes,m[0],m[1].split(",")[0]," ".join(str(x) for x in m[1].split(",")[1:]))
   
#     for m in Record_Link_Cluster:
#         session.execute_write(create_relationship_Records,m[0],m[1])
    
    
#     for m in Record_Record:
#         session.execute_write(create_relationship_Record_Record,m[0],m[1])


    
    
#     driver.close()
    



         
# #ER_Metrics_2.generateMetrics(204, FinalLink,"test_data.txt")
ERR.generateMetrics(FinalLink)
# # import pyTigerGraph as tg 



# # graph= tg.TigerGraphConnection(
# #     host="http://127.0.0.1:14240", 
# #     username="tigergraph",
# #     graphname='NameAddressGraph', 
# #     password="onais1234",restppPort=25900,gsPort=25240)

# # secrets="vcitnrnourll67gvb63fd6kv3qmho42a"
# # authtokens=graph.getToken(secret=secrets)
# # print(authtokens)