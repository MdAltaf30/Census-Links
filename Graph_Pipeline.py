# -*- coding: utf-8 -*-
"""
Created on Wed May 18 19:52:24 2022

@author: onais
"""
import sys
sys.path.insert(0, './dwm-refactor-v1/')
import DWM00_Driver as DWM
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from pyvis.network import Network
import re
from tqdm import tqdm
import jellyfish as sd
from fuzzywuzzy import fuzz
import TEST_FUZZY_MATCH_MERGE as ts
import NameParser_for_Graph as NParser
#DWM.DWM_Cluster("S2-parms.txt")
import re
pattern = r"((?<=^)|(?<= )).((?=$)|(?= ))"

#Parsing 1st program
Address_4CAF50=open("SOG Clean Occupancy Data.txt","r")
Lines = Address_4CAF50.readlines()
DF=[]
ii=0
count = 0
FinalList=[]
fileHandle = open('USAddressWordTable.txt', 'r')
NamefileHandle = open('NamesWordTableOpt.txt', 'r')
SplitWordTable = open('SplitWordTable.txt', 'r')
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
    file_n = open("FileN.txt", "w")
    out.write("Step- 1 Address Parser Output")
    out.write("Name Splitting")
    out.write("\n")
    ID_Name={}
    for element in Lines:
        elem=element.split("|")
        ID_Name[elem[0]]=elem[1]
    file_n.close()
    file_n_r = open("FileNM.txt", "r")
    LinesRead=file_n_r.readlines()
    file_n_r.close()
    out.write("Splitting.......\n\n")
    file_n_w = open("NewFileTemp.txt", "w")
    for k in LinesRead:
        splitData=k.split("|")
        splitName=NParser.ExtractNames(splitData[1])
        splitName=splitName.split("..")
        for o in splitName:
            file_n_w.write(o+"|"+splitData[2])
            out.write(sd.soundex(o)+"|"+splitData[2])
            out.write("\n")
            file_n_w.write("\n")
    file_n_w.close()
    file_n_w.close()
    
    with open('NewFileTemp.txt') as fl:
        content = fl.read().split('\n')
    content = set([line for line in content if line!=' '])
    content = '\n'.join(content)
    
    #(content)
    with open('NewFileTemp.txt', 'w') as fl:
        fl.writelines(content)
    combining_centers=open("NewFileTemp.txt","r")
    LinesRead=combining_centers.readlines()
    Final_Cluster=[]
    file_n_w = open("NewFileTemp.txt", "w")
    con=0
    
    for j in LinesRead:
        SplitW=j.strip().split("|")
        Temp=LinesRead
        Temp.remove(j)
        for p in Temp:
            con+=1
            SplitC=p.strip().split("|")
            try:
                if SplitW[0]==SplitC[0] and SplitW[1]!=SplitC[1]:
            
                    Final_Cluster.append((SplitW[1].strip(),SplitC[1].strip())) 
            except:
                continue
    print(con)
             
    outt=list(set(map(tuple,map(sorted,Final_Cluster))))
    Cluster_to_Cluster=[]
    out.write("Final Cluster Map.........")
    out.write("\n")
    with open("NewFileTemp.txt","w") as O:
        for k in outt:
            O.writelines(k[0]+","+k[1])
            O.writelines("\n")
            out.write(k[0]+","+k[1])
            out.write("\n")
            Cluster_to_Cluster.append([k[0],k[1]])

    g=Network(height='100%', width='100%',directed=True)
            
    open_Cluster=open("NewFileTemp.txt","r")
    listcluster=open_Cluster.readlines()
    ListFrom=[]
    ListTo=[]
    edge_color=[]
    
    NodesCluster=set()
    for m in listcluster:
        SplitX=m.split(",")
        NodesCluster.add(SplitX[0].strip())
        NodesCluster.add(SplitX[1].strip())
        
    for i in NodesCluster:
        #(i)
        g.add_node(i,color='blue',title=i,label=i)
    NodesCluster.clear()
    for m in listcluster:
        SplitX=m.split(",")
        ListFrom.append(SplitX[0].strip())
        ListTo.append(SplitX[1].strip())
        edge_color.append(10)
        g.add_edge(SplitX[0].strip(),SplitX[1].strip(),color='black',width=2,arrowStrikethrough=True)
        NodesCluster.add(SplitX[0].strip())
        NodesCluster.add(SplitX[1].strip())
    
    
    open_Cluster.close()
    rangeo=set(ListFrom+ListTo)
    Map_File=open("SOG Clean Occupancy Data.txt","r")
    Map=Map_File.readlines()
    node_color=[]
    for k in range(len(rangeo)):
        node_color.append("#00ff00")
    Cluster_to_Nodes=[]
    for k in Map:
        SplitX=k.split("|")
        #(SplitX[2])
        ListFrom.append(SplitX[2].strip())
        ListTo.append(SplitX[0].strip())
        g.add_node(SplitX[0].strip(),color='yellow',title=SplitX[0].strip(),label=SplitX[1].strip(),shape="ellipse")
        try:
            g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='red',width=2)
        except:
            g.add_node(SplitX[2].strip(),color='blue',title=SplitX[2].strip(),label=SplitX[2].strip())
            g.add_edge(SplitX[2].strip(),SplitX[0].strip(),color='red',width=2)

        edge_color.append(10)
        node_color.append("#4CAF50")
        Cluster_to_Nodes.append([SplitX[0].strip(),SplitX[2].strip(),SplitX[1].strip()])
    Map_File.close()
    df = pd.DataFrame({ 'from':ListFrom, 'to':ListTo, 'value':edge_color})
     # C1,C2
     # C2,C3
     # C4,C5
     # C1,C3
    # Build your graph
    Cluster_Dictionary={}
    for m in NodesCluster:
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
                    threshold=70,
                    limit=1
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
                df_output = df_output[['col_a_df1', 'col_b_df1', 'col_b_df2']]      # Drop columns used in the matching
                df_output.index.name = 'id'
                df_output=df_output.dropna()
                df_output=df_output.drop_duplicates(subset="col_b_df2")
                if len(df_output)>1:
                    Visited.append(l)
                    Visited.append(key)
                    for index, row in df_output.iterrows():
                        if (row['col_b_df1'] or row['col_b_df1']) in LinkedNodes:
                            continue
                        out.write(row['col_b_df1']+"\t\t"+row['col_b_df2'])
                        out.write("\n")
                        
                        
                        g.add_edge(row['col_b_df1'],row['col_b_df2'],color='green',width=1)
                        LinkedNodes.append(row['col_b_df1'])
                        LinkedNodes.append(row['col_b_df2'])
                    out.write("---- New Record- ------")
                    out.write("\n")
                    break
                
                        # g.add_edge(j[0],j[1],color='green',width=1)
                        # Visited.append(temp)
                        # Visited.append(temp2)
                # DF=fuzzymatcher.fuzzy_left_join(df_left, df_right, left_on = "NAME", right_on = "NAME")
                # Check_Null=DF.isnull().values.any()
                
                # TDF = DF.drop_duplicates(subset = ["__id_right"])
                # TDF = DF.drop_duplicates(subset = ["__id_left"])
                # two=TDF["__id_right"].duplicated().any()
                # if (not Check_Null)and  (not two) and (len(DF)>1):
                    
                #     for index, row in TDF.iterrows():
                #         temp=set()
                #         temp.add(Left_Dict[row["__id_left"]])
                #         temp.add(Right_Dict[row["__id_right"]])
                    
                         
                #        # g.add_edge(Left_Dict[row["__id_left"]],Right_Dict[row["__id_right"]],color='green',width=1)
                #         ##(temp)
                #         ##(TDF)
                #         Visited.append(temp)
                #         Visited.append(temp2)
    #        DF.to_csv("OOO.csv")
                
    
    
    # Str_A = 'Onais Khan Mohammed' 
    # Str_B = 'Onais Khan'
    # ratio = fuzz.partial_ratio(Str_A.lower(), Str_B.lower())
    # #('Similarity score: {}'.format(ratio))
    
    # df_left = pd.read_csv("left_1.csv")
    # df_right = pd.read_csv("right_1.csv")
    # DF=fuzzymatcher.link_table(df_left, df_right, left_on = "name", right_on = "name")
    # DF.to_csv("OOO.csv")
    # fuzzymatcher.fuzzy_left_join(df_left, df_right, left_on = "name", right_on = "name")

    
    
    
    g.show_buttons()
    g.show('Graph.html')


# import pyTigerGraph as tg 



# graph= tg.TigerGraphConnection(
#     host="http://127.0.0.1:14240", 
#     username="tigergraph",
#     graphname='NameAddressGraph', 
#     password="onais1234",restppPort=25900,gsPort=25240)

# secrets="vcitnrnourll67gvb63fd6kv3qmho42a"
# authtokens=graph.getToken(secret=secrets)
# #(authtokens)



























