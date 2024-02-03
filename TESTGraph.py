# -*- coding: utf-8 -*-
"""
Created on Mon May 23 23:36:56 2022

@author: onais
"""

import pandas as pd
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
 
# Build a dataframe with your connections
df = pd.DataFrame({ 'from':["C1","C2","C4","C1","C1","C1"], 'to':["C2","C3","C5","C3","R1","R2"], 'value':['#00ff00','#00ff00','#00ff00','#00ff00','#00ff00','#00ff00']})
 # C1,C2
 # C2,C3
 # C4,C5
 # C1,C3
# Build your graph
G=nx.from_pandas_edgelist(df, 'from', 'to', create_using=nx.Graph() )

# Custom the nodes:
nx.draw_shell(G, with_labels=True,node_color=['lightblue','lightblue','lightblue','lightblue','lightblue','yellow','yellow'], edge_color=df['value'],cmap=plt.get_cmap('jet'),node_size=1500, node_shape="o", alpha=0.8,font_size=15, font_color="black", font_weight="bold")

