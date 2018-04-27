__author__ = 'mirko'
import config as cfg
import pymysql
import numpy
import community
import scipy
import networkx as nx
from scipy import stats
import matplotlib.pyplot as plt
db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8mb4',
             use_unicode=True) # name of the data base

cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()

phases=[1,2,3,4]
tables=["friends","retweet","quote","reply"]
phases_label=['EC','DE','TD','RO']

fig, ax = plt.subplots(1, 4,figsize=(15,5))
#[left, bottom, right, top]
fig.tight_layout(rect=[ 0.07, 0.05, 0.97, 0.79],pad=0.5,w_pad=1)

#ax[i][j].set_ylim(0,0.5)
#ax[i][j].set_xlim(0.8,4.2)
#ax[i][j].set_xticks([1,2,3,4])
#ax[i][j].set_xticklabels(phases_label)
plt.setp(ax, xticks=phases, xticklabels=phases_label,
        yticks=[0, 0.1, 0.2, 0.3, 0.4, 0.5])
#Modularity is the fraction of the edges that fall within the given groups minus
#  the expected fraction if edges were distributed at random.
#  The value of the modularity lies in the range [−1/2,1).
#  It is positive if the number of edges within groups exceeds the number expected on the basis of chance.
#  For a given division of the network's vertices into some modules,
#  modularity reflects the concentration of edges within modules compared
#  with random distribution of links between all nodes regardless of modules.

for table in tables:
    phases=[1,2,3,4]
    ticks_perc=numpy.arange(0.0, 1.1, 0.1)

    Q_AFN=[]

    for phase in phases:
        G = nx.Graph()

        cur.execute(""" select id, stance_"""+str(phase)+""" from user where
         (stance_"""+str(phase)+""" ="si"
         or stance_"""+str(phase)+"""="no"
         or stance_"""+str(phase)+"""="nd"
         )
         """)
        results  = cur.fetchall()
        nodes=[]
        stances={}
        for result  in results:
            nodes.append(result[0])
            stances[result[0]]=result[1]

        cur.execute("""
        SELECT source, target FROM `user_"""+table+"""_relation`
        where
        source  in %s and
        target in %s
        and (phase=%s or phase is null)
        """,(nodes,nodes,phase))
        edges=cur.fetchall()

        for edge in edges:
            G.add_edge(edge[0],edge[1])






        nx.transitivity(G)

        # Find modularity
        #part = community.best_partition(G)
        #print(part)
        mod = community.modularity(stances,G)
        Q_AFN.append(mod)
        colors={"si":"green","no":"red","nd":"blue"}
        # Plot, color nodes using community structure
        #values = [colors[stances.get(node)] for node in G.nodes()]
        #nx.draw_spring(G, cmap=plt.get_cmap('jet'), node_color = values, node_size=30, with_labels=False)
        #plt.show()




    Q_AF=[]

    for phase in phases:
        G = nx.Graph()

        cur.execute(""" select id, stance_"""+str(phase)+""" from user where
         (stance_"""+str(phase)+""" ="si"
         or stance_"""+str(phase)+"""="no"

         )
        """)
        results  = cur.fetchall()
        nodes=[]
        stances={}
        for result  in results:
            nodes.append(result[0])
            stances[result[0]]=result[1]

        cur.execute("""
        SELECT source, target FROM `user_"""+table+"""_relation`
        where
        source  in %s and
        target in %s
        and (phase=%s or phase is null)


        """,(nodes,nodes,phase))
        edges=cur.fetchall()

        for edge in edges:
            G.add_edge(edge[0],edge[1])


        nx.transitivity(G)

        # Find modularity
        #part = community.best_partition(G)
        #print(part)
        mod = community.modularity(stances,G)
        Q_AF.append(mod)
        colors={"si":"green","no":"red","nd":"blue"}
        # Plot, color nodes using community structure
        #values = [colors[stances.get(node)] for node in G.nodes()]
        #nx.draw_spring(G, cmap=plt.get_cmap('jet'), node_color = values, node_size=30, with_labels=False)
        #plt.show()

    i=tables.index(table)

    p1,=ax[i].plot(phases,Q_AFN,"-k", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p2,=ax[i].plot(phases,Q_AF,"-c", clip_on=True, linewidth=3, markersize=15)#linetipes[table])

    lettera=['a','b','c','d']

    if table=="friends":
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"-based networks", fontsize=17, verticalalignment="bottom")
    elif table=="reply":
        title=ax[i].set_title(lettera[tables.index(table)]+") replies-based networks", fontsize=17, verticalalignment="bottom")
    else:
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"s-based networks", fontsize=17, verticalalignment="bottom")


    title.set_position((0.5,-0.2))

    ax[i].set_ylim(0,0.3)
    ax[i].set_xlim(0.8,4.2)
    #ax[i][j].set_xticks([1,2,3,4])
    #ax[i][j].set_xticklabels(phases_label)
    plt.setp(ax[i].get_xticklabels(), visible=True)
    ax[i].grid()




fig.legend( handles=(p1,p2) , labels=  ('$Q_{AFN}$','$Q_{AF}$') ,ncol=2, loc='upper center',
           bbox_to_anchor=[0.5, 0.9],
           columnspacing=1.0, labelspacing=1.0,
           handletextpad=2.0, handlelength=5,
           fancybox=True, shadow=True)

#plt.xlabel('Temporal phases')
# plt.ylabel('Modularity Q')
plt.show()

