__author__ = 'mirko'
import config as cfg
import pymysql
import networkx as nx
import matplotlib.pyplot as plt
import csv
import  numpy




phases=[1,2,3,4]
tables=["friends","retweet","quote","reply"]
phases_label=['EC','DE','TD','RO']

tables_id=[(0,0),(0,1),(1,0),(1,1)]
fig, ax = plt.subplots(1, 4,figsize=(15,5))
#[left, bottom, right, top]
fig.tight_layout(rect=[ 0.07, 0.05, 0.97, 0.79],pad=0.5,w_pad=1)

plt.setp(ax, xticks=phases, xticklabels=phases_label,
        yticks=[0, 0.1, 0.2, 0.3, 0.4, 0.5,0.6])








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


for table in tables:
    fractionAFN={"no":[0,0,0,0],"si":[0,0,0,0],"nd":[0,0,0,0]}
    edgestypeAFN={"nosi":[0,0,0,0],"ndsi":[0,0,0,0],"ndno":[0,0,0,0],
               "nono":[0,0,0,0],"sisi":[0,0,0,0],"ndnd":[0,0,0,0],
               "sino":[0,0,0,0],"sind":[0,0,0,0],"nond":[0,0,0,0],
               }

    fractionAF={"no":[0,0,0,0],"si":[0,0,0,0]}
    edgestypeAF={"nosi":[0,0,0,0],"sino":[0,0,0,0],
                "nono":[0,0,0,0],"sisi":[0,0,0,0],}
    #+++++++++++++++++++++++++++++++++ AFN

    for phase in phases:

        cur.execute(""" select id, stance_"""+str(phase)+""" from user where
         (stance_"""+str(phase)+""" ="si"
         or stance_"""+str(phase)+"""="no"
         or stance_"""+str(phase)+"""="nd"


         )""")
        results  = cur.fetchall()

        nodes=[]
        stances={}
        for result  in results:
            nodes.append(result[0])
            stances[result[0]]=result[1]

        cur.execute("""
        SELECT source, target FROM `user_"""+table+"""_relation`
        where
        source in %s and
        target in %s
        AND (phase=%s or phase is null)""",(nodes,nodes,phase))
        edges=cur.fetchall()

        for edge in edges:
            edgestypeAFN[str(stances[edge[0]])+str(stances[edge[1]])][phase-1]+=1

        for n in nodes:
            fractionAFN[stances[n]][phase-1]+=1

        #+++++++++++++++++++++++++++++++++only AF



        cur.execute(""" select id, stance_"""+str(phase)+""" from user where
         (stance_"""+str(phase)+""" ="si"
         or stance_"""+str(phase)+"""="no"
         )""")
        results  = cur.fetchall()




        nodes=[]
        stances={}
        for result  in results:
            nodes.append(result[0])
            stances[result[0]]=result[1]

        cur.execute("""
        SELECT source, target FROM `user_"""+table+"""_relation`
        where
        source in %s and
        target in %s
        AND (phase=%s or phase is null)""",(nodes,nodes,phase))
        edges=cur.fetchall()

        for edge in edges:
            edgestypeAF[str(stances[edge[0]])+str(stances[edge[1]])][phase-1]+=1

        for n in nodes:
            fractionAF[stances[n]][phase-1]+=1



    #print(edgestypeAFN)
    #print(fractionAFN)
    #print(edgestypeAF)
    #print(fractionAF)
    _2AFN=[]
    _2AF=[]
    crossstanceAFN=[]
    crossstanceAF=[]

    for phase in [0,1,2,3]:

        fractionA=fractionAFN["no"][phase]/(fractionAFN["no"][phase]+fractionAFN["si"][phase]+fractionAFN["nd"][phase])
        fractionF=fractionAFN["si"][phase]/(fractionAFN["no"][phase]+fractionAFN["si"][phase]+fractionAFN["nd"][phase])
        fractionN=fractionAFN["nd"][phase]/(fractionAFN["no"][phase]+fractionAFN["si"][phase]+fractionAFN["nd"][phase])

        _2AFN.append(2*(fractionA*fractionF)+(fractionA*fractionN)+(fractionF*fractionN))

        fractionA=fractionAF["no"][phase]/(fractionAF["no"][phase]+fractionAF["si"][phase])
        fractionF=fractionAF["si"][phase]/(fractionAF["no"][phase]+fractionAF["si"][phase])

        _2AF.append(2*fractionA*fractionF)

        uncrossstanceedges=edgestypeAFN["nono"][phase]+edgestypeAFN["sisi"][phase]+edgestypeAFN["ndnd"][phase]
        crossstanceedges  =edgestypeAFN["nosi"][phase]+edgestypeAFN["nond"][phase]\
                        +edgestypeAFN["sino"][phase]+edgestypeAFN["sind"][phase]\
                        +edgestypeAFN["ndsi"][phase]+edgestypeAFN["ndno"][phase]

        crossstanceAFN.append(crossstanceedges/(crossstanceedges+uncrossstanceedges))

        uncrossstanceedges=edgestypeAF["nono"][phase]+edgestypeAF["sisi"][phase]
        crossstanceedges  =edgestypeAF["nosi"][phase] +edgestypeAF["sino"][phase]

        crossstanceAF.append(crossstanceedges/(crossstanceedges+uncrossstanceedges))

    print(table)
    print("%_2AFN",_2AFN)
    print("%crossstanceAFN",crossstanceAFN)
    print("%crossstanceedgesAFN",numpy.average(crossstanceAFN),"sd",numpy.std(crossstanceAFN))
    print("%2afn",numpy.average(_2AFN),"sd",numpy.std(_2AFN))

    print("%_2AF",_2AF)
    print("%crossstanceAF",crossstanceAF)
    print("%crossstanceedgesAF",numpy.average(crossstanceAF),"sd",numpy.std(crossstanceAF))
    print("%2af",numpy.average(_2AF),"sd",numpy.std(_2AF))
    print(ax)
    i=tables.index(table)#[1]

    p1,=ax[i].plot(phases,_2AFN,"--k", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p2,=ax[i].plot(phases,crossstanceAFN,"-k", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p3,=ax[i].plot(phases,_2AF,"--c", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p4,=ax[i].plot(phases,crossstanceAF,"-c", clip_on=True, linewidth=3, markersize=15)#linetipes[table])

    lettera=['a','b','c','d']

    if table=="friends":
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"-based networks", fontsize=17, verticalalignment="bottom")
    elif table=="reply":
        title=ax[i].set_title(lettera[tables.index(table)]+") replies-based networks", fontsize=17, verticalalignment="bottom")
    else:
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"s-based networks", fontsize=17, verticalalignment="bottom")

    title.set_position((0.5,-0.2))
    ax[i].set_ylim(0,0.6)
    ax[i].set_xlim(0.8,4.2)
    #ax[i][j].set_xticks([1,2,3,4])
    #ax[i][j].set_xticklabels(phases_label)
    plt.setp(ax[i].get_xticklabels(), visible=True)
    ax[i].grid()


fig.legend( handles=(p1,p2,p3,p4) , labels=  ('$2(AF+AN+NF)$','$CE_{AFN}$','$2AF$','$CE_{AF}$') ,ncol=2, loc='upper center',
           bbox_to_anchor=[0.5, 1],
           columnspacing=1.0, labelspacing=1.0,
           handletextpad=2.0, handlelength=5,
           fancybox=True, shadow=True)

#plt.xlabel('Temporal phases')
# plt.ylabel('Modularity Q')
plt.show()
