import numpy
import scipy

__author__ = 'mirko'
import config as cfg
import pymysql
import matplotlib.patches as mpatches

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
#'-' | '--' | '-.' | ':'
#'b', 'g', 'r', 'c', 'm', 'y', 'k', 'w'
#"1"	tri_down"2"	tri_up"3"	tri_left"4"	tri_right
#"v"	triangle_down "^"	triangle_up "<"	triangle_left ">"	triangle_right
# labels = []
#1k -k
pointtipes={"reply":"vk","quote":"^c","retweet":"<m","friends":">y"}
linetipes= {"reply":":k","quote":"--c","retweet":"-.m","friends":"-y"}
data=[]
legends=[]
#for table in ["reply",]:


phases=[1,2,3,4]
tables=[
    "friends",
 "retweet","quote","reply"]
phases_label=['EC','DE','TD','RO']

tables_id=[(0,0),(0,1),(1,0),(1,1)]
fig, ax = plt.subplots(1, 4,figsize=(15,5))
#[left, bottom, right, top]
fig.tight_layout(rect=[ 0.07, 0.15, 0.97, 0.85],pad=0.5,w_pad=1)

#plt.setp(ax, xticks=phases, xticklabels=phases_label,
#        yticks=[0, 0.1, 0.2, 0.3, 0.4, 0.5,0.6])



for table in tables:
    eterogenitychange={}
    eterogenitynotchange={}


    for phase in [1,2,3]:

        if table !="friends":
            cur.execute("""
            SELECT distinct `source` FROM `user_"""+table+"""_relation`
            where phase=%s


            """,(phase))
        else:
            cur.execute("""
            SELECT distinct `source` FROM `user_"""+table+"""_relation`


            """)

        users=cur.fetchall()
        i=len(users)


        for user in users:
            i-=1
            if i%1000==0:
                print(str(i))
            change=None

            cur.execute("SELECT `user`.`stance_"+str(phase)+"` FROM `user` where id=%s",(user[0]))
            stanceNOW=cur.fetchone()

            cur.execute("SELECT `user`.`stance_"+str(phase+1)+"` FROM `user` where id=%s",(user[0]))
            stanceFUTURE=cur.fetchone()

            #print(stance3,stance4)

            if  stanceNOW is not None and stanceFUTURE is not None and stanceNOW[0] is not None and stanceFUTURE[0] is not None:

                if   stanceNOW[0] in ["si","no"] and stanceNOW[0] == stanceFUTURE[0]:
                    change=False
                elif stanceNOW[0] in ["si","no"]      and stanceFUTURE[0] == "nd":
                    change=True

                if change is not None:
                    agree=0
                    disagree=0

                    if table !="friends":
                        cur.execute(""" SELECT distinct `target` FROM `user_"""+table+"""_relation` where
                        phase=%s
                        and source=%s""",(phase,user))
                    else:
                        cur.execute(""" SELECT distinct `target` FROM `user_"""+table+"""_relation` where
                        source=%s""",(user))

                    targets=cur.fetchall()
                    for target in targets:
                        cur.execute("SELECT `user`.`stance_"+str(phase)+"` FROM `user` where id=%s",(target[0]))
                        stance3T=cur.fetchone()
                        if  stance3T is not None and stance3T[0] is not None and stance3T[0]  != "nd":

                            if stanceNOW[0] != stance3T[0]:
                                disagree += 1
                            else:
                                agree += 1

                    if disagree+agree != 0:

                        key=str(round(disagree/(disagree+agree),1))

                        if change:
                            if key in eterogenitychange:
                                eterogenitychange[key]+=1
                            else:
                                eterogenitychange[key]=1

                        if not change:
                            if key in eterogenitynotchange:
                                eterogenitynotchange[key]+=1
                            else:
                                eterogenitynotchange[key]=1




    print("eterogenitychange",eterogenitychange)
    print("eterogenitynotchange",eterogenitynotchange)

    for key in sorted(eterogenitychange):

        if key in eterogenitynotchange:
            print(key, eterogenitychange[key]/(eterogenitychange[key]+eterogenitynotchange[key]),eterogenitychange[key],eterogenitynotchange[key])


    x=[]
    y=[]

    for key in sorted(eterogenitychange):
        if key in eterogenitynotchange:
            x.append(float(key))
            y.append(eterogenitychange[key]/(eterogenitychange[key]+eterogenitynotchange[key]))


    i=tables.index(table)#[1]

    slope, intercept = numpy.polyfit(x, y, 1)
    slope, intercept, r_value, p_value, std_err = scipy.stats.linregress(x, y)

    line = [slope * i + intercept for i in x]
    p1,=ax[i].plot(x,y,"-k", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p2,=ax[i].plot(x,line,"--k", clip_on=True, linewidth=3, markersize=15)#linetipes[table])
    p3,=ax[i].plot(x,y,"ok", clip_on=True, linewidth=3, markersize=5)#linetipes[table])


    lettera=['a','b','c','d']

    if table=="friends":
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"-based networks", fontsize=17, verticalalignment="bottom")
    elif table=="reply":
        title=ax[i].set_title(lettera[tables.index(table)]+") replies-based networks", fontsize=17, verticalalignment="bottom")
    else:
        title=ax[i].set_title(lettera[tables.index(table)]+") "+table+"s-based networks", fontsize=17, verticalalignment="bottom")

    title.set_position((0.5,-0.35))

    ax[i].set_ylim(0,1)
    ax[i].set_xlim(0,1)
    #ax[i][j].set_xticks([1,2,3,4])
    #ax[i][j].set_xticklabels(phases_label)
    plt.setp(ax[i].get_xticklabels(), visible=True)
    ax[i].grid()



fig.text(0.5, 0.1, 'Fraction of cross-stance edges', ha='center')
fig.text(0.04, 0.5, 'Probability of change stance' , va='center', rotation='vertical')

fig.legend( handles=(p1,p2) , labels=  ('$Data$ $Points$','$Interpolation$') ,ncol=2, loc='upper center',
           bbox_to_anchor=[0.5, 0.97],
           columnspacing=1.0, labelspacing=1.0,
           handletextpad=2.0, handlelength=5,
           fancybox=True, shadow=True)

#plt.tight_layout()
plt.show()




