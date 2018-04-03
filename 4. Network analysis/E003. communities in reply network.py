__author__ = 'mirko'
import config as cfg
import pymysql
import community # --> http://perso.crans.org/aynaud/communities/
import networkx as nx


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

cur.execute("""CREATE TABLE IF NOT EXISTS `user_reply_relation_communities` (
  `id` bigint(20) NOT NULL,
  `community` bigint(20) NOT NULL,
  `phase` int(11) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;
""")
db.commit()

for phase in range(1,5):

    cur.execute("""
    SELECT `source`, `target` FROM `user_reply_relation` where phase=%s
    """,(phase))
    edges=cur.fetchall()
    i=len(edges)

    G=nx.Graph()

    for edge in edges:
        i-=1
        if i%10000==0:
            print(str(i))
            print("Number of nodes ",G.number_of_nodes())
            print("Number of edges ",G.number_of_edges())

        G.add_edge(edge[0],edge[1])

    print("Number of nodes ",G.number_of_nodes())
    print("Number of edges ",G.number_of_edges())
    print(G.is_directed())
    partition = community.best_partition(G)
    print("Louvain Modularity: ", community.modularity(partition, G))
    #print("Louvain Partition: ", partition)

    i=len(partition.items())

    for key,value in partition.items():
        i-=1
        print(str(i))
        #print(key,value)
        cur.execute(" INSERT INTO "
                    " `user_reply_relation_communities`"
                    " (`id`, `community`,phase) "
                    " VALUES "
                    " (%s,%s,%s) on duplicate key update community=%s",(key,value,phase,value))





