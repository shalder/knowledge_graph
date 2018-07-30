import sys
import traceback
import pandas as pd
from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.process.traversal import T
from gremlin_python.process.traversal import Order
from gremlin_python.process.traversal import Cardinality
from gremlin_python.process.traversal import Column
from gremlin_python.process.traversal import Direction
from gremlin_python.process.traversal import Operator
from gremlin_python.process.traversal import P
from gremlin_python.process.traversal import Pop
from gremlin_python.process.traversal import Scope
from gremlin_python.process.traversal import Barrier
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
statics.load_statics(globals())
graph = Graph()

try:
    # 8182 is the default gremlin Server port
    g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin','g'))
except Exception as e:
    print ('Connection to gremlin server failed. Connection error stack trace:')
    print (traceback.format_exc())
    sys.exit(-1)


cv_s = pd.read_csv('cv_s.csv', low_memory=False)
candidate_skills = []
for row_num, row in enumerate(cv_s.iterrows()):
    candidate_skills.append(list(cv_s.iloc[row_num].dropna()))

# candidate_skills = [['jboss', 'weblogic', 'netbeans', 'wsad', 'eclipse', 'jakarta', 'hibernate', 'spring', 'cvs', 'svn', 'log4j'],
#                     ['statistics', 'mongodb', 'data analysis', 'business intelligence', 'security', 'desriptinve analytics', 'sql', 'r', 'python', 'tableau', 'time series analysis', 'forcasting', 'data cleaning', 'data mining', 'predictive modeling', 'logistic regression', 'clustering', 'linear regression', 'generalized linear models', 'recommendation engines', 'decision trees', 'data visualization', 'azure ml studio', 'market basket analysis', 'factor analysis', 'hypothesis testing'],
#                     ['struts', 'spring framework', 'lift', 'java', 'j2ee', 'scala', 'html', 'javascript', 'ajax', 'jquery', 'google app engine', 'microsoft sql server', 'mysql', 'hibernate', 'apache cassandra', 'memcache', 'mongodb', 'apache hbase' , 'wordnet', 'pentaho', 'apache hadoop', 'weka', 'apache-pig', 'rest', 'xml', 'jax-rs', 'cvs', 'svn', 'eclipse'],
#                     ['statistics', 'mongodb', 'data analysis', 'business intelligence', 'website security', 'descriptive analytics', 'sql', 'google bigquery', 'r', 'python', 'tableau', 'time series analysis', 'forecasting', 'data cleaning', 'data mining', 'predictive modeling'],
#                     ['r', 'python', 'tensorflow', 'keras', 'java', 'scala', 'pyspark', 'sparkr', 'rhadoop', 'hdfs', 'yarn', 'apache hbase', 'mapreduce', 'apache hive', 'amazon web services (aws)', 'nltk', 'tableau', 'wordnet', 'rshiny', 'c', 'cnn', 'deep neural networks', 'auto-encoders', 'deep learning', 'svm', 'linear regression', 'logistic regression', 'knn', 'decision trees', 'random forest', 'clustering', 'boosting', 'bagging', 'ensembles', 'association rules', 'bayesian', 'algorithms', 'text mining', 'nlp', 'word2vec', 'doc2vec', 'semi-supervised learning', 'computer vision', 'agent based modeling', 'market basket analysis', 'market mix modeling', 'markov models', 'pricing analytics']
#                     ]

count = 0
for cid, skills in enumerate(candidate_skills):
    candidate = g.addV('candidate').property('name', 'candidate_' + str(cid)).next()    
    print (count)
    count += 1
    for skill in skills:
        sk_count = g.V().hasLabel('skill').has('name', skill).count().next()
        
        if sk_count == 1:
            sk_id = g.V().hasLabel('skill').has('name', skill).next().id
            g.V(sk_id).as_('r').V(candidate).addE('has_skill').to('r').toList()
