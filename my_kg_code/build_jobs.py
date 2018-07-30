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
jd_skills = []
for row_num, row in enumerate(cv_s.iterrows()):
    jd_skills.append(list(cv_s.iloc[row_num].dropna()))

# jd_skills = [['object oriented', 'design patterns', 'software architecture', 'product development', 'java', 'j2ee', 'node.js', 'ui technologies', 'rest', 'javascript', 'reactjs', 'react-native', 'html', 'css', 'nosql', 'mongodb', 'amazon web services (aws)', 'any cloud hosting', 'analyse', 'develop', 'validate architecture', 'continuous integration', 'continuous delivery', 'git', 'maven', 'docker'],
#               ['java','object oriented', 'functional programming', 'jax-rs', 'jax-ws', 'j2ee', 'scala', 'scrum', 'architecture', 'design', 'coding standards', 'design patterns', 'code reviews', 'unit testing', 'gradle', 'testing', 'operations', 'cloud', 'saas']
#              ]

count = 0
for jid, skills in enumerate(jd_skills):
    jd = g.addV('job').property('name', 'job_' + str(jid)).next()    
    print (count)
    count += 1
    for skill in skills:
        sk_count = g.V().hasLabel('skill').has('name', skill).count().next()
        
        if sk_count == 1:
            sk_id = g.V().hasLabel('skill').has('name', skill).next().id
            g.V(sk_id).as_('r').V(jd).addE('requires_skill').to('r').toList()
