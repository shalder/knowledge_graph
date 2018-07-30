import sys
import traceback
from collections import Counter, defaultdict

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

matches = []
skills = defaultdict(list)
for match in g.V().hasLabel('candidate').out().inE('requires_skill').outV().hasLabel('job').path().toList():
    c = g.V(match[0].id).values('name').next()
    j = g.V(match[-1].id).values('name').next()
    
    skills[str(c) + ':' + str(j)].append(g.V(match[1].id).values('name').next())
    matches.append(str(c) + ':' + str(j))

import json
# print (json.dumps(Counter(matches), indent=4))

print (json.dumps(dict(skills), indent=4))

skills = defaultdict(set)
for match in (g.V().hasLabel('candidate').out().inE('related').outV().out().inE('requires_skill').outV().path().toList()):
    print (g.V(match[1]).values('name').next(), g.V(match[4]).values('name').next())
    c = g.V(match[0].id).values('name').next()
    j = g.V(match[-1].id).values('name').next()
    
    skills[str(c) + ':' + str(j)].add(g.V(match[1].id).values('name').next() + ' <- ' + g.V(match[3].id).values('name').next() + ' -> ' + g.V(match[4].id).values('name').next())

temp = ([(skill, list(skills[skill])) for skill in skills])
import json
# print (json.dumps(Counter(matches), indent=4))

print (json.dumps(dict(temp), indent=4))
