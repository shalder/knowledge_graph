import sys
import math
import traceback
from collections import Counter
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
    g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin','g'))
except Exception as e:
    print ('Connection to gremlin server failed. Connection error stack trace:')
    print (traceback.format_exc())
    sys.exit(-1)

skills = ['java', 'c', 'c++', 'html', 'css', 'jquery', 'bootstrap', 'mysql', 'javascript', 'wireframing', 'android development']

print ('Candidate Skills: ' + str(skills))

skill_domains = []
for skill in skills:
    count = g.V().has('name', skill).count().next()
    
    if count == 1:
        domain = (g.V().has('name', skill).repeat(in_().simplePath()).until(hasLabel('skill-domain')).path().toList())
        
        if len(domain) > 0:
            domain = domain[0][-1]
        else:
            print ('error' * 6)
        skill_domains.append(g.V(domain).values('name').next())

print (Counter(skill_domains))

def find_min_path(paths):
    if len(paths) == 0:
        return []
    min_path = paths[0]
    
    if len (paths) > 1:
        for path in paths[1:]:
            if len(path[1]) < len(min_path[1]):
                min_path = path

    return ((min_path[1], min_path[0]))

def find_common_ancestor(skill_1, skill_2):
    skill_1 = g.V().has('name', skill_1).next().id
    skill_2 = g.V().has('name', skill_2).next().id
    
    # Direct path from a supply to a demand 
    # ex.: Javascript -> jQuery
    if (g.V(skill_1).out().hasId(skill_2).count().next()) == 1:
        return ((g.V(skill_1).values('name').next(), [g.V(skill_1).values('name').next(), g.V(skill_2).values('name').next()]))
    
    # parents_1 = g.V(skill_1).repeat(in_('related').simplePath()).until(hasLabel('skill-domain')).path().by('name').toList()
    # print (parents_1[0][-1])
    # parents_2 = g.V(skill_2).repeat(in_('related').simplePath()).until(hasLabel('skill-domain')).path().by('name').toList()
    # print (parents_2[0][-1])
    # if (parents_1[0][-1] != parents_2[0][-1]):
    #     return []
    ancestor = g.V(skill_1).repeat(in_('related')).emit().as_('x').repeat(out('related')).emit(hasId(skill_2)).select('x').limit(1).toList()#.hasLabel(neq('skill-domain'))
    if len(ancestor) == 0:
        return []
    ancestor = ancestor[0].id
    
    l = g.V(ancestor).repeat(out().simplePath()).until(hasId(skill_1)).path().by('name').toList()[0]
    r = g.V(ancestor).repeat(out().simplePath()).until(hasId(skill_2)).path().by('name').toList()[0]

    return ([g.V(ancestor).values('name').next(), list(l)[::-1][:-1] + list(r)])

print ('Skill gaps analysis')

print('Java needed but C++ present')
print ('Find a path from Java to C++')
print (find_common_ancestor('c++', 'java'))

print ('*' * 50)

print('SOA needed but REST present')
print (find_common_ancestor('soa', 'rest'))

print ('*' * 50)

print ('No link example: Jira needed but Eclipse present')
print (find_common_ancestor('jira', 'eclipse'))

print ('*' * 50)

print('PyTorch needed but Tensorflow present')
print (find_common_ancestor('pytorch', 'tensorflow'))

print ('*' * 50)

print('PyTorch needed but Python present')
print (find_common_ancestor('pytorch', 'python'))

print ('*' * 50)

print('PyTorch needed but Deep Learning present')
print (find_common_ancestor('pytorch', 'deep learning'))


def cv_jd_match(supply, demand, allowed_hops = 5):
    print ('Supply: ' + str(supply))
    print ('Demand: ' + str(demand))

    match = supply.intersection(demand)

    print ('Matched skills: ' + str(match))

    supply_additional = supply.difference(match)
    demand_missing = demand.difference(match)

    gap_matches = list()
    gap_length = list()

    for dmnd in demand_missing:
        paths = []
        for sply in supply:
            res = find_common_ancestor(sply, dmnd)
            if len(res) > 0:
                paths.append(res)

        if len(paths) > 0:
            min_path, ancestor = find_min_path(paths)
            if len(min_path) <= allowed_hops and len(min_path) > 0:
                print ('Found a gap match for ' + dmnd + " and " + min_path[0] + ' common ancestor: ' + ancestor)
                print ('Path length: ' + str(math.floor(len(min_path) / 2)))
                print (' -> '.join(min_path))
                gap_matches.append(dmnd)
                gap_length.append(math.floor(len(min_path) / 2))


    print ('Alternate demand skills matched: ' + str(gap_matches))
    print ('Missing skills : ' + str(demand_missing.difference(gap_matches)))
    score = len(match)/(0.0  + len(demand))
    for path_len in gap_length:
        score += (1/len(demand)) * (1 / 2 ** path_len)
    print ('Score: '  + str(score))

supply = set(['c', 'c++', 'html', 'css' , 'javascript'])
demand = set(['java', 'html', 'css', 'javascript', 'jquery', 'tomcat'])

cv_jd_match(supply, demand, allowed_hops=5)
print ('#' * 50)
print ()
cv_jd_match(supply, demand, allowed_hops=3)
print ('#' * 50)
print ()
supply = set(['python', 'html', 'css' , 'javascript', 'xen'])
demand = set(['java', 'html', 'css', 'javascript', 'jquery', 'docker', 'vmware'])

cv_jd_match(supply, demand)

# print ('Experiments: Explain a 2 hop')

# skill_a = g.V().has('name', 'c').next().id
# skill_b = g.V().has('name', 'java').next().id

# print ('Skill A: c, Skill B: java')
# ancestor = g.V(skill_b).repeat(in_('related')).emit().as_('x').repeat(out('related')).emit(hasId(skill_a)).select('x').limit(1).toList()[0].id
# print ('Common Ancestor: ' + g.V(ancestor).values('name').next())

# print ('paths to skills')
# print (' -> '.join(g.V(ancestor).repeat(out().simplePath()).until(hasId(skill_a)).path().by('name').toList()[0]))
# print (' -> '.join(g.V(ancestor).repeat(out().simplePath()).until(hasId(skill_b)).path().by('name').toList()[0]))
