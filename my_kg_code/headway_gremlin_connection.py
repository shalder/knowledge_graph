import sys
import traceback

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

class HWGremlinConnection:
    def __init__(self, ip_address='localhost', port='8182'):
        """
            Initialize a connection object to the gremlin server
        """
        # 8182 is the default gremlin Server port
        if type(port) != type(''):
            port = str(port)
        self.g = graph.traversal().withRemote(DriverRemoteConnection('ws://' + ip_address + ':' + port + '/gremlin','g'))


    def get_traversal_object(self):
        return (self.g)

    def clear_db(self):
        g.V().drop().iterate()
        g.E().drop().iterate()
