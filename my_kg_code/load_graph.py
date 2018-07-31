import csv
import sys
import traceback
from headway_gremlin_connection import HWGremlinConnection

class SkillsKWGraph:
    def __init__(self):
        try:
            # 8182 is the default gremlin Server port
            self.gremlin_connection = HWGremlinConnection()
            self.g = self.gremlin_connection.get_traversal_object()
        except Exception as e:
            print ('Connection to gremlin server failed. Connection error stack trace:')
            print (traceback.format_exc())
            sys.exit(-1)

    def add_skill_nodes(self):
        # The skill domain and skill vertices are stored in a file which needs to be loaded into the 
        # Graph DB to form the Knowledge Graph
        g = self.g
        try:
            skillreader = csv.reader(open('skill_vertex.csv'), delimiter=',')
        except Exception as e:
            print ('Failed to load Vertex CSV File "skill_vertex.csv".')
            exit(-1)

        # Remove any stray whitespace characters when reading the skill names from the CSV files

        skillreader = [(row[0].strip(), row[1].strip()) for row in skillreader]
        for row in skillreader:
            
            # See if the skill node is present already
            count = g.V().has('name', row[1]).count().next()
            
            if count == 0:
                print ('Adding vertex ' + row[1])
                g.addV(row[0]).property('name', row[1]).next()

            if count > 0:
                print ('Node already present with count: ' + row[1] + ' ' + str(count))

        print ("*" * 76)
        print (" " * 25 + "Vertex loading complete")
        print ("*" * 76)

    def add_skill_relations(self):
        # The relationships between nodes are stored as a edge list in a CSV file where the
        # first column has the originating node name and the second column has the destination
        # node name 
        g = self.g
        try:
            skillreader = csv.reader(open('skill_edges.csv'), delimiter=',')
        except Exception as e:
            print ('Failed to load Vertex CSV File "skill_edges.csv".')
            exit(-1)

        skillreader = [(row[0].strip(), row[1].strip()) for row in skillreader]

        for row in skillreader:
            # See if the node is present already
            for idx in [0,1]:
                count = g.V().has('name', row[idx]).count().next()
                
                if count != 1:

                    # Ensure there are no duplicate skills before adding an edge between them
                    print ('Duplicate vertex found: ' + row[idx])
                    sys.exit(-1)

            left = g.V().has('name', row[0]).next().id
            right = g.V().has('name', row[1]).next().id
            count = g.V(left).out('related').hasId(right).count().next()
            if count == 0:
                print ('Adding edge from ' + row[0] + " to " + row[1])
                g.V(right).as_('r').V(left).addE('related').to('r').toList()


        print ()
        print ("*" * 76)
        print (" " * 25 + "Edge creation complete")
        print ("*" * 76)


if __name__ == '__main__':
    kw = SkillsKWGraph()
    kw.add_skill_nodes()
    kw.add_skill_relations()
