'''
Created on 3 mar. 2019

@author: XYxXT
'''
class MapReduce():
    global shuffling_map
    global reducing_map
    def __init__(self):
        self.shuffling_map = {}
        self.reducing_map = {}
        
    def readFile(self):
        '''leer todos los ficheros  y return todos los textos en un string'''
        return 0
    
    def splitting(self, items):
        ''' separar el string y crear multiple thread que llamara a mapping'''
        return 0
    
    def mapping(self, items):
        
        mapping_map = []
        
        for item in items:
            mapping_map.append([item, 1])

        self.shuffling(mapping_map)
        
    def shuffling(self, mapping_map):
        for item in mapping_map:
            if item[0] not in self.shuffling_map:
                self.shuffling_map.update({item[0]: item})
            else:
                self.shuffling_map[item].append(item)  


