'''
Created on 3 mar. 2019

@author: XYxXT
'''
class MapReduce():
    '''shuffling_map es un mapa: {'word1':[['word1',1],['word1',1],['word1',1],['word1',1]],
    'word2':[['word2',1],['word2',1],['word2',1],['word2',1]]} '''
    '''reducing_map es un mapa: {'word1':5, 'word2':6}'''
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
    
    
    def mapping(self, text): 
        ''' items es una cadena de texto '''
        '''cada map es una lista: [['word1',1], ['word2',1], ['word1',1], ['word3',1]...]'''
        mapping_map = []
        
        items = text.split()
        
        for item in items:
            mapping_map.append([item, 1])

        self.shuffling(mapping_map)
        
    def shuffling(self, mapping_map):
        for item in mapping_map:
            if item[0] not in self.shuffling_map:
                self.shuffling_map[item[0]] = [item]
            else:
                self.shuffling_map[item[0]].append(item)  


    def reduce(self):
        for itemKey, itemValue in sorted(self.shuffling_map.items()):
            value = 0
            for v in itemValue:
                value += v[1]
            self.reducing_map[itemKey] = value
                   
    def removeChars(self,text, chars):
        for character in chars:
            text = text.replace(character, "")
