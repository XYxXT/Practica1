'''
Created on 3 mar. 2019

@author: XYxXT
'''
import threading

class MapReduce():
    '''shuffling_map es un mapa: {'word1':[['word1',1],['word1',1],['word1',1],['word1',1]],
    'word2':[['word2',1],['word2',1],['word2',1],['word2',1]]} '''
    '''reducing_map es un mapa: {'word1':5, 'word2':6}'''
    global shuffling_map
    global reducing_map
    global local_lists
    
    def __init__(self,numTreads):
        self.shuffling_map = {} 
        self.reducing_map = {}
        self.mapping_map=[]
        self.local_lists=[]
        self.numTreads=numTreads
        
    def readFile(self,name):
        '''leer todos los ficheros  y return todos los textos en un string'''
        self.local_lists=[ln.translate(None, '\n.,:;').lower() for ln in open(name, 'r')]
        self.splitting()
        self.reduce()
        
    
    def splitting(self):
        ''' separar el string y crear multiple thread que llamara a mapping'''
        
        #Threads para cada una de las lineas
        for line in xrange(0,len(self.local_lists)):
            thread = threading.Thread(target = self.mapping, args = [line])
            thread.run()

        
        #threads con indices (thread 0: 1-20,thread 1: 21-40 ...)
        for i in xrange(0,len(self.mapping_map),len(self.mapping_map)/self.numTreads):
            thread = threading.Thread(target = self.shuffling, args = (i,i+(len(self.mapping_map)/self.numTreads)))
            thread.run()
        
    
    
    def mapping(self, text): 
        ''' items es una cadena de texto '''
        '''cada map es una lista: [['word1',1], ['word2',1], ['word1',1], ['word3',1]...]'''

        
        items = self.local_lists[text].split(" ")
        for item in items: 
            self.mapping_map.append([item, 1])
        
        
    def shuffling(self,initialIndex,finalIndex):

        for item in self.mapping_map[initialIndex:finalIndex]:

            if item[0] not in self.shuffling_map:
                self.shuffling_map[item[0]] = [item]
            else:
                self.shuffling_map[item[0]].append(item)  



    def reduce(self):
        
        for itemKey, itemValue in self.shuffling_map.iteritems():
            value = 0
            for v in itemValue:
                value += v[1]
            self.reducing_map[itemKey] = value
                   

mr = MapReduce(4)
mr.readFile("./texts/bible.txt")

for k,v in mr.reducing_map.iteritems():
    print k +":"+ str(v)