# -*- coding: utf-8 -*-

#IMPORTS
'''
Importacoes de bibliotecas
'''

from importacoes_bibliotecas import *

'''
Classe para escrita e automatizacao de arquivos do automatizador de ingestoes, voltado a escrita de linhas rejeitadas
'''
class EscreveArquivos :
    
    fileName = ''
    fileHandler = None
    
    def __init__(self, path , ID, extension) :
        file = 'LINHAS_REJEITADAS_{}_{:%Y%m%d_%H%M%S}.{}'.format(ID, datetime.datetime.now(), extension)   
        self.fileName = os.path.join(path,file)
        self.fileHandler = self.openFile(self.fileName)
        
    @classmethod    
    def openFile(self,fileName):
        self.fileHandler = open(fileName, 'a')
        return self.fileHandler                 
         
    @classmethod
    def writeLine(self,line):
        self.fileHandler.write("{} \n".format(line))