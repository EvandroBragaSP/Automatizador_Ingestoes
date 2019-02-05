# -*- coding: utf-8 -*-

#IMPORTS
from importacoes_bibliotecas import *
from utils import *

#FUNCOES

def lerArquivoParametros(caminhoArquivo, identificador, nome_arquivo_original, dataFrameArqAmbiente):
    '''
    Funcao que executa a carga em memoria dos parametros provenientes do arquivo de configuracoes de ingestao
    @param caminhoArquivo: String referente ao caminho do arquivo de parametros
    @type String:
    @param identificador: String referente ao nome do arquivo para processamento
    @type String:
    @param nomeOriginal: String referente ao nome completo do arquivo para processamento
    @type String:
    ''' 
    
    linhas = []
    
    cabecalho = [
        "DATA_REFERENCIA",
        "NAO_ESTRUTURADO",
        "EXISTE_CABECALHO",
        "NOME_ARQUIVO",
        "DIRETORIO_HDFS_SAIDA",
        "DIRETORIO_HDFS_LOGS",
        "EXTENSAO_ENTRADA",
        "EXTENSAO_DESTINO",
        "DELIMITADOR_ENTRADA",
        "DELIMITADOR_DESTINO",
        "QUANTIDADE_COLUNAS",
        "DESCRICAO_COLUNAS",
        "REMOVER_CABECALHO",
        "REMOVER_RODAPE",
        "NOME_ARQUIVO_SAIDA",
        "REJEITAR_ARQUIVO_COMPLETO",
        "REMOVER_ACENTOS",
        "VALIDAR_CHAVE_VALOR",
        "POSICOES_CHAVE_VALOR",
        "EXECUTAR_BASE_ANALITICA",
        "DIRETORIO_SCRIPT_ANALITICO"
    ]
        
    with open(caminhoArquivo, 'rt') as arquivo:

        for linha in arquivo.readlines()[1:]:
            if(re.search(identificador, linha)):
                linhas.append(linha.split("|"))
                return pd.DataFrame(linhas, columns=cabecalho)
            else:
                continue
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print("validacao se arquivo existe|configuracoes.py|EXCECAO|Configuracoes referentes ao nome identificador {} nao encontradas no arquivo de parametros".format(identificador))
        sys.exit(1)            
            
            
def lerArquivoParametrosAmbiente(caminhoArquivo):
    '''
    Funcao que executa a carga em memoria dos parametros provenientes do arquivo de configuracao do ambiente
    @param caminhoArquivo: String referente ao caminho do arquivo de parametros de ambiente 
    @type String:
    ''' 
    
    linhas = []
    
    cabecalho = [
        "DATA_REFERENCIA",
        "HOST_HDFS",
        "IP",
        "DIRETORIO_ENTRADA",
        "DIRETORIO_PROCESSANDO",
        "DIRETORIO_REJEITADOS",
        "DIRETORIO_PROCESSADOS",
        "DIRETORIO_ARQUIVO_PARAMETROS"
    ]
        
    with open(caminhoArquivo, 'rt') as arquivo:
        for linha in arquivo.readlines()[1:]:
            linhas.append(linha.split("|"))

    return pd.DataFrame(linhas, columns=cabecalho)    