# -*- coding: utf-8 -*-

#IMPORTS
'''
Importacoes de bibliotecas
'''    

from utils import *
from escreve_arquivos import *

#FUNCOES
'''
Funcoes para validacoes e formatacoes do arquivo dde processamento
'''

def validarArquivoNaoEstruturado(dataFrameArqParametros, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a validacao de arquivos nao estruturados e arquivos estruturados
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:    
    '''    
    
    modulo = "validarArquivoNaoEstruturado"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:    
        if (dataFrameArqParametros.at[0,"NAO_ESTRUTURADO"] == "N") :
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Arquivo validado, tipo de arquivo ESTRUTURADO.")
            return True
        else :
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Arquivo validado, tipo de arquivo NAO ESTRUTURADO.")
            return False
            
    except Exception as e:    
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)   

def validarQuantidadeColunas(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a validacao de quantidades de colunas
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linhasArquivo: linhas do arquivo em processamento
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:      
    '''    

    modulo = "validarQuantidadeColunas"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    linhas_ok = []
    linhas_rejeitadas = []
    try:    
        for linha in linhasArquivo:                          
            if (len(linha.split(dataFrameArqParametros.at[0,"DELIMITADOR_ENTRADA"])) == int(dataFrameArqParametros.at[0, "QUANTIDADE_COLUNAS"])):
                linhas_ok.append(linha)            
            else:
                if (dataFrameArqParametros.at[0,"REJEITAR_ARQUIVO_COMPLETO"] == "S"):
                    escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" + "Arquivo completo rejeitado, quantidade de colunas recebidas no arquivo nao sao validas")
                    
                    moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
                    
                    print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo + "|" + "REJEICAO" + "|" + "Arquivo completo rejeitado, quantidade de colunas recebidas no arquivo nao sao validas")
                    sys.exit(1)
                else:
                    linhas_rejeitadas.append(linha)
        
        if(len(linhas_rejeitadas) > 0):
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" + "Linhas rejeitas, quantidade de colunas recebidas no arquivo nao sao validas")
            arquivo_rejeitados = EscreveArquivos(dataFrameArqAmbiente.at[0,"DIRETORIO_REJEITADOS"], dataFrameArqParametros.at[0,"NOME_ARQUIVO"], "txt")
            for line in linhas_rejeitadas:
                arquivo_rejeitados.writeLine(line)
        else:
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Quantidade de colunas recebidas no arquivo validadas com sucesso.")    
        
        return linhas_ok           
        
    except Exception as e:    
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1) 

def validarCabecalhoArquivo(dataFrameArqParametros, linha, cabecalho, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a validacao das descrcoes das colunas do arquivo, comparando com o parametro [DESCRICOES_COLUNAS]
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linhasArquivo: linhas do arquivo em processamento
    @type array:
    @param cabecalho: cabecalho do arquivo em processamento
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:       
    '''      
    
    modulo = "validarCabecalhoArquivo"
    
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:
        if(dataFrameArqParametros.at[0,"EXISTE_CABECALHO"] == "S"):
            ajusteLinha = ','.join(str(x) for x in linha)
            cabecalho_arquivo = ajusteLinha.replace(dataFrameArqParametros.at[0,"DELIMITADOR_ENTRADA"],",").split(",")
            if(set(cabecalho_arquivo)==set(cabecalho)):
                escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" + "SUCESSO" + "|" + "Descricoes das colunas validadas com sucesso")
            else :
                escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" + "Descricoes das colunas nao estao validas")
                
                moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
                
                print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo + "|" + "REJEICAO" + "|" + "Descricoes das colunas nao estao validas")
                sys.exit(1)
                             
    except Exception as e:
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "EXCECAO" + "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)
            
def validarRemoverCabecalho(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a remocao do cabecalho
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linhasArquivo: linhas do arquivo em processamento
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:        
    '''    

    modulo = "validarRemoverCabecalho"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:
        if(dataFrameArqParametros.at[0,"REMOVER_CABECALHO"] == "S"):    
            linhasArquivo.pop(0)
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"SUCESSO"+ "|" + "Remocao de cabecalho executada com sucesso.")
        
        return linhasArquivo                   
    except Exception as e: 
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)

def validarRemoverRodape(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a remocao do rodape
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linhasArquivo: linhas do arquivo em processamento
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:             
    '''    

    modulo = "validarRemoverRodape"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:
        if(dataFrameArqParametros.at[0,"REMOVER_RODAPE"] == "S"):
            linhasArquivo = linhasArquivo[:-1]
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"SUCESSO"+ "|" + "Remocao de rodape executada com sucesso.")
        
        return linhasArquivo                   
    except Exception as e: 
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)

def validarRemoverAcentos(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a validacao para remoover acentos e caracteres especiais
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linhasArquivo: linhas do arquivo em processamento
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:    
    '''   

    modulo = "validarRemoverAcentos"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:
        if(dataFrameArqParametros.at[0,"REMOVER_ACENTOS"] == "S") :
            for i in range(len(linhasArquivo)):
                linhasArquivo[i] = removerAcentosECaracteresEspeciais(linhasArquivo[i])
                
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"SUCESSO"+ "|" + "Remocao de acentos e caracteres especiais executada com sucesso.")
        
        return linhasArquivo
    
    except Exception as e:
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" +datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)                             

def validarExecutarScriptExterno(dataFrameArqParametros, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa script externo em subprocesso
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:     
    '''    

    modulo = "validarExecutarScriptExterno"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    
    try:    
        if (dataFrameArqParametros.at[0,"EXECUTAR_BASE_ANALITICA"] == "S") :
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Necessario executar script externo.")
            return True
        else :
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Nao e necessario executar script externo.")
            return False
            
    except Exception as e:    
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)           
    
def validarChaveValor(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao que executa a validacao de chaves e valores, para verificacao de nulos
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros 
    @type DataFrame Pandas:
    @param linha: linha do arquivo em processamento
    @type string: 
    @param Cabecalho: Lista de descricoes de colunas
    @type array:
    @param dataFrameArqAmbiente: Objeto DataFrame com o arquivo de parametros do ambiente 
    @type DataFrame Pandas:
    @param nome_arquivo_original: Descricao completa do nome do arquivo de entrada 
    @type String:      
    '''    
    
    modulo = "validarChaveValor"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
    linhas_ok = []
    linhas_rejeitadas = []
    
    try:
        if(dataFrameArqParametros.at[0,"VALIDAR_CHAVE_VALOR"] == "S"):    
            if (dataFrameArqParametros.at[0,"POSICOES_CHAVE_VALOR"] != ""):
                for linha in linhasArquivo:
                    
                    linhaArr = np.array(linha.split(dataFrameArqParametros.at[0,"DELIMITADOR_ENTRADA"]))
                    posicoes = list(map(int, dataFrameArqParametros.at[0,"POSICOES_CHAVE_VALOR"].split(",")))
                    chaves = linhaArr[posicoes]
                    
                    if(np.all(chaves == "")):
                        if(dataFrameArqParametros.at[0,"REJEITAR_ARQUIVO_COMPLETO"] == "S"):
                            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" + "Valores nulos foram encontrados em campos chaves para a ingestao, parametro rejeitar arquivo completo habilitado!")
                            
                            moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
                            
                            print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo + "|" + "REJEICAO" + "|" + "Valores nulos foram encontrados em campos chaves para a ingestao, parametro rejeitar arquivo completo habilitado, o arquivo completo foi rejeitado.")
                            sys.exit(1)
                        else:
                            linhas_rejeitadas.append(linha)
                    else:    
                        linhas_ok.append(linha)
                        
                escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Validacao de chave e valor realizada com sucesso!")     
            else :
                escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" +"Validacao do parametro de configuracao [POSICOES_CHAVE_VALOR] nao esta de acordo.")
                
                moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
                
                print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo + "|" + "REJEICAO" + "|" +"Validacao do parametro de configuracoes[POSICOES_CHAVE_VALOR] nao esta de acordo.")
                sys.exit(1)
        else :
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "SUCESSO" + "|" + "Validacao de chave e valor desabilitada!")
           
        if(len(linhas_rejeitadas) > 0):
            escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo + "|" + dt + "|" + hr_ini + "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "|" + "REJEICAO" + "|" +"Validacao de chave valor realizada no arquivo, linhas foram rejeitadas por nao estarem de acordo.")    
            for linha in linhas_rejeitadas:
                arquivo_rejeitados = EscreveArquivos(dataFrameArqAmbiente.at[0,"DIRETORIO_REJEITADOS"], dataFrameArqParametros.at[0,"NOME_ARQUIVO"], "txt")
                arquivo_rejeitados.writeLine(linha)
            
        return linhas_ok
          
    except Exception as e:  
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        
        moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
        
        print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
        sys.exit(1)