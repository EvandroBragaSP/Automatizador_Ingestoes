# -*- coding: utf-8 -*-

#IMPORTS
'''
Importacoes de bibliotecas
'''    

from importacoes_bibliotecas import *


#FUNCOES
'''
Funcoes para utilizacoes genericas
'''

def upsertArray(listaLogs, valorUpdate):
    '''
    Funcao que atualiza valores de um array
    @param listaLogs: Lista de logs 
    @type array:
    @param valorUpdate: valor que sera atualizado em posicao da lista
    @type string:
    '''    
    
    if(len(listaLogs) > 0):
        for item in range (len(listaLogs)):
            if "SUCESSO" in listaLogs[item] and "SUCESSO" in valorUpdate:#arquivoNaoEstruturado:
                listaLogs[item]=valorUpdate
    else:
        listaLogs.append(valorUpdate)
    
def criacaoCabecalho(dataFrameArqParametros):
    '''
    Funcao faz a criacao do cabecalho do arquivo
    @param dataFrameArqParametros: Objeto DataFrame com o arquivo de parametros carregado em memoria 
    @type DataFrame pandas:
    '''    

    if(dataFrameArqParametros.at[0, "DESCRICAO_COLUNAS"] != ""):
        return dataFrameArqParametros.at[0, "DESCRICAO_COLUNAS"].split(",")
    else: 
        return []
            
def carregaLinhasProcessamento(dataFrameArquivo):
    '''
    Funcao faz a carga em memoria das linhas do arquivo de processamento
    @param dataFrameArquivo: Objeto DataFrame com o arquivo de processamento carregado em memoria 
    @type DataFrame pandas:
    '''    

    arquivoConvertido = []
    for linha in dataFrameArquivo.itertuples():
        arquivoConvertido.append(str(linha._1))
                      
    return arquivoConvertido     

def lerArquivoHdfs(file):
    '''
    Funcao faz a leitura de arquivo proveniente do HDFS 
    @param file: Diretorio com o caminho do arquivo no HDFS 
    @type string:
    '''
    
    client_hdfs = InsecureClient('http://' + os.environ['IP_HDFS'] + ':50070')
    with client_hdfs.read(file, encoding = 'utf-8') as reader:
        return pd.read_csv(reader,index_col=0)             

def removerAcentosECaracteresEspeciais(texto):
    '''
    Funcao faz a remocao de acentos e caracteres especiais de um texto 
    @param texto: Texto para remocao de acentos e caracteres especiais 
    @type string:
    '''

    nfkd = unicodedata.normalize('NFKD', texto)
    palavraSemAcento = u"".join([c for c in nfkd if not unicodedata.combining(c)])
    return re.sub('[^a-zA-Z0-9 |/-;\\\]', '', palavraSemAcento)

def run_cmd(args_list):
    '''
    Funcao faz a execucao de um comando em nivel de subprocesso 
    @param args_list: Lista de parametros para composicao de comando 
    @type string:
    '''

    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err 

def moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao faz a movimentacao dos arquivos de processando para o diretorio de rejeitados 
    @param dataFrameArqAmbiente: Objeto com as variaveis dos parametros de ambiente 
    @type Object Data Frame Pandas:
    @param nome_arquivo_original: Nome do arquivo completo de origem 
    @type string:    
    '''
    
    (ret, out, err) = run_cmd(['mv', dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSANDO"] + nome_arquivo_original, dataFrameArqAmbiente.at[0,"DIRETORIO_REJEITADOS"] + nome_arquivo_original.replace(".", datetime.datetime.now().strftime("_%Y%m%d_%H%M%S."))])
    
def moveProcessados(dataFrameArqAmbiente, nome_arquivo_original):
    '''
    Funcao faz a movimentacao dos arquivo processando para o diretorio de processados 
    @param dataFrameArqAmbiente: Objeto com as variaveis dos parametros de ambiente 
    @type Object Data Frame Pandas:
    @param nome_arquivo_original: Nome do arquivo completo de origem 
    @type string:    
    '''
    
    (ret, out, err) = run_cmd(['mv', dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSANDO"] + nome_arquivo_original, dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSADOS"] + nome_arquivo_original.replace(".", datetime.datetime.now().strftime("_%Y%m%d_%H%M%S."))])    

def escreveLogs(path, mensagem):
    '''
    Funcao faz a escrita dos logs  
    @param path: caminho HDFS para a gravacao dos logs 
    @type String:
    @param mensagem: Texto que sera inserido no arquivo de log 
    @type string:    
    '''
    
    hdfs = InsecureClient('http://bcodxbgd01.externo.ad:50070')
    if hdfs.status(path, strict=False) is not None :
        hdfs.write(path, data="{} \n".format(mensagem), append=True, encoding='utf-8-sig')
    else:
        hdfs.write(path, data="{} \n".format(mensagem), encoding='utf-8-sig')
        
def compactarArquivo(filePath) :
    '''
    Funcao faz a compactacao de arquivo em zip 
    @param filePath: Diretorio com o caminho do arquivo para zip 
    @type string:
    '''
    
    fileZIP = filePath.replace(".txt" , ".zip")
    fileComp = zipfile.ZipFile(fileZIP , 'w')
    fileComp.write(filePath)
    fileComp.close()

def lerDiretorio(path):
    '''
    Funcao faz a leitura de diretorio 
    @param path: Diretorio para leitura 
    @type string:
    '''
    
    caminhos = [os.path.join(path,nome) for nome in os.listdir(path)]
    txt = [arq for arq in caminhos if arq.lower().endswith(".txt")]
    csv = [arq for arq in caminhos if arq.lower().endswith(".csv")]
    arquivos = []
    arquivos.extend(txt)
    arquivos.extend(csv)
    return arquivos

def deletarArquivos(lista,data):
    '''
    Funcao faz a exclusao de arquivos 
    @param lista: Lista de arquivos para exclusao 
    @type string:
    @param data: Data de vigencia para exclusao 
    @type string:
    '''
    
    arq = [arq for arq in lista if arq.endswith(data+".txt") or arq.endswith(data+".csv")] 
    for x in arq:
        os.remove(x)