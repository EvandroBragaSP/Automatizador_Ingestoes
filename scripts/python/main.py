# -*- coding: utf-8 -*-

#IMPORTS
'''
Importacoes de bibliotecas
'''

from configuracoes import *
from validacoes import *

dataFrameArqAmbiente = lerArquivoParametrosAmbiente("/home/david.braga/AutomatizadorIngestoes_Original/configuracoes/arquivo_parametros_ambiente.txt")

'''
Movimenta arquivo de FTP para diretorio PROCESSANDO
'''
    
os.system("for i in $(ls -rt {} | tail -1); do mv {}$i {} ; done; > /dev/null".format(dataFrameArqAmbiente.at[0,"DIRETORIO_ENTRADA"], dataFrameArqAmbiente.at[0,"DIRETORIO_ENTRADA"], dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSANDO"]))

process = Popen('basename {}*'.format(dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSANDO"]),shell=True,stdout=PIPE, stderr=PIPE)
std_out, std_err = process.communicate()
nome_arquivo_original = str(std_out.strip()).replace("b","").replace("'","")



index = nome_arquivo_original.index(re.search("_(20\d{2})(\d{2})(\d{2})",nome_arquivo_original).group(0))
nome_arquivo=nome_arquivo_original[:index]
data_arquivo_entrada=re.search("(20\d{2})(\d{2})(\d{2})",nome_arquivo_original).group(0)

dataFrameArqParametros = lerArquivoParametros(dataFrameArqAmbiente.at[0,"DIRETORIO_ARQUIVO_PARAMETROS"].strip(), nome_arquivo, nome_arquivo_original, dataFrameArqAmbiente)

hdfs = InsecureClient('http://' + dataFrameArqAmbiente.at[0,'HOST_HDFS'] + ':50070')
            
try:
    '''
    Instancias de variaveis
    '''
   
    modulo = "main"
    hr_ini = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dt = datetime.datetime.now().strftime("%Y-%m-%d")
   
    '''
    Funcoes e carregamento do arquivo em memoria
    '''
    
    arquivo_processando = dataFrameArqAmbiente.at[0,"DIRETORIO_PROCESSANDO"] + nome_arquivo_original
    cabecalho = criacaoCabecalho(dataFrameArqParametros)    
    dataFrameArquivoEmProcessamento=pd.read_csv(arquivo_processando, header=None)

    '''
    Validacao, formatacao do arquivo em processamento
    '''
    
    if(validarArquivoNaoEstruturado(dataFrameArqParametros, dataFrameArqAmbiente, nome_arquivo_original)):
        
        head = dataFrameArquivoEmProcessamento.values[0]
        validarCabecalhoArquivo(dataFrameArqParametros, head, cabecalho, dataFrameArqAmbiente, nome_arquivo_original)
        linhasArquivo = carregaLinhasProcessamento(dataFrameArquivoEmProcessamento)
        linhasArquivo = validarRemoverCabecalho(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original)
        linhasArquivo = validarRemoverRodape(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original)
        linhasArquivo = validarChaveValor(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original)
        linhasArquivo = validarQuantidadeColunas(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original)
        linhasArquivo = validarRemoverAcentos(dataFrameArqParametros, linhasArquivo, dataFrameArqAmbiente, nome_arquivo_original)
        
        linhas_ok = []
        for line in linhasArquivo:
            delimitador_saida = ""
            if(dataFrameArqParametros.at[0,"DELIMITADOR_DESTINO"] == ""):
                delimitador_saida = "|"
            else:
                delimitador_saida = dataFrameArqParametros.at[0,"DELIMITADOR_DESTINO"]
            
            linhas_ok.append(line.replace(dataFrameArqParametros.at[0,"DELIMITADOR_ENTRADA"], delimitador_saida))
        
        df = pd.DataFrame(data=linhas_ok)

        with hdfs.write(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_SAIDA"] + dataFrameArqParametros.at[0,"NOME_ARQUIVO_SAIDA"] + "_" + data_arquivo_entrada + datetime.datetime.now().strftime("_%Y%m%d_%H%M%S.") + dataFrameArqParametros.at[0,"EXTENSAO_DESTINO"], overwrite=True, encoding='utf-8-sig') as arq:
            df.to_csv(arq, header=None, index=False)
    else:
        os.system("hadoop fs -copyFromLocal {} {}> /dev/null".format(arquivo_processando, dataFrameArqParametros.at[0,"DIRETORIO_HDFS_SAIDA"] + dataFrameArqParametros.at[0,"NOME_ARQUIVO_SAIDA"] + "_" + data_arquivo_entrada + datetime.datetime.now().strftime("_%Y%m%d_%H%M%S.") + dataFrameArqParametros.at[0,"EXTENSAO_DESTINO"]))

    if(validarExecutarScriptExterno(dataFrameArqParametros, dataFrameArqAmbiente, nome_arquivo_original)):
        os.system("/etc/anaconda3/bin/python3 {} > /dev/null".format(dataFrameArqParametros.at[0,"DIRETORIO_SCRIPT_ANALITICO"].strip()))
        
        escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],"validarExecutarScriptExterno",dt,"log"), dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + "validarExecutarScriptExterno" + "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"SUCESSO"+ "|" + "Script externo disparado com sucesso")
    
    escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"SUCESSO"+ "|" + "Processo de ingestao executado com sucesso")
    
    moveProcessados(dataFrameArqAmbiente, nome_arquivo_original)
    
    print("0") 
except Exception as e:  
    #escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),dataFrameArqParametros.at[0,"NOME_ARQUIVO"] + "|" + modulo+ "|" +dt+ "|" +hr_ini+ "|" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
    
    escreveLogs("{}{}_{}.{}".format(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"],modulo,dt,"log"),"teste")
    
    moveRejeitados(dataFrameArqAmbiente, nome_arquivo_original)
    
    print(dataFrameArqParametros.at[0,"DIRETORIO_HDFS_LOGS"] + "|" + modulo+ "|" +"EXCECAO"+ "|" + str("Error on line {} Exception : {} {}".format(sys.exc_info()[-1].tb_lineno, type(e).__name__, e)))
    sys.exit(1)                