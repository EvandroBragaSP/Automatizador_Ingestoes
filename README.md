![image](https://user-images.githubusercontent.com/83982983/118044840-8929be00-b34d-11eb-807e-3754b17eaab1.png)<br>
<b>------- A sua comunidade de Engenharia de Dados -------</b>

# Automatizador de Ingestões:
Alinhamento Técnico

<h2 align=left><b>1. Escopo</b></h2>
<p align=left>A função do projeto será a de disponibilizar bases, arquivos, extrações e demais fontes internacionais XRM, construindo assim um automatizador de ingestões, modular e parametrizável, onde a disponibilidade e consumo das informações sejam transparentes e integradas ao ambiente Big Data (HDFS), provendo motores, mecanismos e controles necessários que garantem a qualidade, governança e a segurança da ingestão informacional.<br>

<h2 align=left><b>2. Estrutura de desenvolvimento</b></h2>
  
<h3 align=left><b>2.1 Linguagem</b></h3>
  
<p align=left>Será utilizado PYTHON.
  
  <h3 align=left><b>2.2 Versão</b></h3>
  <p align=left>Será usada a versão 3.6, a mais instável atualmente e que contêm a maior abrangência de bibliotecas.<br>
<h3 align=left><b>2.3 Dependências</b></h3>
  
  <p align=left>Algumas bibliotecas que utilizaremos para o desenvolvimento, serão:<br>
<p align=left>•	<b>Pandas</b> (Versão disponível no ambiente)<br>
<p align=left>•	<b>Lib.</b> para leitura e gravação arquivos. <b>ORC</b> (Verificar qual está disponível no ambiente)<br>
  
<h3 align=left><b>2.4 Ferramentas</b></h3>
  <p align=left>Usaremos para o desenvolvimento:<br>
<p align=left><b>•	Apache Zeppelin</b>, notebook onde centralizaremos os scripts python, testes e modularizações;<br>
<p align=left><b>•	Putty</b>, acesso aos servidores<br>
  
<p align=left><b>•	WINCSP</b>, movimentação de arquivos locais para os servidores do cluster.<br>
<h3 align=left><b>2.5 Ferramenta de agendamento</b></h3>
  <p align=left>O cliente utilizará o CONTROL-M como ferramenta de agendamento e execuções dos scripts do automatizador de ingestões.<br>
    
<h2 align=left><b>3. Arquivos para ingerir</b></h2>
<h3 align=left><b>3.1 Enriquecimento</b></h3>
  <p align=left>Todos os arquivos devem ser adicionados a data de ingestão, formato YYYYMMDD.<br>
<h3 align=left><b>3.2 Nomenclatura arquivo</b></h3>
  
  <p align=left>A nomenclatura deve seguir o seguinte formato, NOME_ARQUIVO _YYYYMMDD.EXTENSAO<br>
<h3 align=left><b>3.3 Frequência</b></h3>
  <p align=left>Dependerá da necessidade do arquivo, podendo haver:<br>
    
<p align=left><b>•	Diário</b><br>
<p align=left><b>•	Mensal</b><br>
<p align=left><b>•	Hora em hora</b><br>
<p align=left><b>•	Específico</b><br>
  
<h3 align=left><b>3.4 Volumetria</b></h3>
 <p align=left>Variável pela interface, atualmente a maior carga possui 5GB de dados.<br>
  <h2 align=left><b>4. Arquivos de parâmetros</b></h2>
  
  <h3 align=left><b>4.1 Formato</b></h3>
  <p align=left>O formato será de arquivo posicional, com delimitador e cabeçalho.<br>
<h3 align=left><b>4.2 Layout</b></h3>
<p align=left><b>• NAO_ESTRUTURADO</b><br>
  
<p align=left><b>• FILENAME</b><br>
<p align=left><b>• INCOMING-PATH</b><br>
<p align=left><b>• OUTPUT-PATH</b><br>
<p align=left><b>• ARCHIVE-PATH</b><br>
<p align=left><b>• LOG-EXECUTION-PATH</b><br>
<p align=left><b>• EXTENSION-SOURCE</b><br>
<p align=left><b>• EXTENSION-DESTINY</b><br>
<p align=left><b>• DELIMITADOR</b><br>
<p align=left><b>• COLUMNS</b><br>
<p align=left><b>• OUTPUT-CHARSET</b><br>
<p align=left><b>• FILENAME-PREFIX</b><br>
<p align=left><b>• FILENAME-EXTENSION</b><br>
<p align=left><b>• REMOVE-HEADER</b><br>
<p align=left><b>• REMOVE-FOOTER</b><br>
<p align=left><b>• KEEP-ORIGINAL-FILENAME</b><br>
<p align=left><b>• IGNORE-WRONGS-ROWS</b><br>
<p align=left><b>• REMOVE-ACCENTS</b><br>
<p align=left><b>• REMOVE-CHAR</b><br>
<p align=left><b>• CHAR-TO-REMOVE</b><br>
<p align=left><b>• TYPE_DATA_VALIDATE</b><br>
<p align=left><b>• KEY_VALUE</b><br>
<p align=left><b>• BASE_ANALYTICS</b><br>
<p align=left><b>• SCRIPT_ANAYTICS_PATH</b><br>
  
<h3 align=left><b>4.3 Formato de logs</b></h3>
<p align=left>Adicionar logs específicos de erros, descrevendo mensagens de erros técnica e uma descrição funcional do módulo que abendou.<br>
<h2 align=left><b>5. Logs</b></h2>
<h3 align=left><b>5.1 Formato para automatizador de ingestões</b></h3>

<p align=left>Os logs do automatizador serão otimizadas em um modelo que facilite a manutenção e identificação do erro e módulo que abendou. Sua gravação será em diretório específico, com o arquivo diário usando nomenclatura, ID_YYYYMMDD.log.<br>
<h2 align=left><b>6. Metadados</b></h2>
<h3 align=left><b>6.1	Ferramenta de mapeamento</b></h3>

<p align=left>Utilizaremos o <b>Information Map</b>, para armazenar as estruturas de metadados definidas para as cargas.<br>
<h3 align=left><b>6.2	Acesso ao Infomation Map</b></h3>
<p align=left>Será preciso a criação de usuário e senha para o acesso a ferramenta instalada por ambiente.<br>
<h2 align=left><b>7. Base analytics</b></h2>

<h3 align=left><b>7.1 Execução</b></h3>
<p align=left>Será implementado um parâmetro para a validação no automatizador, verificando a necessidade de executar o script para carga complementar de tabelas na base analytics. E outro parâmetro é o diretório de origem do script.
Esses arquivos serão gerados e disponibilizados pela área fornecedora.
