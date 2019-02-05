
from configuracoes import *
from validacoes import *

linha = "1;2;1000111019;PASP_EXT;00000005401;2015-04-09 20:12:41.190000;1;INDIVIDUAL;Pessoa Física;3;Consumo;Consumo;2;26300.0;2015-04-09 20:12:39.949000;;7;VIGENTE;Vigente;2015-04-09 20:12:41.190000;TECHNISYS01;TECHNISYS01 SERVICO;"

linhaArr = np.array(linha.split(";"))
posicoes = list(map(int, "1,3"))
chaves = linhaArr[posicoes]

if(np.all(chaves == "")):
    print("TRUE")
else:    
    print("ELSE")
  