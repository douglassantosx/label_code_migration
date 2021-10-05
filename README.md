# label_code_migration
Carga para etiquetas de código de barras

Estudo de caso: Carga para etiquetas de código de barras

Cenário: Durante a migração de um sistema para outro, foram feitas duas extrações
com o objetivo de tratar a carga das etiquetas de código de barras.
Nos arquivos lote_fornecedor.csv e lote_fornecedor2.csv foram salvos os registros do arquivo 
lote_fornecedor.xls, que possue duas pastas de trabalho.Estes arquivos somados contém todos 
os números de etiquetas gerados no sistema em uso desde que as etiquetas começaram a ser usadas
Para a migração no novo sistema a necessidade é levar somente as etiquetas de produtos/lotes,
que ainda possuem saldo no sistema atual. Para consistir esta informação, será necessário ler o
arquivo carga.csv, que possuem todos os materiais que possuem algum saldo em estoque no sistema,
através dos critérios de código de produto, lote e validade.

Solução:
Utilizamos a biblioteca pandas para ler os arquivos csv e gerar os arquivos após o processamento.
A solução consiste em leitura de arquivos, tratamento dos dados, verificação e geração de arquivos:
etiquetas.csv que é o arquivo final a ser carregado e lote_nao_encontrados.csv com todos os
registros não encontrados em carga.csv, ou seja, que não possuem saldo.

Comentários:
Os arquivos recebidos contém algumas sujeiras, que precisaram ser tratadas para que o programa
não abortasse com erro.
 
