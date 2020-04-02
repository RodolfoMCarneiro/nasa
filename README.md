# Desafio Engenheiro de Dados


## Descrição do problema

Utilizando o dataset de requisições HTTP para o servidor da NASA Kennedy Space Center WWW na Flórida, algumas perguntas devem ser respondidas, sendo elas:

    - Número de hosts únicos
    - O total de erros 404
    - Os 5 URLs que mais causaram erro 404
    - Quantidade de erros 404 por dia
    - O total de bytes retornados

## Método de resolução

Por ser um problema relativamente aberto, onde não existem regras para tratativas de erros nos dados, a resolução se baseia em uma forma expositiva.

Todas as respostas estão sendo armazenadas em um arquivo chamado Resposta_desafio.log durante o processamento.

### Resolução 1

No arquivo de nome <ins>resolucaoSimplificada</ins>, existe uma abordagem direta onde simplesmente é considerado que todos os dados estejam dentro da forma esperada, como descrito na proposta do desafio.

### Resolução 2

Considerando que sempre existe a necessidade de avaliar o conjunto de dados e tratar os possíveis erros que aparecem, foi realizada uma segunda, contida em <ins>resolucaoDetalhada</ins> que, apenas de forma expositiva, separa os conjuntos de dados em que houve falta de informação dos dados que possuem todas as informações.

Sendo assim, são apresentados os resultados de cada um dos grupos isoladamente e também a unificação dos resultados, formando uma resposta mais completa.

Ao final da resolução, são apresentados alguns dataframes para exemplificar os dados que estão faltando em cada uma das colunas

## Referências

Durante a realização do desafio, foram os seguintes materiais de auxílio:

[Documentação Spark](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)

[Stack Overflow](https://stackoverflow.com/)

Notas de aula FIAP (Matéria: Computação paralela e distribuída)
