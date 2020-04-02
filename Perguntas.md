# Perguntas

## 1 - Qual o objetivo do comando cache em Spark?

O objetivo do comando cache é manter em memória um conjunto de dados. Ou seja, caso exista a necessidade de acessar um conjunto de dados diversas vezes, é possível utilizar esse comando para melhorar o desempenho.

## 2 - O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em MapReduce. Por quê?

O motivo dessa diferença está na forma de processamento. Isso ocorre pois o spark trabalha com os dados na memória, enquanto o MapReduce precisa ler e gravar os dados em disco. Além disso, as funções existentes para spark fazem com que a quantidade de código escrita seja menor do que a quantidade necessária em MapReduce ao considerar um mesmo objetivo.

## 3 - Qual é a função do SparkContext?

SparkContext é a conexão com o cluster Spark.

## 4 - Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

Resilient - Resiliência dos dados. Caso algum dado seja perdido, ele pode ser recriado.
Distributed - Distribuído em memória pelo cluster.
Datasets - Dados iniciais.

RDD é um objeto imutável distribuído pelo cluster.

## 5 - GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

Isso ocorre porque o reduceByKey realiza uma combinação em cada uma das partições e depois cada uma envia uma única resposta. Já o GroupByKey envia toda a informação e depois ela é combinada.

## 6 - Explique o que o código Scala abaixo faz. 
---
            val textFile = sc.textFile("hdfs://...") 
            val counts = textFile.flatMap(line => line.split(" ")) 
            .map(word => (word, 1)) 
            .reduceByKey(_ + _) 
            counts.saveAsTextFile("hdfs://...") 
---

O código acima é um contador de palavras.
Basicamente ele está lendo um arquivo no hdfs, realizando uma contagem onde faz uma separação do texto considerando espaço (" ") como caracter para realizar um split. Após isso ele inicia o processo de map e faz a redução somando. Ao final, o resultado da quantidade de palavras é salvo em um arquivo.