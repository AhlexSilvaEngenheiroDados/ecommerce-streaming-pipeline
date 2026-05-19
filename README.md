# Pipeline de Dados em Tempo Real para E-commerce

Este projeto demonstra um pipeline de dados em tempo real para processamento de vendas de e-commerce, utilizando Apache Kafka para ingestão de dados, Apache Spark Streaming para processamento e análise, PostgreSQL para persistência de dados agregados e Grafana para visualização em tempo real. Todo o ambiente é orquestrado via Docker Compose, facilitando a configuração e execução.

## Arquitetura do Projeto

A arquitetura do pipeline é composta pelos seguintes componentes:

*   **Producer (Python):** Simula a geração de eventos de vendas de e-commerce e os publica em um tópico Kafka.
*   **Apache Kafka:** Atua como um barramento de mensagens distribuído e de alta vazão, recebendo os eventos do Producer.
*   **Apache Zookeeper:** Gerencia e coordena os brokers Kafka.
*   **Apache Spark Streaming (PySpark):** Consome os dados do Kafka, realiza transformações, limpezas e agregações em tempo real.
*   **PostgreSQL:** Armazena os dados processados e agregados pelo Spark.
*   **Grafana:** Plataforma de visualização e monitoramento para criar dashboards interativos com os dados do PostgreSQL.



## Arquitetura Medalhão

Este pipeline segue os princípios da **Arquitetura Medalhão**, que organiza os dados em camadas para garantir qualidade e reusabilidade:

*   **Camada Bronze (Dados Brutos):** Os dados brutos, exatamente como são recebidos do Kafka, sem transformações. No nosso `spark_processor.py`, a leitura inicial do Kafka representa esta camada.
*   **Camada Prata (Dados Tratados):** Dados limpos, padronizados e enriquecidos. No `spark_processor.py`, após o parsing do JSON, casting de tipos, cálculo de `total_value` e remoção de duplicados, os dados estão nesta camada.
*   **Camada Ouro (Dados Agregados):** Dados agregados e prontos para consumo por aplicações de BI, dashboards ou relatórios. No `spark_processor.py`, a agregação `total_sales` por categoria e janela de tempo representa esta camada, que é persistida no PostgreSQL.

## Pré-requisitos

Para rodar este projeto, você precisará ter instalado:

*   **Docker**
*   **Docker Compose**

## Como Rodar o Projeto

Siga os passos abaixo para configurar e executar o pipeline:

1.  **Clone o Repositório:**

    ```bash
    git clone https://github.com/seu-usuario/ecommerce-streaming-pipeline.git
    cd ecommerce-streaming-pipeline
    ```

2.  **Construa e Inicie os Serviços Docker:**

    ```bash
    docker-compose up --build -d
    ```

    Este comando irá:
    *   Baixar as imagens necessárias (Kafka, Zookeeper, Spark, PostgreSQL, Grafana).
    *   Criar a rede `kafka-spark-network`.
    *   Inicializar o Zookeeper, Kafka, Spark Master, Spark Worker, PostgreSQL e Grafana.
    *   Executar o script `init.sql` no PostgreSQL para criar a tabela `sales_summary`.
    *   Configurar automaticamente o Data Source do PostgreSQL no Grafana via provisioning.

3.  **Instale as Dependências do Python:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Execute o Producer de Dados:**

    Abra um novo terminal e execute o script Python que gera dados de vendas e os envia para o Kafka:

    ```bash
    python producer/producer.py
    ```

    Você verá mensagens indicando as vendas sendo enviadas para o tópico Kafka.

5.  **Execute o Processador Spark Streaming:**

    Abra outro novo terminal e execute o script PySpark que consome do Kafka, processa os dados e os persiste no PostgreSQL:

    ```bash
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 spark/spark_processor.py
    ```

    *Nota:* O `spark-submit` deve ser executado de dentro do container do `spark-master` ou `spark-worker` se você quiser que ele se conecte diretamente ao cluster Spark. Para simplificar, estamos executando-o localmente com a configuração de pacotes Kafka para Spark. Em um ambiente de produção, o job Spark seria submetido ao cluster de forma mais robusta.

    Você verá a saída das agregações de vendas por categoria a cada minuto no console e os dados serão persistidos no PostgreSQL.

6.  **Acesse o Grafana:**

    Após todos os serviços estarem rodando, acesse o Grafana em seu navegador:

    [http://localhost:3000](http://localhost:3000)

    *   **Usuário:** `admin`
    *   **Senha:** `admin`

    O Data Source `PostgreSQL` já estará configurado. Você pode criar um novo dashboard e adicionar painéis (panels) para visualizar as vendas por categoria. Por exemplo, uma query SQL para um painel de gráfico de barras pode ser:

    ```sql
    SELECT
      window_end AS "time",
      category,
      total_sales
    FROM sales_summary
    WHERE $__timeFilter(window_end)
    ORDER BY window_end, category;
    ```

## Limpeza

Para parar e remover todos os serviços Docker, execute:

```bash
docker-compose down -v
```

Isso removerá os containers, redes e volumes criados pelo Docker Compose.

## Link para pagina "https://dataportfo-muxkk3gy.manus.space/"
