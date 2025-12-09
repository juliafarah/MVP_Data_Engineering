Repository dedicated to MVP Project for Data Engineering sprint for PUC-Rio Data Science and Analytics Post Graduate course.


# MVP Data Engineering: Pipeline Airbnb Rio de Janeiro

Este projeto é focado na construção de um pipeline de dados completo (End-to-End) utilizando a plataforma **Databricks**. O notebook com todo o passo a passo do MVP está disponível neste [link](https://github.com/juliafarah/MVP_Data_Engineering/blob/main/MVP_Pipeline_Airbnb_Rio.ipynb).

Ferramentas utilizadas:
* **Databricks** (Plataforma de Dados)
* **PySpark** (Processamento Distribuído)
* **Python** (Linguagem de Programação)
* **SQL** (Consultas e Análises)
* **Pandas/Matplotlib** (Visualização Gráfica)

---

## 1. Objetivo e Definição do Problema

O objetivo é processar dados públicos do Airbnb referentes à cidade do Rio de Janeiro, transformando dados brutos em insights valiosos sobre o mercado de aluguel por temporada através da **Arquitetura Medalhão** aliada à **Modelagem Dimensional**.

**Perguntas a serem respondidas:**
1. Qual a região do Rio tem a média de preço (ticket médio) mais cara da cidade?
2. Quais são os 10 bairros com a maior disponibilidade de anúncios?
3. Quais regiões da cidade têm mais disponibilidade de apto inteiro, de apenas quarto inteiro e quarto compartilhado?
4. A plataforma está dominada por empresas especializadas ou por hosts que visam apenas ter uma renda extra?
5. Quais bairros garantem maior taxa de ocupação?

---

## 2. Busca e Coleta de Dados

**Fonte de Dados:**
Os dados foram extraídos do portal **Inside Airbnb**, uma fonte independente e pública que disponibiliza dados da plataforma Airbnb.

* **Dataset:** listings.csv
* **Fonte Original:** [Inside Airbnb - Get the Data - Rio de Janeiro, Brasil](http://insideairbnb.com/get-the-data.html) 

**Metodologia de Coleta:**
A ingestão dos dados foi realizada diretamente do Inside Airbnb, baixando o arquivo bruto (`listings.csv`) e armazenando-o neste repositório e conectando com o Databricks utilizando a linguagem Python (Pandas) na camada inicial do Data Lake.

---

## 3. Modelagem e Arquitetura de Dados

O projeto foi construido combinando o fluxo de dados da **Arquitetura Medalhão** com a estruturação lógica em **Star Schema** desde a concepção.

### A. Star Schema

A principal estratégia foi transformar a tabela original `listings` (uma tabela *flat* com todas as informações misturadas) em um modelo relacional analítico composto por **1 Tabela Fato e 3 Tabelas Dimensão**, como mostra a figura.


![star_schema_mvp_de_airbnb](https://github.com/user-attachments/assets/c1d68287-f40a-40bb-af88-de2d028f0947)


### B. Arquitetura Medalhão

* **Camada Bronze (Raw):** Dados brutos da tabela *flat*, mantendo o histórico imutável.

  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | bronze | listings | false |
  | bronze | zonas_bairros | false |

* **Camada Silver (Cleaned & Modeled):** Etapa onde ocorre a limpeza e a decomposição lógica dos dados.
    * Separação dos atributos nas 3 Dimensões (Localização, Host, Anúncio).
    * Definição da Tabela Fato com as métricas quantitativas (PK e FK).
    * Tratar o nan convertendo para um nulo real do banco de dados.
    * Remoçao de dados que corrompem a análise dos dados.


  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | silver | dim_anuncio | false |
  | silver | dim_host | false |
  | silver | dim_localizacao | false |
  | silver | fact_listings | false |
  
* **Camada Gold (Curated):** Tabelas Fato e Dimensões consolidadas e otimizadas para performance em ferramentas de BI e análises exploratória dos dados.

  | **database** | **tableName** | **isTemporary** |
  | :--- | :--- | :--- |
  | gold | dim_anuncio | false |
  | gold | dim_host | false |
  | gold | dim_localizacao | false |
  | gold | fact_listings | false |


Essa abordagem garante integridade referencial e facilita respostas rápidas para as perguntas de negócio definidas no início do projeto.

---

## 4. Pipeline de Carga e Transformação (ETL)

O processo de ETL foi desenvolvido utilizando **PySpark** no Databricks Community Edition, orientado à construção do modelo dimensional:

1.  **Extração (Bronze):** Leitura do arquivo CSV bruto com inferência de schema inicial.
2.  **Transformação e Modelagem (Silver):**
    * Decomposição da tabela flat original.
    * **Criação das Dimensões:** Seleção e tratamento de atributos descritivos (bairros, dados do host, tipo de propriedade).
    * **Criação da Fato:** Isolamento de métricas transacionais (preço, disponibilidade, reviews) e chaves estrangeiras.
    * Limpeza de caracteres especiais (ex: moeda) e conversão de tipos (String -> Double/Int).
3.  **Carga (Gold):**
    * Persistência do Star Schema (Fato + 3 Dimensões) pronto para consumo.
    * Criação de visões agregadas para responder às perguntas de negócio.


---

## 5. Qualidade de Dados

* Verificação de valores nulos em campos críticos como `host_id` e `ad_id`.
* Identificação de preços negativos.
* Garantia de que os dados categóricos (bairros e regiões) estavam padronizados nas dimensões.

---

## 6. Solução do Problema (Insights Obtidos)

Com base nos dados processados, as principais conclusões foram: ([análise detalhada no notebook](https://github.com/juliafarah/MVP_Data_Engineering/blob/main/MVP_Pipeline_Airbnb_Rio.ipynb)) 

* **Zona Sul:** Mantém sua hegemonia em volume e liquidez ao abraçar o turismo de massa.  Copacabana consolida-se como o líder absoluto em número de anúncios, detendo 10.500 anúncios cadastrados na plataforma. Além disso, a demanda transborda para bairros satélites (ex: Gávea e Vidigal), que oferecem custo-benefício ou experiências autênticas (vista panorâmica) em comparação aos bairros mais caros da orla.
* **Zona Central:** A região central valida a aposta na revitalização (Reviver Centro e Porto Maravilha) e turismo cultural. Santa Teresa (58%) e Centro (52%) mantêm ocupação competitiva o que a torna uma aposta inteligente tanto para pequenos investidores quanto para empresas especializadas atentos na retomada do turismo cultural e corporativo.
* **Zona Oeste (Liquidez):** A busca por natureza e exclusividade garante as maiores taxas da cidade. A **Barra da Tijuca** possui o maior portfolio (2.739 anúncios). A taxa de ocupação de 56% indica alta liquidez, logo, é um mercado seguro de giro constante.


---

## 7. Autoavaliação e Conclusão

O pipeline construído atingiu o objetivo de estruturar dados desorganizados em informações estratégicas. A decisão arquitetural de **modelar os dados em Star Schema (Fato + 3 Dimensões)** desde as etapas iniciais de transformação provou-se eficaz, facilitando a análise e garantindo a escalabilidade do modelo. A utilização do PySpark permitiu a manipulação performática dos dados através das camadas Bronze, Silver e Gold.

